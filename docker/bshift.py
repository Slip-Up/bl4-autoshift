#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bl4-autoshift.py — High-performance Borderlands 4 SHiFT code scraper and redeemer

Optimized version focused on performance, readability, and maintainability.
Features connection pooling, batch operations, and improved error handling.
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from dotenv import dotenv_values

# -------------------------------
# Configuration and Constants
# -------------------------------

class Config:
    """Centralized configuration management"""
    
    def __init__(self):
        # Load .env file if it exists (for local development)
        env_file_config = dotenv_values(".env") if Path(".env").exists() else {}
        self.env_config = env_file_config
        
        # Paths (Docker-aware)
        self.root_dir = Path(__file__).resolve().parent
        
        # Use Docker environment variables if available, otherwise use local paths
        self.db_path = Path(os.environ.get("DATABASE_PATH", self.root_dir / "bshift.db"))
        self.session_file = Path(os.environ.get("SESSION_PATH", self.root_dir / "session.json"))
        self.notification_file = Path(os.environ.get("NOTIFICATION_PATH", self.root_dir / "last_notification.txt"))
        self.debug_dir = self.root_dir / "bshift_debug"
        
        # API Configuration
        self.base_url = "https://shift.gearboxsoftware.com"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        
        # Runtime Configuration
        self.verbose = self._get_bool("VERBOSE", False)
        self.debug = self._get_bool("DEBUG", False)
        self.delay_seconds = self._get_float("DELAY_SECONDS", 2.0)
        self.max_retries = self._get_int("MAX_RETRIES", 8)
        self.no_redeem = self._get_bool("NO_REDEEM", False)
        
        # Platform Configuration - Required
        platforms_str = self._get_str("ALLOWED_PLATFORMS")
        if not platforms_str:
            raise ValueError("ALLOWED_PLATFORMS environment variable is required. Supported platforms: steam, epic, psn, xboxlive")
        self.allowed_platforms = [p.strip() for p in platforms_str.split(",")]
        
        # Validate platforms
        valid_platforms = {"steam", "epic", "psn", "xboxlive", "nintendo"}
        invalid_platforms = [p for p in self.allowed_platforms if p not in valid_platforms]
        if invalid_platforms:
            raise ValueError(f"Invalid platforms: {invalid_platforms}. Supported platforms: {', '.join(valid_platforms)}")
        
        self.email = self._get_str("SHIFT_EMAIL")
        self.password = self._get_str("SHIFT_PASSWORD")
        
        # Discord Webhook Configuration
        self.discord_webhook_url = self._get_str("DISCORD_WEBHOOK_URL")
        self.discord_notification_hours = self._get_int("DISCORD_NOTIFICATION_HOURS", 24)
        
        # Create debug directory only if debug mode is enabled
        if self.debug:
            self.debug_dir.mkdir(parents=True, exist_ok=True)
        
        # Performance Settings
        self.max_workers = 3  # Conservative for API rate limiting
        self.connection_timeout = 10
        self.read_timeout = 30
        self.expiration_buffer_days = 3
        self.permanent_code_years = 2
        
        # Regex Patterns (compiled once)
        self.uuid_pattern = re.compile(r"/code_redemptions/([0-9a-fA-F-]{36})")
        self.code_pattern = re.compile(r"\b[A-Z0-9]{5}(?:-[A-Z0-9]{5}){4}\b")
        
    def _get_str(self, key: str, default: str = "") -> str:
        # Check .env file first, then environment variables
        return self.env_config.get(key, os.environ.get(key, default))
    
    def _get_bool(self, key: str, default: bool = False) -> bool:
        # Check .env file first, then environment variables
        value = self.env_config.get(key, os.environ.get(key, "0" if not default else "1"))
        return value == "1"
    
    def _get_int(self, key: str, default: int) -> int:
        try:
            # Check .env file first, then environment variables
            value = self.env_config.get(key, os.environ.get(key, str(default)))
            return int(value)
        except ValueError:
            return default
    
    def _get_float(self, key: str, default: float) -> float:
        try:
            # Check .env file first, then environment variables
            value = self.env_config.get(key, os.environ.get(key, str(default)))
            return float(value)
        except ValueError:
            return default

# Global config instance
config = Config()

# -------------------------------
# Enums and Data Classes
# -------------------------------

class RedemptionStatus(Enum):
    """Enumeration of possible redemption statuses"""
    SUCCESS = "success"
    ALREADY_REDEEMED = "already_redeemed"
    EXPIRED = "expired"
    INVALID = "invalid"
    PLATFORM_UNAVAILABLE = "platform_unavailable"
    GAME_REQUIRED = "game_required"  # New status for "launch game first" message
    ERROR = "error"
    UNKNOWN = "unknown"

@dataclass
class CodeInfo:
    """Represents a SHiFT code with metadata"""
    code: str
    source: str
    expiration_date: Optional[datetime] = None
    first_seen: Optional[datetime] = None
    
    def is_expired(self) -> bool:
        """Check if code is expired considering buffer days"""
        if not self.expiration_date:
            return False  # No expiration = never expires
        
        # Treat codes years in the future as permanent
        years_ahead = (self.expiration_date - datetime.now()).days / 365
        if years_ahead > config.permanent_code_years:
            return False
        
        # Apply buffer for timezone differences
        cutoff_date = datetime.now() + timedelta(days=config.expiration_buffer_days)
        return self.expiration_date < cutoff_date

@dataclass
class RedemptionResult:
    """Result of a code redemption attempt"""
    code: str
    platform: str
    status: RedemptionStatus
    message: str
    timestamp: datetime

# -------------------------------
# Logging Setup
# -------------------------------

class CustomFormatter(logging.Formatter):
    """Custom formatter for console output"""
    
    def format(self, record):
        timestamp = datetime.now().strftime("%H:%M:%S")
        return f"[{timestamp}] {record.getMessage()}"

def setup_logging():
    """Configure logging system"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(CustomFormatter())
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# Console colors for beautiful output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'
    GRAY = '\033[90m'

def log(message: str, color: str = ""):
    """Enhanced logging function with colors and better formatting"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    if color:
        print(f"{Colors.GRAY}[{timestamp}]{Colors.END} {color}{message}{Colors.END}")
    else:
        print(f"{Colors.GRAY}[{timestamp}]{Colors.END} {message}")

def log_section(message: str):
    """Log a section separator with smaller formatting"""
    print(f"\n{Colors.CYAN}{'─' * 40}{Colors.END}")
    print(f"{Colors.CYAN}{message}{Colors.END}")
    print(f"{Colors.CYAN}{'─' * 40}{Colors.END}")

def log_success(message: str):
    """Log a success message"""
    log(f"SUCCESS: {message}", Colors.GREEN)

def log_error(message: str):
    """Log an error message"""
    log(f"ERROR: {message}", Colors.RED)

def log_warning(message: str):
    """Log a warning message"""
    log(f"WARNING: {message}", Colors.YELLOW)

def log_info(message: str):
    """Log an info message"""
    log(f"INFO: {message}", Colors.CYAN)

def log_code(code: str, status: str, details: str = "", color: str = Colors.CYAN):
    """Log code-related information with consistent formatting"""
    log(f"{status}: {Colors.BOLD}{code}{Colors.END} {details}", color)

def send_discord_notification(message: str, urgent: bool = False):
    """Send notification to Discord webhook if configured"""
    if not config.discord_webhook_url:
        return
    
    try:
        payload = {
            "content": message,
            "username": "SHiFT Code Bot"
        }
        
        response = requests.post(
            config.discord_webhook_url,
            json=payload,
            timeout=10
        )
        
        if response.status_code != 204:
            log_warning(f"Discord notification failed: {response.status_code}")
            
    except Exception as e:
        log_warning(f"Discord notification error: {e}")

def should_send_notification() -> bool:
    """Check if enough time has passed since last notification"""
    notification_file = config.notification_file
    
    if not notification_file.exists():
        return True
    
    try:
        last_time = datetime.fromisoformat(notification_file.read_text().strip())
        hours_passed = (datetime.now() - last_time).total_seconds() / 3600
        return hours_passed >= config.discord_notification_hours
    except:
        return True

def record_notification():
    """Record the time of the last notification"""
    notification_file = config.notification_file
    notification_file.write_text(datetime.now().isoformat())

def log_config():
    """Log current configuration with beautiful formatting"""
    log_section("Configuration")
    
    log_info(f"Verbose Mode: {Colors.BOLD}{config.verbose}{Colors.END}")
    log_info(f"Delay Between Requests: {Colors.BOLD}{config.delay_seconds}s{Colors.END}")
    log_info(f"Max Retries: {Colors.BOLD}{config.max_retries}{Colors.END}")
    log_info(f"Redeem Mode: {Colors.BOLD}{'Disabled' if config.no_redeem else 'Enabled'}{Colors.END}")
    log_info(f"Target Platforms: {Colors.BOLD}{', '.join(config.allowed_platforms)}{Colors.END}")
    
    if config.no_redeem:
        log_warning("Redemption is disabled - codes will only be scraped")
    elif config.email:
        email_masked = f"{config.email[:3]}***@{config.email.split('@')[1]}"
        log_info(f"SHiFT Account: {Colors.BOLD}{email_masked}{Colors.END}")
    else:
        log_warning("No SHiFT credentials configured")

# -------------------------------
# Database Management
# -------------------------------

class DatabaseManager:
    """Handles all database operations with connection pooling"""
    
    def __init__(self):
        self.db_path = config.db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize database schema"""
        with self.get_connection() as conn:
            conn.executescript("""
                PRAGMA foreign_keys=ON;
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                
                CREATE TABLE IF NOT EXISTS codes (
                    code TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    first_seen_ts TEXT NOT NULL,
                    expiration_date TEXT
                );
                
                CREATE TABLE IF NOT EXISTS redemptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    status TEXT NOT NULL,
                    detail TEXT,
                    FOREIGN KEY(code) REFERENCES codes(code) ON DELETE CASCADE
                );
                
                CREATE INDEX IF NOT EXISTS idx_redemptions_code_platform 
                ON redemptions(code, platform);
                
                CREATE INDEX IF NOT EXISTS idx_codes_expiration 
                ON codes(expiration_date);
            """)
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        try:
            yield conn
        finally:
            conn.close()
    
    def add_codes_batch(self, codes: List[CodeInfo]) -> int:
        """Add multiple codes in a batch operation"""
        if not codes:
            return 0
        
        new_count = 0
        with self.get_connection() as conn:
            for code_info in codes:
                # Check if exists and potentially update expiration
                cursor = conn.execute(
                    "SELECT expiration_date FROM codes WHERE code = ?", 
                    (code_info.code,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    existing_exp = existing[0]
                    if (code_info.expiration_date and existing_exp and 
                        code_info.expiration_date > datetime.fromisoformat(existing_exp)):
                        conn.execute(
                            "UPDATE codes SET expiration_date = ? WHERE code = ?",
                            (code_info.expiration_date.isoformat(), code_info.code)
                        )
                        if config.verbose:
                            old_date = datetime.fromisoformat(existing_exp).strftime('%Y-%m-%d')
                            new_date = code_info.expiration_date.strftime('%Y-%m-%d')
                            log(f"Updated {code_info.code} expiration: {old_date} -> {new_date}")
                    elif code_info.expiration_date and not existing_exp:
                        conn.execute(
                            "UPDATE codes SET expiration_date = ? WHERE code = ?",
                            (code_info.expiration_date.isoformat(), code_info.code)
                        )
                else:
                    # New code
                    now = datetime.now(timezone.utc).isoformat()
                    exp_str = code_info.expiration_date.isoformat() if code_info.expiration_date else None
                    conn.execute(
                        "INSERT INTO codes (code, source, first_seen_ts, expiration_date) VALUES (?, ?, ?, ?)",
                        (code_info.code, code_info.source, now, exp_str)
                    )
                    new_count += 1
            
            conn.commit()
        
        return new_count
    
    def get_unredeemed_codes(self, platforms: List[str]) -> List[Tuple[str, Optional[datetime]]]:
        """Get codes that need redemption with their expiration dates (includes game_required for retry)"""
        placeholders = ','.join(['?' for _ in platforms])
        
        with self.get_connection() as conn:
            cursor = conn.execute(f"""
                SELECT DISTINCT c.code, c.expiration_date
                FROM codes c 
                WHERE NOT EXISTS (
                    SELECT 1 FROM redemptions r 
                    WHERE r.code = c.code 
                    AND r.platform IN ({placeholders})
                    AND r.status IN ('success', 'already_redeemed', 'expired')
                )
                ORDER BY c.first_seen_ts DESC
            """, platforms)
            
            results = []
            for row in cursor.fetchall():
                code = row[0]
                exp_str = row[1]
                exp_date = datetime.fromisoformat(exp_str) if exp_str else None
                results.append((code, exp_date))
            
            return results
    
    def get_game_required_codes(self, platforms: List[str]) -> List[Tuple[str, Optional[datetime]]]:
        """Get codes that are blocked by 'launch game first' requirement"""
        placeholders = ','.join(['?' for _ in platforms])
        
        with self.get_connection() as conn:
            cursor = conn.execute(f"""
                SELECT DISTINCT c.code, c.expiration_date
                FROM codes c 
                WHERE EXISTS (
                    SELECT 1 FROM redemptions r 
                    WHERE r.code = c.code 
                    AND r.platform IN ({placeholders})
                    AND r.status = 'game_required'
                )
                ORDER BY c.first_seen_ts DESC
            """, platforms)
            
            results = []
            for row in cursor.fetchall():
                code = row[0]
                exp_str = row[1]
                exp_date = datetime.fromisoformat(exp_str) if exp_str else None
                results.append((code, exp_date))
            
            return results
    
    def clear_game_required_status(self, platforms: List[str]) -> int:
        """Clear game_required status so codes can be attempted again"""
        placeholders = ','.join(['?' for _ in platforms])
        
        with self.get_connection() as conn:
            cursor = conn.execute(f"""
                DELETE FROM redemptions 
                WHERE status = 'game_required' 
                AND platform IN ({placeholders})
            """, platforms)
            conn.commit()
            return cursor.rowcount
    
    def add_redemption(self, result: RedemptionResult):
        """Add a redemption result to the database"""
        with self.get_connection() as conn:
            conn.execute(
                "INSERT INTO redemptions (code, platform, ts, status, detail) VALUES (?, ?, ?, ?, ?)",
                (result.code, result.platform, result.timestamp.isoformat(), 
                 result.status.value, result.message)
            )
            conn.commit()
    
    def mark_codes_expired_batch(self, codes: List[str], platforms: List[str]):
        """Mark multiple codes as expired for all platforms"""
        if not codes:
            return
        
        now = datetime.now(timezone.utc).isoformat()
        
        with self.get_connection() as conn:
            for code in codes:
                for platform in platforms:
                    # Check if already marked
                    cursor = conn.execute(
                        "SELECT 1 FROM redemptions WHERE code = ? AND platform = ? AND status = 'expired'",
                        (code, platform)
                    )
                    if not cursor.fetchone():
                        conn.execute(
                            "INSERT INTO redemptions (code, platform, ts, status, detail) VALUES (?, ?, ?, ?, ?)",
                            (code, platform, now, "expired", "Code expired (not attempted due to expiration date)")
                        )
            conn.commit()
    
    def is_code_redeemed(self, code: str, platform: str) -> bool:
        """Check if a code has been successfully redeemed"""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT 1 FROM redemptions WHERE code = ? AND platform = ? AND status IN ('success', 'already_redeemed') LIMIT 1",
                (code, platform)
            )
            return cursor.fetchone() is not None

# Global database manager
db = DatabaseManager()

# -------------------------------
# HTTP Session Management
# -------------------------------

class OptimizedSession:
    """High-performance HTTP session with connection pooling and rate limiting"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Configure connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=requests.adapters.Retry(
                total=config.max_retries,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504]
            )
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        self.authenticated = False
        self.csrf_token = None
        self._last_request_time = 0.0
        self._load_session()
    
    def _save_session(self):
        """Save session state to file"""
        try:
            session_data = {
                'cookies': dict(self.session.cookies),
                'csrf_token': self.csrf_token,
                'authenticated': self.authenticated
            }
            config.session_file.write_text(json.dumps(session_data))
        except Exception as e:
            if config.verbose:
                log(f"Failed to save session: {e}")
    
    def _load_session(self):
        """Load session state from file"""
        try:
            if config.session_file.exists():
                session_data = json.loads(config.session_file.read_text())
                
                for name, value in session_data.get('cookies', {}).items():
                    self.session.cookies[name] = value
                
                self.csrf_token = session_data.get('csrf_token')
                self.authenticated = session_data.get('authenticated', False)
                
                if self.authenticated and not self._verify_auth():
                    self.authenticated = False
                    self.csrf_token = None
        except Exception:
            pass  # Start fresh if loading fails
    
    def _verify_auth(self) -> bool:
        """Verify authentication status"""
        try:
            resp = self.session.get(
                f"{config.base_url}/account", 
                timeout=(config.connection_timeout, config.read_timeout)
            )
            
            if resp.status_code == 200 and "/account" in resp.url:
                content = resp.text.lower()
                is_authenticated = any(indicator in content for indicator in 
                                     ["sign_out", "logout", "profile"])
                
                if is_authenticated:
                    log_success("Authentication verified")
                else:
                    log_error("Authentication failed")
                
                return is_authenticated
            
            return False
        except Exception as e:
            log_error(f"Authentication check failed: {e}")
            return False
    
    def _throttle(self):
        """Rate limiting for API requests"""
        elapsed = time.time() - self._last_request_time
        wait_time = max(0.0, config.delay_seconds - elapsed)
        if wait_time > 0:
            time.sleep(wait_time)
        self._last_request_time = time.time()
    
    def login(self, email: str, password: str) -> bool:
        """Authenticate with SHiFT website"""
        if self.authenticated and self._verify_auth():
            log("Already authenticated")
            return True
        
        log("Logging in...")
        
        try:
            # Get login page
            resp = self.session.get(
                f"{config.base_url}/home",
                timeout=(config.connection_timeout, config.read_timeout)
            )
            
            if config.verbose:
                log(f"Login page status: {resp.status_code}")
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Extract CSRF token
            csrf_input = soup.find('input', {'name': 'authenticity_token'})
            if csrf_input:
                self.csrf_token = csrf_input.get('value')
            else:
                csrf_meta = soup.find('meta', {'name': 'csrf-token'})
                if csrf_meta:
                    self.csrf_token = csrf_meta.get('content')
            
            if not self.csrf_token:
                log("ERROR: Could not find CSRF token")
                return False
            
            if config.verbose:
                log(f"Found CSRF token: {self.csrf_token[:20]}...")
            
            # Submit login form
            login_data = {
                'utf8': '%E2%9C%93',  # URL-encoded checkmark
                'authenticity_token': self.csrf_token,
                'user[email]': email,
                'user[password]': password,
                'commit': 'Sign In'
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': config.base_url,
                'Referer': f"{config.base_url}/home"
            }
            
            log("Submitting login form...")
            resp = self.session.post(
                f"{config.base_url}/sessions",
                data=login_data,
                headers=headers,
                timeout=(config.connection_timeout, config.read_timeout),
                allow_redirects=True
            )
            
            # Verify login success
            if self._verify_auth():
                self.authenticated = True
                self._save_session()
                log_success("Successfully logged into SHiFT")
                return True
            else:
                log_error("Login failed - authentication verification failed")
                return False
                
        except Exception as e:
            log_error(f"Login failed: {e}")
            return False
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """Make throttled GET request"""
        self._throttle()
        return self.session.get(url, **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """Make throttled POST request"""
        self._throttle()
        return self.session.post(url, **kwargs)
    
    def get_csrf_token(self) -> Optional[str]:
        """Get fresh CSRF token from rewards page"""
        try:
            resp = self.get(
                f"{config.base_url}/rewards",
                timeout=(config.connection_timeout, config.read_timeout)
            )
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Try input field first
            csrf_input = soup.find('input', {'name': 'authenticity_token'})
            if csrf_input:
                self.csrf_token = csrf_input.get('value')
                return self.csrf_token
            
            # Try meta tag
            csrf_meta = soup.find('meta', {'name': 'csrf-token'})
            if csrf_meta:
                self.csrf_token = csrf_meta.get('content')
                return self.csrf_token
            
        except Exception as e:
            if config.verbose:
                log(f"Error getting CSRF token: {e}")
        
        return None

# -------------------------------
# Code Scraping
# -------------------------------

class CodeScraper:
    """High-performance code scraper with concurrent processing"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': config.user_agent})
        
        # Scraping sources
        self.sources = [
            ("https://mentalmars.com/game-news/borderlands-4-shift-codes/", "mentalmars"),
            ("https://gaming.news/codex/borderlands-4-shift-codes-list-guide-and-troubleshooting/", "gaming"),
            ("https://thegamepost.com/borderlands-4-all-shift-codes/", "thegamepost"),
            ("https://www.polygon.com/borderlands-4-active-shift-codes-redeem/", "polygon"),
            ("https://www.ign.com/wikis/borderlands-4/Borderlands_4_SHiFT_Codes", "ign"),
            ("https://www.gamespot.com/articles/borderlands-4-shift-codes-all-active-keys-and-how-to-redeem-them/1100-6533833/", "gamespot")
        ]
    
    def scrape_all_sources(self) -> Dict[str, List[CodeInfo]]:
        """Scrape all sources concurrently"""
        all_results = {}
        
        with ThreadPoolExecutor(max_workers=len(self.sources)) as executor:
            # Submit all scraping tasks
            future_to_source = {
                executor.submit(self._scrape_source, url, name): name
                for url, name in self.sources
            }
            
            # Collect results
            for future in as_completed(future_to_source):
                source_name = future_to_source[future]
                try:
                    codes = future.result()
                    all_results[source_name] = codes
                    
                    if codes:
                        log_success(f"{len(codes)} codes from {source_name}")
                    # Don't log when no codes found - reduces noise
                        
                except Exception as e:
                    log(f"Error scraping {source_name}: {e}")
                    all_results[source_name] = []
        
        return all_results
    
    def _scrape_source(self, url: str, source_name: str) -> List[CodeInfo]:
        """Scrape codes from a single source with enhanced content extraction"""
        try:
            resp = self.session.get(url, timeout=(config.connection_timeout, config.read_timeout))
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Use enhanced extraction for IGN and fallback for others
            if source_name == "ign":
                # Always use enhanced extraction for IGN
                codes_found = self._enhanced_content_extraction(soup, resp.text, source_name)
            else:
                # Primary extraction method for other sources
                text_content = soup.get_text()
                codes_found = self._extract_codes_from_text(text_content, source_name)
                
                # If no codes found, try enhanced extraction methods
                if not codes_found:
                    codes_found = self._enhanced_content_extraction(soup, resp.text, source_name)
            
            return codes_found
            
        except Exception as e:
            if config.verbose:
                log(f"Error scraping {url}: {e}")
            return []
    
    def _extract_codes_from_text(self, text_content: str, source_name: str) -> List[CodeInfo]:
        """Extract codes from text content"""
        codes_found = []
        processed_codes = set()  # Avoid duplicates from same source
        
        # Find all code patterns
        for match in config.code_pattern.finditer(text_content):
            code = self._normalize_code(match.group())
            
            if code in processed_codes:
                continue
            
            processed_codes.add(code)
            
            # Extract expiration date near the code
            expiration_date = self._extract_expiration_date(text_content, match.start())
            
            codes_found.append(CodeInfo(
                code=code,
                source=source_name,
                expiration_date=expiration_date
            ))
        
        return codes_found
    
    def _enhanced_content_extraction(self, soup: BeautifulSoup, raw_html: str, source_name: str) -> List[CodeInfo]:
        """Enhanced extraction for sites with dynamic content or complex structures"""
        codes_found = []
        
        # Special handling for IGN site structure
        if source_name == "ign":
            ign_codes = self._extract_ign_codes(soup, raw_html)
            codes_found.extend(ign_codes)
        
        # Method 1: Extract from script tags (for JavaScript-embedded data)
        script_codes = self._extract_from_scripts(soup, source_name)
        codes_found.extend(script_codes)
        
        # Method 2: Extract from data attributes and hidden elements
        data_codes = self._extract_from_data_attributes(soup, source_name)
        codes_found.extend(data_codes)
        
        # Method 3: Extract from specific HTML patterns (tables, lists, etc.)
        pattern_codes = self._extract_from_html_patterns(soup, source_name)
        codes_found.extend(pattern_codes)
        
        # Method 4: Raw HTML regex search as last resort
        if not codes_found:
            raw_codes = self._extract_from_raw_html(raw_html, source_name)
            codes_found.extend(raw_codes)
        
        # Remove duplicates
        unique_codes = {}
        for code_info in codes_found:
            if code_info.code not in unique_codes:
                unique_codes[code_info.code] = code_info
        
        return list(unique_codes.values())
    
    def _extract_from_scripts(self, soup: BeautifulSoup, source_name: str) -> List[CodeInfo]:
        """Extract codes from JavaScript/JSON in script tags"""
        codes_found = []
        
        script_tags = soup.find_all('script')
        for script in script_tags:
            if script.string:
                script_text = script.string
                
                # Look for code patterns in script content
                for match in config.code_pattern.finditer(script_text):
                    code = self._normalize_code(match.group())
                    
                    # Try to extract expiration from nearby JSON/JS data
                    expiration_date = self._extract_expiration_date(script_text, match.start())
                    
                    codes_found.append(CodeInfo(
                        code=code,
                        source=source_name,
                        expiration_date=expiration_date
                    ))
        
        return codes_found
    
    def _extract_from_data_attributes(self, soup: BeautifulSoup, source_name: str) -> List[CodeInfo]:
        """Extract codes from HTML data attributes and hidden elements"""
        codes_found = []
        
        # Search in data attributes
        for element in soup.find_all(attrs={'data-code': True}):
            code_text = element.get('data-code', '')
            if config.code_pattern.match(code_text):
                code = self._normalize_code(code_text)
                codes_found.append(CodeInfo(code=code, source=source_name))
        
        # Search in hidden input values
        for input_elem in soup.find_all('input', type='hidden'):
            value = input_elem.get('value', '')
            if config.code_pattern.match(value):
                code = self._normalize_code(value)
                codes_found.append(CodeInfo(code=code, source=source_name))
        
        return codes_found
    
    def _extract_from_html_patterns(self, soup: BeautifulSoup, source_name: str) -> List[CodeInfo]:
        """Extract codes from structured HTML patterns (tables, lists)"""
        codes_found = []
        
        # Extract from table cells
        for td in soup.find_all(['td', 'th']):
            td_text = td.get_text(strip=True)
            for match in config.code_pattern.finditer(td_text):
                code = self._normalize_code(match.group())
                
                # Try to find expiration in same row
                expiration_date = None
                row = td.find_parent('tr')
                if row:
                    row_text = row.get_text()
                    expiration_date = self._extract_expiration_date(row_text, 0)
                
                codes_found.append(CodeInfo(
                    code=code,
                    source=source_name,
                    expiration_date=expiration_date
                ))
        
        # Extract from list items
        for li in soup.find_all(['li', 'div', 'span', 'p']):
            li_text = li.get_text(strip=True)
            for match in config.code_pattern.finditer(li_text):
                code = self._normalize_code(match.group())
                expiration_date = self._extract_expiration_date(li_text, match.start())
                
                codes_found.append(CodeInfo(
                    code=code,
                    source=source_name,
                    expiration_date=expiration_date
                ))
        
        return codes_found
    
    def _extract_from_raw_html(self, raw_html: str, source_name: str) -> List[CodeInfo]:
        """Last resort: extract codes directly from raw HTML"""
        codes_found = []
        
        for match in config.code_pattern.finditer(raw_html):
            code = self._normalize_code(match.group())
            expiration_date = self._extract_expiration_date(raw_html, match.start())
            
            codes_found.append(CodeInfo(
                code=code,
                source=source_name,
                expiration_date=expiration_date
            ))
        
        return codes_found
    
    def _extract_ign_codes(self, soup: BeautifulSoup, raw_html: str) -> List[CodeInfo]:
        """Specialized extraction for IGN's wiki structure"""
        codes_found = []
        
        # IGN uses specific patterns in their wiki structure
        # Look for codes in h3/h4 headers followed by code blocks
        headers = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        
        for header in headers:
            header_text = header.get_text()
            
            # Find sections that mention codes or SHiFT
            if any(keyword in header_text.lower() for keyword in ['shift', 'code', 'key', 'reward']):
                # Look for codes in the next few siblings
                current = header.next_sibling
                search_count = 0
                
                while current and search_count < 10:  # Limit search depth
                    if hasattr(current, 'get_text'):
                        sibling_text = current.get_text()
                        
                        # Find codes in this section
                        for match in config.code_pattern.finditer(sibling_text):
                            code = self._normalize_code(match.group())
                            expiration_date = self._extract_expiration_date(sibling_text, match.start())
                            
                            codes_found.append(CodeInfo(
                                code=code,
                                source="ign",
                                expiration_date=expiration_date
                            ))
                    
                    current = current.next_sibling
                    search_count += 1
        
        # Also search for common IGN wiki patterns
        # Look for div classes commonly used in IGN wikis
        for div_class in ['wiki-content', 'entry-content', 'article-content', 'content', 'main-content']:
            content_divs = soup.find_all('div', class_=lambda x: x and div_class in str(x).lower())
            
            for div in content_divs:
                div_text = div.get_text()
                for match in config.code_pattern.finditer(div_text):
                    code = self._normalize_code(match.group())
                    expiration_date = self._extract_expiration_date(div_text, match.start())
                    
                    codes_found.append(CodeInfo(
                        code=code,
                        source="ign", 
                        expiration_date=expiration_date
                    ))
        
        # Search for codes in paragraph elements that might contain code blocks
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            p_text = p.get_text()
            if len(p_text) < 200:  # Focus on shorter paragraphs that might contain just codes
                for match in config.code_pattern.finditer(p_text):
                    code = self._normalize_code(match.group())
                    expiration_date = self._extract_expiration_date(p_text, match.start())
                    
                    codes_found.append(CodeInfo(
                        code=code,
                        source="ign",
                        expiration_date=expiration_date
                    ))
        
        # Search in any element that has "code" in its class name or id
        code_elements = soup.find_all(attrs={'class': lambda x: x and 'code' in str(x).lower()})
        code_elements.extend(soup.find_all(attrs={'id': lambda x: x and 'code' in str(x).lower()}))
        
        for element in code_elements:
            element_text = element.get_text()
            for match in config.code_pattern.finditer(element_text):
                code = self._normalize_code(match.group())
                expiration_date = self._extract_expiration_date(element_text, match.start())
                
                codes_found.append(CodeInfo(
                    code=code,
                    source="ign",
                    expiration_date=expiration_date
                ))
        
        # Raw HTML search specifically for IGN patterns
        # IGN might have codes in HTML comments or specific markup
        ign_patterns = [
            r'<[^>]*>([A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5})<[^>]*>',
            r'code["\']?:\s*["\']?([A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5})',
        ]
        
        for pattern in ign_patterns:
            matches = re.findall(pattern, raw_html, re.IGNORECASE)
            for match in matches:
                code = self._normalize_code(match)
                codes_found.append(CodeInfo(code=code, source="ign"))
        
        # More aggressive raw HTML search for IGN - search everywhere
        all_code_matches = config.code_pattern.finditer(raw_html)
        for match in all_code_matches:
            code = self._normalize_code(match.group())
            expiration_date = self._extract_expiration_date(raw_html, match.start())
            
            codes_found.append(CodeInfo(
                code=code,
                source="ign",
                expiration_date=expiration_date
            ))
        
        return codes_found
    
    def _normalize_code(self, code: str) -> str:
        """Normalize code format"""
        return re.sub(r'[^A-Z0-9-]', '', code.upper())
    
    def _extract_expiration_date(self, text: str, code_pos: int) -> Optional[datetime]:
        """Extract expiration date near a code position"""
        # Get surrounding text (400 chars before/after)
        start = max(0, code_pos - 400)
        end = min(len(text), code_pos + 400)
        surrounding_text = text[start:end]
        
        # Date patterns to try
        date_patterns = [
            r'(?:expires?|until|valid until|expires on)\s*:?\s*([A-Za-z]+ \d{1,2},? \d{4})',
            r'\[expires\s+([A-Za-z]+ \d{1,2})\]',  # GameSpot format: [Expires September 26]
            r'expires?\s+([A-Za-z]+ \d{1,2})',     # GameSpot format: Expires September 26
            r'([A-Za-z]{3,4} \d{1,2}, \d{4})',     # MentalMars format: Sept 28, 2025
            r'([A-Za-z]+ \d{1,2},? \d{4})',
            r'(\d{1,2}/\d{1,2}/\d{4})',
            r'(\d{4}-\d{2}-\d{2})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, surrounding_text, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                parsed_date = self._parse_date_string(date_str)
                if parsed_date and parsed_date > datetime.now():
                    return parsed_date
        
        return None
    
    def _parse_date_string(self, date_str: str) -> Optional[datetime]:
        """Parse various date string formats"""
        date_str = date_str.strip()
        
        formats = [
            "%B %d, %Y",    # January 15, 2025
            "%b %d, %Y",    # Jan 15, 2025
            "%B %d %Y",     # January 15 2025
            "%b %d %Y",     # Jan 15 2025
            "%m/%d/%Y",     # 01/15/2025
            "%d/%m/%Y",     # 15/01/2025
            "%Y-%m-%d",     # 2025-01-15
        ]
        
        # Try parsing with explicit year first
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Handle special abbreviated months like "Sept"
        month_replacements = {
            'Sept': 'Sep',  # September abbreviation fix
            'July': 'Jul',  # Sometimes sites use July vs Jul
        }
        
        for old_month, new_month in month_replacements.items():
            if old_month in date_str:
                modified_date = date_str.replace(old_month, new_month)
                for fmt in formats:
                    try:
                        return datetime.strptime(modified_date, fmt)
                    except ValueError:
                        continue
        
        # Try parsing without year (assume current year, or next year if past)
        year_less_formats = [
            "%B %d",        # September 26
            "%b %d",        # Sep 26
        ]
        
        for fmt in year_less_formats:
            try:
                current_year = datetime.now().year
                parsed = datetime.strptime(date_str, fmt).replace(year=current_year)
                
                # If the date has passed this year, assume next year
                if parsed < datetime.now():
                    parsed = parsed.replace(year=current_year + 1)
                
                return parsed
            except ValueError:
                continue
        
        return None

# -------------------------------
# Code Redemption
# -------------------------------

class CodeRedeemer:
    """Handles code redemption with optimized processing"""
    
    def __init__(self, session: OptimizedSession):
        self.session = session
    
    def redeem_codes_batch(self, codes_with_dates: List[Tuple[str, Optional[datetime]]]) -> List[RedemptionResult]:
        """Redeem multiple codes efficiently (codes are already pre-filtered for expiration)"""
        if not codes_with_dates:
            return []
        
        all_results = []
        game_required_encountered = False
        
        # Process each code for each platform
        for code, exp_date in codes_with_dates:
            if game_required_encountered:
                # Mark remaining codes as game_required without attempting redemption
                for platform in config.allowed_platforms:
                    if not db.is_code_redeemed(code, platform):
                        timestamp = datetime.now(timezone.utc)
                        result = RedemptionResult(
                            code=code, 
                            platform=platform, 
                            status=RedemptionStatus.GAME_REQUIRED,
                            message="Blocked - launch SHiFT-enabled game required",
                            timestamp=timestamp
                        )
                        all_results.append(result)
                        db.add_redemption(result)
                continue
            
            for platform in config.allowed_platforms:
                if db.is_code_redeemed(code, platform):
                    continue
                
                log_info(f"Attempting {Colors.BOLD}{code}{Colors.END} for {Colors.BOLD}{platform}{Colors.END}")
                result = self._redeem_single_code(code, platform)
                all_results.append(result)
                
                # Log result with appropriate colors
                if result.status == RedemptionStatus.SUCCESS:
                    log_code(code, "SUCCESS", f"redeemed for {platform}", Colors.GREEN)
                elif result.status == RedemptionStatus.ALREADY_REDEEMED:
                    log_code(code, "SKIP", f"already redeemed for {platform}", Colors.YELLOW)
                elif result.status == RedemptionStatus.EXPIRED:
                    log_code(code, "EXPIRED", "code has expired", Colors.RED)
                elif result.status == RedemptionStatus.GAME_REQUIRED:
                    log_code(code, "GAME REQUIRED", "launch SHiFT-enabled game first", Colors.YELLOW)
                    game_required_encountered = True
                    # Send Discord notification
                    self._handle_game_required_notification()
                else:
                    log_code(code, "FAILED", f"{result.message}", Colors.RED)
                
                # Add to database
                db.add_redemption(result)
                
                # Break out of platform loop if game is required
                if game_required_encountered:
                    break
        
        success_count = sum(1 for r in all_results if r.status == RedemptionStatus.SUCCESS)
        
        if success_count > 0:
            log_success(f"{success_count} codes successfully redeemed")
            
            # Check if this resolves a previous game-required block
            self._check_and_notify_resolution(all_results)
        # Don't log when no new redemptions - reduces noise
        
        return all_results
    
    def _check_and_notify_resolution(self, results: List[RedemptionResult]):
        """Check if we successfully redeemed codes after game-required block and notify"""
        if not config.discord_webhook_url:
            return
            
        # Check if any successful redemptions were for codes that had game_required status
        successful_codes = [r.code for r in results if r.status == RedemptionStatus.SUCCESS]
        if not successful_codes:
            return
            
        # Check if any of these codes previously had game_required status
        with db.get_connection() as conn:
            placeholders = ','.join(['?' for _ in successful_codes])
            cursor = conn.execute(f"""
                SELECT COUNT(DISTINCT code) FROM redemptions 
                WHERE code IN ({placeholders})
                AND status = 'game_required'
                AND datetime(ts) > datetime('now', '-7 days')
            """, successful_codes)
            
            blocked_count = cursor.fetchone()[0]
            
        if blocked_count > 0:
            # Send resolution notification
            message = (
                "**SHiFT Redemption Restored**\n\n"
                f"Successfully redeemed {len(successful_codes)} codes after resolving the game launch requirement.\n\n"
                f"**Status:** Block cleared, redemption working normally"
            )
            
            send_discord_notification(message)
            log_success(f"Sent redemption resolution notification for {len(successful_codes)} codes")
    
    def _handle_game_required_notification(self):
        """Handle the game required scenario with notifications"""
        # Only send notification if enough time has passed
        if not should_send_notification():
            log_warning("Game launch required - notification skipped (within cooldown period)")
            return
            
        pending_count = len(db.get_game_required_codes(config.allowed_platforms))
        
        message = (
            "**SHiFT Redemption Blocked**\n\n"
            "Gearbox now requires launching a SHiFT-enabled game before redeeming codes.\n\n"
            "**Action Required:**\n"
            "1. Launch any Borderlands game\n"
            "2. Sign into SHiFT in-game\n"
            "3. Script will automatically retry on next run\n\n"
            f"**Status:** {pending_count} codes waiting for redemption"
        )
        
        send_discord_notification(message)
        record_notification()
        log_warning("Game launch required - Discord notification sent")
    
    def _check_and_notify_resolution(self, results: List[RedemptionResult]):
        """Check if we successfully redeemed codes after game-required block and notify"""
        if not config.discord_webhook_url:
            return
            
        # Check if any successful redemptions were for codes that had game_required status
        successful_codes = [r.code for r in results if r.status == RedemptionStatus.SUCCESS]
        if not successful_codes:
            return
            
        # Check if any of these codes previously had game_required status
        resolution_file = config.root_dir / "game_required_resolved.txt"
        
        # Look for recent game_required redemptions that are now successful
        with db.get_connection() as conn:
            placeholders = ','.join(['?' for _ in successful_codes])
            cursor = conn.execute(f"""
                SELECT COUNT(DISTINCT code) FROM redemptions 
                WHERE code IN ({placeholders})
                AND status = 'game_required'
                AND datetime(ts) > datetime('now', '-7 days')
            """, successful_codes)
            
            blocked_count = cursor.fetchone()[0]
            
        if blocked_count > 0:
            # Send resolution notification
            message = (
                "**SHiFT Redemption Restored**\n\n"
                f"Successfully redeemed {len(successful_codes)} codes after resolving the game launch requirement.\n\n"
                f"**Status:** Block cleared, redemption working normally"
            )
            
            send_discord_notification(message)
            log_success(f"Sent redemption resolution notification for {len(successful_codes)} codes")
    
    def _redeem_single_code(self, code: str, platform: str) -> RedemptionResult:
        """Redeem a single code for a platform"""
        timestamp = datetime.now(timezone.utc)
        
        try:
            # Precheck the code
            precheck_result = self._precheck_code(code, platform)
            
            if "error" in precheck_result:
                error_msg = precheck_result["error"]
                status = self._classify_error(error_msg)
                return RedemptionResult(code, platform, status, error_msg, timestamp)
            
            form_data = precheck_result["form_data"]
            title = precheck_result.get("title", "Unknown")
            
            # Submit redemption
            result = self._submit_redemption(form_data)
            status, message = self._parse_redemption_response(result)
            
            return RedemptionResult(code, platform, status, message, timestamp)
            
        except Exception as e:
            error_msg = f"Redemption exception: {e}"
            return RedemptionResult(code, platform, RedemptionStatus.ERROR, error_msg, timestamp)
    
    def _precheck_code(self, code: str, platform: str) -> Dict[str, Any]:
        """Precheck a code to get redemption form"""
        try:
            precheck_url = f"{config.base_url}/entitlement_offer_codes?code={quote(code)}"
            
            csrf_token = self.session.get_csrf_token()
            headers = {
                "X-Requested-With": "XMLHttpRequest",
                "X-CSRF-Token": csrf_token,
                "Accept": "text/html, */*; q=0.01",
                "Referer": f"{config.base_url}/rewards",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
            
            resp = self.session.get(
                precheck_url,
                headers=headers,
                timeout=(config.connection_timeout, config.read_timeout)
            )
            
            if resp.status_code != 200:
                return {"error": f"Precheck failed with status {resp.status_code}"}
            
            return self._parse_precheck_response(resp.text, platform)
            
        except Exception as e:
            return {"error": f"Precheck exception: {e}"}
    
    def _parse_precheck_response(self, html: str, platform: str) -> Dict[str, Any]:
        """Parse precheck response for form data"""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Check for flash messages first
        flash_msg = self._extract_flash_message(html)
        if flash_msg:
            return {"error": flash_msg}
        
        # Look for redemption forms
        forms = soup.find_all('form', action='/code_redemptions')
        if not forms:
            # Check for common error indicators
            content_lower = html.lower()
            if "not found" in content_lower or "invalid" in content_lower:
                return {"error": "Code not found or invalid"}
            elif "expired" in content_lower:
                return {"error": "Code expired"}
            elif "redeemed" in content_lower:
                return {"error": "Already redeemed"}
            return {"error": "No redemption form found"}
        
        # Find form for our platform
        platform_mapping = {"steam": "steam", "xboxlive": "xboxlive", "epic": "epic", "psn": "psn", "nintendo": "nintendo"}
        target_service = platform_mapping.get(platform, platform)
        
        for form in forms:
            service_input = form.find('input', {'name': 'archway_code_redemption[service]'})
            if service_input and service_input.get('value', '').lower() == target_service.lower():
                # Extract all form data
                form_data = {}
                for input_tag in form.find_all('input'):
                    name = input_tag.get('name')
                    value = input_tag.get('value', '')
                    if name:
                        form_data[name] = value
                
                # Get title
                title_element = (form.find('input', {'name': 'archway_code_redemption[title]'}) or
                               form.find_previous('h3') or form.find_previous('h2'))
                title = (title_element.get('value') if hasattr(title_element, 'get') and title_element.get('value')
                        else title_element.get_text().strip() if title_element else 'Unknown Title')
                
                return {"form_data": form_data, "title": title}
        
        # Form found but not for our platform
        available_services = []
        for form in forms:
            service_input = form.find('input', {'name': 'archway_code_redemption[service]'})
            if service_input:
                available_services.append(service_input.get('value', ''))
        
        return {"error": f"Platform {platform} not available. Available: {available_services}"}
    
    def _extract_flash_message(self, html: str) -> Optional[str]:
        """Extract flash message from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        
        flash_selectors = ['.flash', '.alert', '.notice', '.error', '.message', '[data-flash]', '#flash', '.flash-message']
        
        for selector in flash_selectors:
            flash_elem = soup.select_one(selector)
            if flash_elem:
                text = flash_elem.get_text().strip()
                if text:
                    return text
        
        # Check for text patterns
        text_content = soup.get_text().lower()
        if "already redeemed" in text_content:
            return "Already redeemed"
        elif "expired" in text_content and "code" in text_content:
            return "Code expired"
        elif "invalid" in text_content and "code" in text_content:
            return "Invalid code"
        
        return None
    
    def _submit_redemption(self, form_data: Dict[str, str]) -> requests.Response:
        """Submit redemption form"""
        csrf_token = self.session.get_csrf_token()
        if csrf_token:
            form_data["authenticity_token"] = csrf_token
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': config.base_url,
            'Referer': f"{config.base_url}/rewards",
            'X-Requested-With': 'XMLHttpRequest',
            'Accept': 'text/vnd.turbo-stream.html, text/html;q=0.9, */*;q=0.8',
        }
        
        return self.session.post(
            f"{config.base_url}/code_redemptions",
            data=form_data,
            headers=headers,
            timeout=(config.connection_timeout, config.read_timeout),
            allow_redirects=False
        )
    
    def _parse_redemption_response(self, resp: requests.Response) -> Tuple[RedemptionStatus, str]:
        """Parse redemption response"""
        if resp.status_code == 200:
            content_type = resp.headers.get("Content-Type", "").lower()
            if "turbo-stream" in content_type or "html" in content_type:
                flash_msg = self._extract_flash_message(resp.text)
                if flash_msg:
                    return self._classify_flash_message(flash_msg), flash_msg
        elif resp.status_code in [302, 303, 307, 308]:
            # Handle redirect
            location = resp.headers.get("Location", "")
            
            if not location.startswith("http"):
                location = config.base_url + location if location.startswith("/") else f"{config.base_url}/{location}"
            
            try:
                redirect_resp = self.session.get(location, timeout=(config.connection_timeout, config.read_timeout))
                flash_msg = self._extract_flash_message(redirect_resp.text)
                if flash_msg:
                    return self._classify_flash_message(flash_msg), flash_msg
            except Exception:
                pass
        
        return RedemptionStatus.ERROR, f"HTTP {resp.status_code}: {resp.text[:200]}"
    
    def _classify_error(self, error_msg: str) -> RedemptionStatus:
        """Classify error message into status"""
        error_lower = error_msg.lower()
        if "expired" in error_lower:
            return RedemptionStatus.EXPIRED
        elif "redeemed" in error_lower:
            return RedemptionStatus.ALREADY_REDEEMED
        elif "not available" in error_lower or "platform" in error_lower:
            return RedemptionStatus.PLATFORM_UNAVAILABLE
        elif "invalid" in error_lower:
            return RedemptionStatus.INVALID
        elif "launch" in error_lower and "shift-enabled" in error_lower:
            return RedemptionStatus.GAME_REQUIRED
        else:
            return RedemptionStatus.ERROR
    
    def _classify_flash_message(self, flash_msg: str) -> RedemptionStatus:
        """Classify flash message into status"""
        flash_lower = flash_msg.lower()
        if "success" in flash_lower or ("redeemed" in flash_lower and "already" not in flash_lower):
            return RedemptionStatus.SUCCESS
        elif "already" in flash_lower:
            return RedemptionStatus.ALREADY_REDEEMED
        elif "expired" in flash_lower:
            return RedemptionStatus.EXPIRED
        elif "invalid" in flash_lower:
            return RedemptionStatus.INVALID
        elif "launch" in flash_lower and "shift-enabled" in flash_lower:
            return RedemptionStatus.GAME_REQUIRED
        else:
            return RedemptionStatus.UNKNOWN

# -------------------------------
# Main Application
# -------------------------------

class ShiftCodeManager:
    """Main application orchestrator"""
    
    def __init__(self):
        self.scraper = CodeScraper()
        self.session = OptimizedSession()
        self.redeemer = CodeRedeemer(self.session)
    
    def run(self):
        """Main execution flow"""
        log_info("Starting SHiFT code check...")
        
        # Only show config if verbose mode is enabled
        if config.verbose:
            log_config()
        
        # Process new codes
        new_count = self._process_new_codes()
        
        # Handle redemption
        unredeemed_codes = db.get_unredeemed_codes(config.allowed_platforms)
        if new_count > 0 or unredeemed_codes:
            self._redeem_pending_codes(unredeemed_codes)
        else:
            log_info("No new codes found, no pending redemptions")
        
        log_success("Completed successfully")
    
    def _process_new_codes(self) -> int:
        """Scrape and process new codes"""        
        # Scrape all sources concurrently
        all_results = self.scraper.scrape_all_sources()
        
        # Process results
        all_new_codes = []
        for source_name, codes in all_results.items():
            source_new_codes = []
            
            for code_info in codes:
                # Check if code exists
                with db.get_connection() as conn:
                    cursor = conn.execute("SELECT expiration_date FROM codes WHERE code = ?", (code_info.code,))
                    existing = cursor.fetchone()
                
                if not existing:
                    # New code
                    all_new_codes.append(code_info)
                    source_new_codes.append(code_info)
                    
                    # Format for display
                    if code_info.expiration_date:
                        years_ahead = (code_info.expiration_date - datetime.now()).days / 365
                        if years_ahead > config.permanent_code_years:
                            exp_display = "permanent (never expires)"
                        else:
                            exp_display = code_info.expiration_date.strftime("%Y-%m-%d")
                    else:
                        exp_display = "unknown expiration"
                    
                    log_code(code_info.code, "NEW", f"expires: {exp_display}", Colors.GREEN)
            
            # Summary for this source - only if there are new codes
            if source_new_codes:
                log_success(f"{len(source_new_codes)} new codes from {source_name}")
        
        # Batch add all new codes
        new_count = db.add_codes_batch(all_new_codes)
        
        if new_count > 0:
            log_section("New Codes Found")
            log_success(f"Added {new_count} new codes to database")
        # Don't log when no new codes - reduces noise
        
        return new_count
    
    def _redeem_pending_codes(self, unredeemed_codes: List[Tuple[str, Optional[datetime]]]):
        """Handle code redemption"""
        if config.no_redeem:
            log("NO_REDEEM is enabled, skipping redemption")
            return
        
        if not config.email or not config.password:
            log("ERROR: SHIFT_EMAIL and SHIFT_PASSWORD must be set in .env file")
            return
        
        # Authenticate
        if not self.session.login(config.email, config.password):
            log("ERROR: Authentication failed")
            return
        
        if not unredeemed_codes:
            log("No codes need redemption")
            return
        
        # Pre-filter expired codes before redemption attempts
        valid_codes = []
        expired_codes = []
        
        for code, exp_date in unredeemed_codes:
            code_info = CodeInfo(code=code, source="", expiration_date=exp_date)
            if code_info.is_expired():
                expired_codes.append(code)
                # Only log expired codes in verbose mode
                if config.verbose:
                    exp_str = exp_date.strftime("%Y-%m-%d") if exp_date else "unknown"
                    log_code(code, "SKIPPED", f"(expired: {exp_str})", Colors.YELLOW)
            else:
                valid_codes.append((code, exp_date))
        
        # Mark expired codes in database without attempting redemption
        if expired_codes:
            db.mark_codes_expired_batch(expired_codes, config.allowed_platforms)
            if config.verbose:
                log_info(f"Marked {len(expired_codes)} expired codes in database")
        
        if not valid_codes:
            log_info("No valid codes to redeem (all expired)")
            return
        
        log_section("Code Redemption")
        log_info(f"Redeeming {len(valid_codes)} codes on {', '.join(config.allowed_platforms)}")
        
        # Redeem valid codes
        results = self.redeemer.redeem_codes_batch(valid_codes)
        
        # Summary
        if results:
            success_count = sum(1 for r in results if r.status == RedemptionStatus.SUCCESS)
            
            if success_count > 0:
                log_success(f"{success_count} codes successfully redeemed")
            
            # Only show expired count if verbose and there are expired codes
            if config.verbose and expired_codes:
                log_info(f"Additionally marked {len(expired_codes)} codes as expired")
    
    def _clear_game_required_for_codes(self, codes: List[str]) -> int:
        """Clear game_required status for specific codes"""
        if not codes:
            return 0
        
        placeholders = ','.join(['?' for _ in codes])
        platforms_placeholders = ','.join(['?' for _ in config.allowed_platforms])
        
        with db.get_connection() as conn:
            cursor = conn.execute(f"""
                DELETE FROM redemptions 
                WHERE status = 'game_required' 
                AND code IN ({placeholders})
                AND platform IN ({platforms_placeholders})
            """, codes + config.allowed_platforms)
            conn.commit()
            return cursor.rowcount

# -------------------------------
# Entry Point
# -------------------------------

def main():
    """Application entry point"""
    import sys
    
    # Handle command line arguments
    if len(sys.argv) > 1:
        if "--clear-game-required" in sys.argv:
            cleared = db.clear_game_required_status(config.allowed_platforms)
            log_success(f"Cleared game-required status from {cleared} redemption records")
            
            # Also show how many codes are now available for redemption
            unredeemed = db.get_unredeemed_codes(config.allowed_platforms)
            log_info(f"{len(unredeemed)} codes are now available for redemption")
            
            # Send Discord notification about resolution
            if cleared > 0:
                message = (
                    f"**SHiFT Game Requirement Resolved**\n"
                    f"Cleared game-required status from {cleared} codes.\n"
                    f"{len(unredeemed)} codes are now ready for redemption."
                )
                send_discord_notification(message)
            return
        elif "--debug" in sys.argv:
            # Enable debug mode via command line
            config.debug = True
            if not config.debug_dir.exists():
                config.debug_dir.mkdir(parents=True, exist_ok=True)
            log_info("Debug mode enabled")
        elif "--help" in sys.argv or "-h" in sys.argv:
            print("SHiFT Code Scraper - Usage:")
            print("  python bshift.py                       # Normal operation")
            print("  python bshift.py --clear-game-required # Clear game-required blocks")
            print("  python bshift.py --debug               # Enable debug logging")
            print("  python bshift.py --help                # Show this help")
            return
    
    try:
        app = ShiftCodeManager()
        app.run()
    except KeyboardInterrupt:
        log("Interrupted by user")
    except Exception as e:
        log(f"Unexpected error: {e}")
        if config.verbose:
            import traceback
            log(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    main()