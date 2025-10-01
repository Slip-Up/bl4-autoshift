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
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import dotenv_values
import hashlib
import traceback
from pprint import pformat

# -------------------------------
# Version and Constants
# -------------------------------

__version__ = "1.0"

# -------------------------------
# Debug and Analysis Tools
# -------------------------------

class HTMLDebugger:
    """Advanced HTML debugging and analysis tools"""
    
    def __init__(self, save_dir="/app/logs"):
        self.save_dir = Path(save_dir)
        self.save_dir.mkdir(exist_ok=True)
    
    def save_html(self, html_content: str, prefix: str = "debug") -> str:
        """Save HTML content to file with timestamp"""
        timestamp = datetime.now().strftime("%H%M%S_%f")[:-3]  # milliseconds
        hash_suffix = hashlib.md5(html_content.encode()).hexdigest()[:8]
        filename = f"{prefix}_{timestamp}_{hash_suffix}.html"
        filepath = self.save_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            return str(filepath)
        except Exception as e:
            log_warning(f"Failed to save HTML file: {e}")
            return ""
    
    def analyze_alerts(self, html_content: str) -> Dict[str, Any]:
        """Comprehensive analysis of alert elements in HTML"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        analysis = {
            'total_alerts': 0,
            'alert_elements': [],
            'text_matches': {},
            'selectors_tested': {}
        }
        
        # Find all potential alert elements
        alert_selectors = [
            'div[class*="alert"]',
            '.alert',
            '.notice', 
            '.error',
            '.message',
            '.alert.notice',
            'div.alert.notice'
        ]
        
        for selector in alert_selectors:
            elements = soup.select(selector)
            analysis['selectors_tested'][selector] = len(elements)
            
            for elem in elements:
                elem_info = {
                    'tag': elem.name,
                    'classes': elem.get('class', []),
                    'id': elem.get('id', ''),
                    'style': elem.get('style', ''),
                    'text': elem.get_text().strip(),
                    'html': str(elem)[:200] + '...' if len(str(elem)) > 200 else str(elem),
                    'selector_found': selector
                }
                analysis['alert_elements'].append(elem_info)
        
        analysis['total_alerts'] = len(analysis['alert_elements'])
        
        # Check for specific text patterns
        text_patterns = [
            'this shift code has already been redeemed',
            'this shift code has expired',
            'this is not a valid shift code',
            'invalid code',
            'redeemed',
            'expired'
        ]
        
        html_lower = html_content.lower()
        for pattern in text_patterns:
            if pattern in html_lower:
                # Find context around the match
                match_pos = html_lower.find(pattern)
                start = max(0, match_pos - 100)
                end = min(len(html_content), match_pos + len(pattern) + 100)
                context = html_content[start:end]
                
                analysis['text_matches'][pattern] = {
                    'found': True,
                    'position': match_pos,
                    'context': context
                }
            else:
                analysis['text_matches'][pattern] = {'found': False}
        
        return analysis
    
    def format_analysis(self, analysis: Dict[str, Any]) -> str:
        """Format analysis results for readable output"""
        lines = []
        lines.append(f"=== HTML ANALYSIS RESULTS ===")
        lines.append(f"Total alert elements found: {analysis['total_alerts']}")
        
        lines.append(f"\nSelector Results:")
        for selector, count in analysis['selectors_tested'].items():
            lines.append(f"  {selector}: {count} elements")
        
        lines.append(f"\nText Pattern Matches:")
        for pattern, result in analysis['text_matches'].items():
            if result['found']:
                lines.append(f"  [+] '{pattern}' found at position {result['position']}")
                lines.append(f"    Context: {repr(result['context'][:100])}")
            else:
                lines.append(f"  [-] '{pattern}' not found")
        
        lines.append(f"\nAlert Elements Details:")
        for i, elem in enumerate(analysis['alert_elements']):
            lines.append(f"  Alert {i}:")
            lines.append(f"    Classes: {elem['classes']}")
            lines.append(f"    Text: '{elem['text']}'")
            lines.append(f"    Style: '{elem['style']}'")
            lines.append(f"    Found by: {elem['selector_found']}")
        
        return "\n".join(lines)

class DebugLogger:
    """Enhanced logging for debugging"""
    
    @staticmethod
    def log_with_context(message: str, context: Dict[str, Any] = None):
        """Log message with additional context"""
        log_warning(f"DEBUG: {message}")
        if context:
            log_warning(f"DEBUG CONTEXT: {pformat(context, width=120, depth=3)}")
    
    @staticmethod
    def log_exception(message: str, exception: Exception):
        """Log exception with full traceback"""
        log_warning(f"DEBUG EXCEPTION: {message}")
        log_warning(f"DEBUG TRACEBACK: {traceback.format_exc()}")

# Global debug tools
html_debugger = HTMLDebugger()
debug_logger = DebugLogger()

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
        self.delay_seconds = self._get_float("DELAY_SECONDS", 5.0)
        self.max_retries = self._get_int("MAX_RETRIES", 8)
        self.no_redeem = self._get_bool("NO_REDEEM", False)
        
        # Service Configuration - Required (renamed from ALLOWED_PLATFORMS for clarity)
        services_str = self._get_str("ALLOWED_SERVICES") or self._get_str("ALLOWED_PLATFORMS")  # Support both for backwards compatibility
        if not services_str:
            raise ValueError("ALLOWED_SERVICES environment variable is required. Supported services: steam, epic, psn, xboxlive, nintendo")
        self.allowed_services = [s.strip() for s in services_str.split(",")]
        
        # Validate services
        valid_services = {"steam", "epic", "psn", "xboxlive", "nintendo"}
        invalid_services = [s for s in self.allowed_services if s not in valid_services]
        if invalid_services:
            raise ValueError(f"Invalid services: {invalid_services}. Supported services: {', '.join(valid_services)}")
        
        # Title Configuration - Required
        titles_str = self._get_str("ALLOWED_TITLES")
        if not titles_str:
            raise ValueError("ALLOWED_TITLES environment variable is required. Supported titles: bl1, bl2, blps, bl3, ttw, bl4")
        
        # User-friendly abbreviations to internal code mapping
        self.title_mapping = {
            # User-friendly abbreviations
            "bl1": "mopane",      # Borderlands: Game of the Year Edition
            "bl2": "willow2",     # Borderlands 2
            "blps": "cork",       # Borderlands: The Pre-Sequel
            "bl3": "oak",         # Borderlands 3
            "ttw": "daffodil",    # Tiny Tina's Wonderlands
            "bl4": "oak2",        # Borderlands 4
            # Also accept internal codes for backwards compatibility
            "mopane": "mopane",
            "willow2": "willow2", 
            "cork": "cork",
            "oak": "oak",
            "daffodil": "daffodil",
            "oak2": "oak2"
        }
        
        # Convert user abbreviations to internal codes
        user_titles = [t.strip().lower() for t in titles_str.split(",")]
        self.allowed_titles = []
        
        for title in user_titles:
            if title in self.title_mapping:
                internal_code = self.title_mapping[title]
                if internal_code not in self.allowed_titles:  # Avoid duplicates
                    self.allowed_titles.append(internal_code)
            else:
                valid_abbrevs = [k for k in self.title_mapping.keys() if not k.startswith(('mopane', 'willow2', 'cork', 'oak', 'daffodil'))]
                raise ValueError(f"Invalid title '{title}'. Supported abbreviations: {', '.join(valid_abbrevs)}")
        
        if not self.allowed_titles:
            raise ValueError("No valid titles specified in ALLOWED_TITLES")
        
        # Create reverse mapping for display purposes
        self.title_display_names = {
            "mopane": "Borderlands 1",
            "willow2": "Borderlands 2", 
            "cork": "Borderlands: TPS",
            "oak": "Borderlands 3",
            "daffodil": "Tiny Tina's Wonderlands",
            "oak2": "Borderlands 4"
        }
        
        # Keep legacy allowed_platforms for backwards compatibility in other parts of code
        self.allowed_platforms = self.allowed_services
        
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
        self.expiration_buffer_days = 7  # Try codes for 1 week after expiration
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
    TITLE_MISMATCH = "title_mismatch"  # New status for codes that don't match user's allowed titles
    RATE_LIMITED = "rate_limited"  # New status for 429 Too Many Requests
    ERROR = "error"
    UNKNOWN = "unknown"

@dataclass
class CodeInfo:
    """Represents a SHiFT code with metadata"""
    code: str
    source: str
    expiration_date: Optional[datetime] = None
    first_seen: Optional[datetime] = None
    reward: Optional[str] = None
    
    def is_expired(self) -> bool:
        """Check if code is expired considering buffer days"""
        if not self.expiration_date:
            return False  # No expiration = never expires
        
        # Treat codes years in the future as permanent
        years_ahead = (self.expiration_date - datetime.now()).days / 365
        if years_ahead > config.permanent_code_years:
            return False
        
        # Apply buffer for timezone differences - code is valid until expiration + buffer days
        expiration_with_buffer = self.expiration_date + timedelta(days=config.expiration_buffer_days)
        return datetime.now() > expiration_with_buffer

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

def log_section(message: str, show_time: bool = False):
    width = 50
    if show_time:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        title = f"{message} - {timestamp}"
    else:
        title = message
    
    print(f"\n{Colors.CYAN}{'─' * width}{Colors.END}")
    print(f"{Colors.CYAN}{Colors.BOLD}{title}{Colors.END}")
    print(f"{Colors.CYAN}{'─' * width}{Colors.END}")

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

class DiscordNotifier:
    """
    Enhanced Discord notification system with embeds for BL4-AutoSHiFT
    
    Provides clean, professional notifications for:
    - Code redemption reports
    - Authentication failures
    - Game launch requirements
    """
    
    def __init__(self, webhook_url: str):
        """
        Initialize Discord notifier
        
        Args:
            webhook_url: Discord webhook URL for sending notifications
        """
        self.webhook_url = webhook_url
        self.username = "BL4-AutoSHiFT"
        self.avatar_url = "https://images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com/f/af7dfb2d-8cc1-45ab-9bab-6b0d0c655e58/dk06as6-6a40975f-0310-42d8-abad-625073ade92f.png?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1cm46YXBwOjdlMGQxODg5ODIyNjQzNzNhNWYwZDQxNWVhMGQyNmUwIiwiaXNzIjoidXJuOmFwcDo3ZTBkMTg4OTgyMjY0MzczYTVmMGQ0MTVlYTBkMjZlMCIsIm9iaiI6W1t7InBhdGgiOiIvZi9hZjdkZmIyZC04Y2MxLTQ1YWItOWJhYi02YjBkMGM2NTVlNTgvZGswNmFzNi02YTQwOTc1Zi0wMzEwLTQyZDgtYWJhZC02MjUwNzNhZGU5MmYucG5nIn1dXSwiYXVkIjpbInVybjpzZXJ2aWNlOmZpbGUuZG93bmxvYWQiXX0.ewSRBhW6vHnmTngv3yqRgPQqxDlOnRE1PZFiknhTY1E"
        self.footer_icon_url = self.avatar_url
        
        # Color scheme
        self.colors = {
            'success': 0x23eb5b,  # Bright green for successful redemptions
            'error': 0xe74c3c,    # Red for critical errors
            'warning': 0xf39c12,  # Orange for warnings/action required
            'info': 0x3498db,     # Blue for informational
            'neutral': 0x95a5a6   # Gray for neutral updates
        }
    
    def send_embed(self, title: str, description: str = None, color: str = 'info', 
                   fields: list = None, footer: str = None, timestamp: bool = True):
        """
        Send a Discord embed message
        
        Args:
            title: Main title of the embed
            description: Optional description text
            color: Color key ('success', 'error', 'warning', 'info', 'neutral')
            fields: List of dicts with 'name' and 'value' keys
            footer: Optional footer text
            timestamp: Whether to include timestamp
        """
        embed = {
            'title': title,
            'color': self.colors.get(color, self.colors['info'])
        }
        
        if description:
            embed['description'] = description
        
        if fields:
            embed['fields'] = fields
        
        if footer:
            embed['footer'] = {
                'text': footer,
                'icon_url': self.footer_icon_url
            }
        
        if timestamp:
            embed['timestamp'] = datetime.now(timezone.utc).isoformat()
        
        payload = {
            'username': self.username,
            'avatar_url': self.avatar_url,
            'embeds': [embed]
        }
        
        try:
            response = requests.post(self.webhook_url, json=payload, timeout=10)
            
            if response.status_code == 204:
                return True
            else:
                log_warning(f"Discord notification failed: {response.status_code}")
                return False
        except Exception as e:
            log_warning(f"Discord notification error: {e}")
            return False
    
    def authentication_failed(self, error_message: str = None):
        """
        Send notification when authentication fails
        
        Args:
            error_message: Optional specific error message
        """
        description = "Failed to authenticate with SHiFT. The bot cannot redeem codes until this is resolved."
        
        fields = [
            {
                'name': 'Action Required',
                'value': 'Check your SHIFT_EMAIL and SHIFT_PASSWORD environment variables',
                'inline': False
            }
        ]
        
        if error_message:
            fields.append({
                'name': 'Error Details',
                'value': f"```{error_message[:500]}```",  # Truncate long errors
                'inline': False
            })
        
        return self.send_embed(
            title='Authentication Failed',
            description=description,
            color='error',
            fields=fields,
            footer='BL4-AutoSHiFT'
        )
    
    def game_launch_required(self, blocked_codes: list = None):
        """
        Send notification when SHiFT requires launching a game before more redemptions
        
        Args:
            blocked_codes: Optional list of codes that couldn't be redeemed
        """
        description = "SHiFT is requiring you to launch a Borderlands game before redeeming more codes."
        
        fields = [
            {
                'name': 'Action Required',
                'value': 'Launch any Borderlands game. The bot will automatically retry these codes on the next run.',
                'inline': False
            }
        ]
        
        if blocked_codes:
            # Show up to 5 blocked codes
            codes_display = blocked_codes[:5]
            codes_text = '\n'.join([f"`{code}`" for code in codes_display])
            
            if len(blocked_codes) > 5:
                codes_text += f"\n... and {len(blocked_codes) - 5} more"
            
            fields.append({
                'name': f'Blocked Codes ({len(blocked_codes)})',
                'value': codes_text,
                'inline': False
            })
        
        return self.send_embed(
            title='Game Launch Required',
            description=description,
            color='warning',
            fields=fields,
            footer='BL4-AutoSHiFT'
        )
    
    def codes_redeemed_report(self, redemptions: list):
        """
        Send aggregate report of redeemed codes
        
        Args:
            redemptions: List of dicts with keys: code, titles, services, rewards
                Example: [
                    {'code': 'XXXXX-XXXXX-...', 'titles': ['Borderlands 4'], 'services': ['Steam', 'Epic'], 'rewards': '1 Golden Key'},
                    {'code': 'YYYYY-YYYYY-...', 'titles': ['Borderlands 3', 'Borderlands 4'], 'services': ['Steam'], 'rewards': '5 Golden Keys'}
                ]
        """
        if not redemptions:
            return False
        
        # Create description with summary
        count = len(redemptions)
        description = f"Successfully redeemed {count} code{'s' if count != 1 else ''}"
        
        # Build fields for each code
        fields = []
        
        for i, redemption in enumerate(redemptions, 1):
            code = redemption['code']
            titles = redemption.get('titles', ['Unknown'])
            services = redemption.get('services', ['Unknown'])
            rewards = redemption.get('rewards', 'N/A')
            
            # Format titles and services as comma-separated lists (filter out None values)
            titles_str = ', '.join([t for t in titles if t])
            services_str = ', '.join([s for s in services if s])
            
            # Format value with all details (excluding code since it's in the title)
            value_lines = [
                f"**Titles:** {titles_str}",
                f"**Services:** {services_str}",
                f"**Rewards:** {rewards}"
            ]
            
            fields.append({
                'name': f"`{code}`",
                'value': '\n'.join(value_lines),
                'inline': False
            })
        
        return self.send_embed(
            title='SHiFT Codes Redeemed',
            description=description,
            color='success',
            fields=fields,
            footer='BL4-AutoSHiFT'
        )


def send_discord_notification(message: str, urgent: bool = False):
    """Send notification to Discord webhook if configured (legacy function, prefer DiscordNotifier)"""
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
    print("")
    print(f"{Colors.CYAN}Configuration:{Colors.END}")
    
    # Use clean indented format
    print(f"  {Colors.CYAN}Verbose Mode:{Colors.END} {Colors.BOLD}{config.verbose}{Colors.END}")
    print(f"  {Colors.CYAN}Delay Between Requests:{Colors.END} {Colors.BOLD}{config.delay_seconds}s{Colors.END}")
    print(f"  {Colors.CYAN}Max Retries:{Colors.END} {Colors.BOLD}{config.max_retries}{Colors.END}")
    print(f"  {Colors.CYAN}Redeem Mode:{Colors.END} {Colors.BOLD}{'Disabled' if config.no_redeem else 'Enabled'}{Colors.END}")
    print(f"  {Colors.CYAN}Target Services:{Colors.END} {Colors.BOLD}{', '.join(config.allowed_services)}{Colors.END}")
    friendly_titles = [config.title_display_names.get(title, title) for title in config.allowed_titles]
    print(f"  {Colors.CYAN}Target Titles:{Colors.END} {Colors.BOLD}{', '.join(friendly_titles)}{Colors.END}")
    
    if config.no_redeem:
        print(f"  {Colors.YELLOW}[WARN] Redemption is disabled - codes will only be scraped{Colors.END}")
    elif config.email:
        email_masked = f"{config.email[:3]}***@{config.email.split('@')[1]}"
        print(f"  {Colors.CYAN}SHiFT Account:{Colors.END} {Colors.BOLD}{email_masked}{Colors.END}")
    else:
        print(f"  {Colors.YELLOW}[WARN] No SHiFT credentials configured{Colors.END}")
    print("")

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
            # Enable features and set pragmas
            conn.executescript("""
                PRAGMA foreign_keys=ON;
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
            """)
            
            # Create or migrate schema
            self._migrate_schema(conn)
            
            # Check for v3 migration (reward column)
            cursor = conn.execute("SELECT MAX(version) FROM db_version")
            result = cursor.fetchone()
            current_version = result[0] if result else 0
            if current_version and current_version < 3:
                self.migrate_to_v3(conn)
    
    def _migrate_schema(self, conn):
        """Handle database schema migration from older versions"""
        # Check if this is a v0.1 database (no version table)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='db_version'")
        has_version_table = cursor.fetchone() is not None
        
        if not has_version_table:
            # Check if v0.1 tables exist
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='codes'")
            has_codes_table = cursor.fetchone() is not None
            
            if has_codes_table:
                log_info("Migrating database from v0.1 to current version...")
                self._migrate_from_v01(conn)
            else:
                # Brand new database
                self._create_fresh_schema(conn)
        else:
            # Check version and migrate if needed
            cursor = conn.execute("SELECT version FROM db_version ORDER BY id DESC LIMIT 1")
            row = cursor.fetchone()
            current_version = row[0] if row else 1
            
            if current_version < 2:
                self._migrate_to_v2(conn)
    
    def _migrate_from_v01(self, conn):
        """Migrate from v0.1 schema to current schema"""
        try:
            # v0.1 had basic codes and redemptions tables
            # Add the titles column to codes table (ignore error if exists)
            try:
                conn.execute("ALTER TABLE codes ADD COLUMN titles TEXT")
            except:
                pass  # Column might already exist
            
            # Add the new code_availability table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS code_availability (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    service TEXT NOT NULL,
                    title TEXT NOT NULL,
                    game_name TEXT,
                    checked_ts TEXT NOT NULL,
                    FOREIGN KEY(code) REFERENCES codes(code) ON DELETE CASCADE,
                    UNIQUE(code, service, title)
                )
            """)
            
            # Add the config_tracking table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS config_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    allowed_services TEXT NOT NULL,
                    allowed_titles TEXT NOT NULL,
                    last_used_ts TEXT NOT NULL
                )
            """)
            
            # Add new indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_code_availability_code ON code_availability(code)")
            
            # Create version table and mark as v2
            conn.execute("CREATE TABLE db_version (id INTEGER PRIMARY KEY AUTOINCREMENT, version INTEGER, migrated_ts TEXT)")
            conn.execute("INSERT INTO db_version (version, migrated_ts) VALUES (2, ?)", (datetime.now(timezone.utc).isoformat(),))
            
            conn.commit()
            log_success("Database migrated from v0.1 to v2.0")
        except Exception as e:
            log_error(f"Migration from v0.1 failed: {e}")
            raise
    
    def _migrate_to_v2(self, conn):
        """Migrate to version 2 (adds code_availability tracking)"""
        try:
            # Add titles column to codes table if it doesn't exist
            try:
                conn.execute("ALTER TABLE codes ADD COLUMN titles TEXT")
            except:
                pass  # Column might already exist
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS code_availability (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    service TEXT NOT NULL,
                    title TEXT NOT NULL,
                    game_name TEXT,
                    checked_ts TEXT NOT NULL,
                    FOREIGN KEY(code) REFERENCES codes(code) ON DELETE CASCADE,
                    UNIQUE(code, service, title)
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS config_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    allowed_services TEXT NOT NULL,
                    allowed_titles TEXT NOT NULL,
                    last_used_ts TEXT NOT NULL
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_code_availability_code ON code_availability(code)")
            conn.execute("UPDATE db_version SET version = 2, migrated_ts = ? WHERE id = (SELECT MAX(id) FROM db_version)", (datetime.now(timezone.utc).isoformat(),))
            
            conn.commit()
            log_success("Database migrated to v2.0")
        except Exception as e:
            log_error(f"Migration to v2 failed: {e}")
            raise
    
    def _create_fresh_schema(self, conn):
        """Create fresh database schema for new installations"""
        conn.executescript("""
            CREATE TABLE codes (
                code TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                first_seen_ts TEXT NOT NULL,
                expiration_date TEXT,
                titles TEXT,
                reward TEXT
            );
            
            CREATE TABLE redemptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                platform TEXT NOT NULL,
                ts TEXT NOT NULL,
                status TEXT NOT NULL,
                detail TEXT,
                FOREIGN KEY(code) REFERENCES codes(code) ON DELETE CASCADE
            );
            
            CREATE TABLE code_availability (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                service TEXT NOT NULL,
                title TEXT NOT NULL,
                game_name TEXT,
                checked_ts TEXT NOT NULL,
                FOREIGN KEY(code) REFERENCES codes(code) ON DELETE CASCADE,
                UNIQUE(code, service, title)
            );
            
            CREATE TABLE config_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                allowed_services TEXT NOT NULL,
                allowed_titles TEXT NOT NULL,
                last_used_ts TEXT NOT NULL
            );
            
            CREATE TABLE db_version (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version INTEGER,
                migrated_ts TEXT
            );
            
            CREATE INDEX idx_redemptions_code_platform ON redemptions(code, platform);
            CREATE INDEX idx_code_availability_code ON code_availability(code);
            CREATE INDEX idx_codes_expiration ON codes(expiration_date);
        """)
        
        # Insert version record with parameter binding
        conn.execute("INSERT INTO db_version (version, migrated_ts) VALUES (3, ?)", 
                    (datetime.now(timezone.utc).isoformat(),))
        
        conn.commit()
    
    def migrate_to_v3(self, conn):
        """Add reward column to existing database"""
        try:
            # Check if reward column exists
            cursor = conn.execute("PRAGMA table_info(codes)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'reward' not in columns:
                log_info("Migrating database to version 3: adding reward column")
                conn.execute("ALTER TABLE codes ADD COLUMN reward TEXT")
                conn.execute("UPDATE db_version SET version = 3, migrated_ts = ? WHERE id = (SELECT MAX(id) FROM db_version)",
                           (datetime.now(timezone.utc).isoformat(),))
                conn.commit()
                log_info("[OK] Database migrated to version 3")
        except Exception as e:
            log_warning(f"Migration to v3 failed: {e}")
    
    def check_and_update_configuration(self):
        """Check if configuration changed and clear availability data if needed"""
        current_services = ','.join(sorted(config.allowed_services))
        current_titles = ','.join(sorted(config.allowed_titles))
        
        with self.get_connection() as conn:
            # Check if config_tracking table exists first
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='config_tracking'")
            table_exists = cursor.fetchone() is not None
            
            if not table_exists:
                # Create the missing table (migration issue)
                conn.execute("""
                    CREATE TABLE config_tracking (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        allowed_services TEXT NOT NULL,
                        allowed_titles TEXT NOT NULL,
                        last_used_ts TEXT NOT NULL
                    )
                """)
                log_info("Created missing config_tracking table")
            
            # Get the most recent configuration
            cursor = conn.execute("""
                SELECT allowed_services, allowed_titles 
                FROM config_tracking 
                ORDER BY id DESC LIMIT 1
            """)
            row = cursor.fetchone()
            
            config_changed = False
            if row:
                last_services, last_titles = row
                if last_services != current_services or last_titles != current_titles:
                    config_changed = True
                    log_info(f"Configuration changed from ({last_services}, {last_titles}) to ({current_services}, {current_titles}) - clearing availability data for efficiency")
                    # Clear availability data since the configuration changed
                    conn.execute("DELETE FROM code_availability")
                else:
                    if config.verbose:
                        log_info(f"Configuration unchanged: {current_services}, {current_titles}")
            else:
                # First run, no previous config
                config_changed = True
                if config.verbose:
                    log_info(f"First run - initializing configuration tracking: {current_services}, {current_titles}")
            
            # Update configuration tracking
            if config_changed:
                conn.execute("""
                    INSERT INTO config_tracking (allowed_services, allowed_titles, last_used_ts)
                    VALUES (?, ?, ?)
                """, (current_services, current_titles, datetime.now(timezone.utc).isoformat()))
                conn.commit()

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
                    elif not code_info.expiration_date and existing_exp:
                        # Keep existing expiration if new code doesn't have one
                        pass
                    
                    # Always update reward if we have new reward data
                    if code_info.reward:
                        conn.execute(
                            "UPDATE codes SET reward = ? WHERE code = ?",
                            (code_info.reward, code_info.code)
                        )
                else:
                    # New code
                    now = datetime.now(timezone.utc).isoformat()
                    exp_str = code_info.expiration_date.isoformat() if code_info.expiration_date else None
                    conn.execute(
                        "INSERT INTO codes (code, source, first_seen_ts, expiration_date, reward) VALUES (?, ?, ?, ?, ?)",
                        (code_info.code, code_info.source, now, exp_str, code_info.reward)
                    )
                    new_count += 1
            
            conn.commit()
        
        return new_count
    
    def get_unredeemed_codes(self, platforms: List[str]) -> List[Tuple[str, Optional[datetime], Optional[str]]]:
        """Get codes that need redemption - either unchecked or matching current user config"""
        
        with self.get_connection() as conn:
            # Get codes where at least one platform hasn't been successfully redeemed yet
            # Skip codes that have universal statuses (expired, title_mismatch) on ANY platform
            # These statuses apply to all platforms/services, so no need to retry
            
            placeholders = ','.join(['?' for _ in platforms])
            cursor = conn.execute(f"""
                SELECT DISTINCT c.code, c.expiration_date, c.titles, c.reward
                FROM codes c 
                WHERE 
                    -- Exclude codes with universal failure statuses (expired, title_mismatch)
                    NOT EXISTS (
                        SELECT 1 FROM redemptions r
                        WHERE r.code = c.code
                        AND r.status IN ('expired', 'title_mismatch')
                    )
                    -- Only include codes where not all platforms have been successfully redeemed
                    AND (
                        SELECT COUNT(DISTINCT r.platform)
                        FROM redemptions r
                        WHERE r.code = c.code
                        AND r.platform IN ({placeholders})
                        AND r.status IN ('success', 'already_redeemed')
                    ) < ?
                ORDER BY c.first_seen_ts DESC
            """, platforms + [len(platforms)])
            
            results = []
            # Convert internal codes back to user abbreviations for comparison
            user_allowed_abbreviations = set()
            for internal_code in config.allowed_titles:
                # Find the abbreviation that maps to this internal code
                for abbrev, mapped_internal in config.title_mapping.items():
                    if mapped_internal == internal_code and len(abbrev) <= 4:  # Use short abbreviations only
                        user_allowed_abbreviations.add(abbrev)
                        break
            
            for row in cursor.fetchall():
                code = row[0]
                exp_str = row[1]
                titles_csv = row[2]
                reward = row[3]
                
                exp_date = datetime.fromisoformat(exp_str) if exp_str else None
                
                # Check if we should process this code
                should_process = False
                
                if not titles_csv:
                    # No titles stored yet - needs to be checked
                    should_process = True
                else:
                    # Check if any of the code's titles match user's allowed titles
                    code_titles = set(titles_csv.split(','))
                    if code_titles & user_allowed_abbreviations:  # Intersection
                        should_process = True
                
                if should_process:
                    results.append((code, exp_date, reward))
            
            # Removed verbose debug logging
            
            if len(results) > 0:
                print("")
                log(f"Filtered {len(results)} codes to process from database")
                print("")
            return results
    
    def get_codes_matching_current_config(self, platforms: List[str]) -> List[Tuple[str, Optional[datetime]]]:
        """Get codes that we know work with current user configuration"""
        placeholders = ','.join(['?' for _ in platforms])
        
        # Create parameters for current service/title combinations
        service_title_params = []
        for service in config.allowed_services:
            for title in config.allowed_titles:
                service_title_params.extend([service, title])
        
        service_title_placeholders = ','.join(['(?,?)' for _ in range(len(config.allowed_services) * len(config.allowed_titles))])
        
        with self.get_connection() as conn:
            cursor = conn.execute(f"""
                SELECT DISTINCT c.code, c.expiration_date
                FROM codes c 
                INNER JOIN code_availability ca ON c.code = ca.code
                WHERE (ca.service, ca.title) IN ({service_title_placeholders})
                AND ca.game_name IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM redemptions r 
                    WHERE r.code = c.code 
                    AND r.platform IN ({placeholders})
                    AND r.status IN ('success', 'already_redeemed', 'expired')
                )
                ORDER BY c.first_seen_ts DESC
            """, service_title_params + platforms)
            
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
    
    def store_code_availability(self, code: str, combinations: List[Dict]) -> None:
        """Store available service/title combinations for a code"""
        with self.get_connection() as conn:
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # Clear existing availability data for this code
            conn.execute("DELETE FROM code_availability WHERE code = ?", (code,))
            
            # Insert availability data for what the code actually works for
            for combo in combinations:
                conn.execute("""
                    INSERT OR REPLACE INTO code_availability 
                    (code, service, title, game_name, checked_ts) 
                    VALUES (?, ?, ?, ?, ?)
                """, (code, combo['service'], combo['title'], combo['game_name'], timestamp))
            
            # ALSO store that we checked this code against current user configuration
            # This ensures we don't check the same code again for the same user config
            for service in config.allowed_services:
                for title in config.allowed_titles:
                    conn.execute("""
                        INSERT OR REPLACE INTO code_availability 
                        (code, service, title, game_name, checked_ts) 
                        VALUES (?, ?, ?, NULL, ?)
                    """, (code, service, title, timestamp))
            
            conn.commit()
    
    def update_code_titles(self, code: str, combinations: List[Dict]) -> None:
        """Update code's titles field with CSV of abbreviations it works for"""
        # Create a comprehensive mapping from game names to abbreviations
        game_name_mappings = {
            # Borderlands 1 variants
            "borderlands: game of the year edition": "bl1",
            "borderlands goty": "bl1", 
            "borderlands": "bl1",
            
            # Borderlands 2 variants
            "borderlands 2": "bl2",
            "bl2": "bl2",
            
            # Borderlands: The Pre-Sequel variants
            "borderlands: the pre-sequel": "blps",
            "borderlands pre-sequel": "blps",
            "borderlands tps": "blps",
            
            # Borderlands 3 variants
            "borderlands 3": "bl3",
            "bl3": "bl3",
            
            # Tiny Tina's Wonderlands variants
            "tiny tina's wonderlands": "ttw",
            "wonderlands": "ttw",
            "ttw": "ttw",
            
            # Borderlands 4 variants  
            "borderlands 4": "bl4",
            "bl4": "bl4"
        }
        
        abbreviations = []
        for combo in combinations:
            game_name = combo.get('game_name', '').lower()
            
            # Try exact matches first
            abbrev = game_name_mappings.get(game_name)
            
            # If no exact match, try partial matches
            if not abbrev:
                for game_pattern, abbr in game_name_mappings.items():
                    if game_pattern in game_name or game_name in game_pattern:
                        abbrev = abbr
                        break
            
            if abbrev and abbrev not in abbreviations:
                abbreviations.append(abbrev)
        
        # Store as CSV
        titles_csv = ','.join(abbreviations) if abbreviations else ''
        
        with self.get_connection() as conn:
            conn.execute("UPDATE codes SET titles = ? WHERE code = ?", (titles_csv, code))
            conn.commit()
    
    def get_code_availability(self, code: str) -> List[Dict]:
        """Get available service/title combinations for a code"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT service, title, game_name FROM code_availability 
                WHERE code = ?
            """, (code,))
            
            return [
                {'service': row[0], 'title': row[1], 'game_name': row[2]}
                for row in cursor.fetchall()
            ]
    
    def get_codes_needing_redemption_for_new_config(self) -> List[str]:
        """Get codes that have availability for current config but haven't been redeemed yet"""
        with self.get_connection() as conn:
            # Build query to find codes that:
            # 1. Have availability data for current allowed services/titles
            # 2. Haven't been successfully redeemed for those service/title combinations
            
            service_placeholders = ','.join(['?' for _ in config.allowed_services])
            title_placeholders = ','.join(['?' for _ in config.allowed_titles])
            
            cursor = conn.execute(f"""
                SELECT DISTINCT ca.code
                FROM code_availability ca
                LEFT JOIN codes c ON ca.code = c.code
                WHERE ca.service IN ({service_placeholders})
                AND ca.title IN ({title_placeholders})
                AND (c.expiration_date IS NULL OR c.expiration_date > datetime('now'))
                AND NOT EXISTS (
                    SELECT 1 FROM redemptions r 
                    WHERE r.code = ca.code 
                    AND r.platform = ca.service 
                    AND r.status IN ('success', 'already_redeemed')
                )
            """, config.allowed_services + config.allowed_titles)
            
            return [row[0] for row in cursor.fetchall()]

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
                
                # Clear existing cookies first to avoid duplicates
                self.session.cookies.clear()
                
                # Properly set cookies with domain/path information
                for name, value in session_data.get('cookies', {}).items():
                    self.session.cookies.set(
                        name=name,
                        value=value,
                        domain='.gearboxsoftware.com',
                        path='/'
                    )
                
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
                    if config.verbose:
                        print(f"{Colors.GREEN}[OK] Authentication verified{Colors.END}")
                else:
                    print(f"{Colors.RED}[FAIL] Authentication failed{Colors.END}")
                
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
            if config.verbose:
                print(f"{Colors.GREEN}[OK] Already authenticated{Colors.END}")
            return True
        
        if config.verbose:
            print(f"{Colors.CYAN}[INFO] Logging in...{Colors.END}")
        
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
                print(f"{Colors.GREEN}[OK] Successfully logged into SHiFT{Colors.END}")
                return True
            else:
                print(f"{Colors.RED}[FAIL] Login failed - authentication verification failed{Colors.END}")
                return False
                
        except Exception as e:
            print(f"{Colors.RED}[FAIL] Login failed: {e}{Colors.END}")
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
            ("https://mentalmars.com/game-news/borderlands-golden-keys/", "mentalmars-golden"),
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
                    # Logging moved to _process_new_codes() to show only new codes
                        
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
        # Debug settings from global config
        self.debug = config.debug
        self.debug_dir = config.debug_dir
        if self.debug:
            self.debug_dir.mkdir(parents=True, exist_ok=True)
            log_warning(f"DEBUG: Flash message debug directory initialized at {self.debug_dir}")
        self.html_debugger = HTMLDebugger(save_dir=str(self.debug_dir if self.debug else Path('/app/logs')))
        # Track suspicious invalid streaks that likely indicate rate limiting
        self._suspect_invalid_streak = 0
        self._last_suspect_ts = 0.0
        # Track last time we observed a 429 to gate invalid classifications during throttle windows
        self._last_429_ts = 0.0
        # Track last time a precheck returned valid combinations (to bias against sudden invalid)
        self._last_valid_precheck_ts = 0.0
    
    def _deduplicate_cookies(self):
        """Remove duplicate cookies from session to prevent 'multiple cookies with name' error"""
        from http.cookiejar import CookieJar
        
        # Get current cookies
        current_cookies = list(self.session.session.cookies)
        
        # Track seen cookie names (keep only the last occurrence)
        seen = {}
        for cookie in current_cookies:
            seen[cookie.name] = cookie
        
        # Clear and re-add unique cookies
        self.session.session.cookies.clear()
        for cookie in seen.values():
            self.session.session.cookies.set_cookie(cookie)
    
    def redeem_codes_batch(self, codes_with_dates: List[Tuple[str, Optional[datetime]]]) -> List[RedemptionResult]:
        """Redeem multiple codes efficiently (codes are already pre-filtered for expiration)"""
        if not codes_with_dates:
            return []
        
        # Start with a clean redemption page to avoid stale messages
        self._refresh_redemption_page()
        
        all_results = []
        game_required_encountered = False
        total_codes = len(codes_with_dates)
        
        # Process each code for all allowed service/title combinations
        for code_index, (code, exp_date) in enumerate(codes_with_dates, 1):
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
            
            # Check if code is already fully redeemed (all platforms)
            already_redeemed_count = sum(1 for platform in config.allowed_platforms if db.is_code_redeemed(code, platform))
            if already_redeemed_count == len(config.allowed_platforms):
                continue
            
            print(f"\n{Colors.CYAN}[Attempting {code_index}/{total_codes}]{Colors.END} {Colors.BOLD}{code}{Colors.END}")
            results = self._redeem_code_combinations(code)
            all_results.extend(results)
            
            # Log results and check for game required status
            for result in results:
                # Format platform name nicely
                platform_display = result.platform.capitalize().ljust(10)
                
                # Log result with appropriate colors and tree-like structure
                if result.status == RedemptionStatus.SUCCESS:
                    print(f"  {Colors.GREEN}{platform_display}{Colors.END} {Colors.GREEN}{result.message}{Colors.END}")
                elif result.status == RedemptionStatus.ALREADY_REDEEMED:
                    print(f"  {Colors.YELLOW}{platform_display}{Colors.END} {Colors.GRAY}already redeemed{Colors.END}")
                elif result.status == RedemptionStatus.EXPIRED:
                    print(f"  {Colors.RED}{platform_display}{Colors.END} {Colors.RED}expired{Colors.END}")
                elif result.status == RedemptionStatus.RATE_LIMITED:
                    print(f"  {Colors.YELLOW}{platform_display}{Colors.END} {Colors.YELLOW}rate limited - waiting{Colors.END}")
                elif result.status == RedemptionStatus.PLATFORM_UNAVAILABLE:
                    print(f"  {Colors.BLUE}{platform_display}{Colors.END} {Colors.GRAY}{result.message}{Colors.END}")
                elif result.status == RedemptionStatus.TITLE_MISMATCH:
                    print(f"  {Colors.BLUE}{platform_display}{Colors.END} {Colors.GRAY}{result.message}{Colors.END}")
                elif result.status == RedemptionStatus.GAME_REQUIRED:
                    print(f"  {Colors.YELLOW}{platform_display}{Colors.END} {Colors.YELLOW}launch game first{Colors.END}")
                    game_required_encountered = True
                    # Send Discord notification
                    self._handle_game_required_notification()
                else:
                    print(f"  {Colors.RED}[-] {platform_display}{Colors.END} {Colors.RED}{result.message}{Colors.END}")
                
                # Add to database
                db.add_redemption(result)
            
            # Break out of code loop if game is required
            if game_required_encountered:
                break
        
        success_count = sum(1 for r in all_results if r.status == RedemptionStatus.SUCCESS)
        
        if success_count > 0:
            print(f"\n{Colors.GREEN}[OK] Successfully redeemed {success_count} code{'s' if success_count != 1 else ''}{Colors.END}")
            
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
        
        if not config.discord_webhook_url:
            return
            
        # Get blocked codes
        blocked_codes = [code for code, _ in db.get_game_required_codes(config.allowed_platforms)]
        
        # Send notification with new Discord notifier
        notifier = DiscordNotifier(config.discord_webhook_url)
        notifier.game_launch_required(blocked_codes=blocked_codes)
        
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
    
    def _redeem_code_combinations(self, code: str) -> List[RedemptionResult]:
        """Redeem a code for all allowed service/title combinations"""
        timestamp = datetime.now(timezone.utc)
        results = []
        
        try:
            # Precheck the code to get all available combinations
            precheck_result = self._precheck_code(code)
            
            if "error" in precheck_result:
                error_msg = precheck_result["error"]
                status = self._classify_error(error_msg)
                
                # If we got rate limited, back off for 60 seconds then retry the same code
                if status == RedemptionStatus.RATE_LIMITED:
                    log_warning(f"Rate limited for code {code}, backing off for 60 seconds then retrying same code")
                    time.sleep(60)
                    # Refresh the page to clear any stale state
                    self._refresh_redemption_page()
                    
                    # Retry the same code once after backoff
                    log_warning(f"Retrying precheck for {code} after rate limit backoff")
                    retry_precheck_result = self._precheck_code(code)
                    
                    if "error" not in retry_precheck_result:
                        # Success on retry, continue with normal redemption flow
                        precheck_result = retry_precheck_result
                    else:
                        # Still failed on retry, return rate limited status
                        log_warning(f"Precheck still failed for {code} after retry")
                        for platform in config.allowed_platforms:
                            results.append(RedemptionResult(code, platform, status, error_msg, timestamp))
                        return results
                else:
                    # Non rate-limit error, return immediately
                    for platform in config.allowed_platforms:
                        results.append(RedemptionResult(code, platform, status, error_msg, timestamp))
                    return results
            
            # Store availability data for this code
            available_combinations = precheck_result["combinations"]
            if available_combinations:
                db.store_code_availability(code, available_combinations)
                db.update_code_titles(code, available_combinations)
            
            # Filter combinations by user's allowed services and titles
            valid_combinations = []
            
            for combo in available_combinations:
                service = combo['service']
                title = combo['title']
                
                if service in config.allowed_services and title in config.allowed_titles:
                    valid_combinations.append(combo)
            
            if not valid_combinations:
                # Show which titles this code IS valid for
                available_titles = []
                seen_titles = set()  # Track unique titles to avoid duplicates
                
                for combo in available_combinations:
                    title_code = combo['title']
                    game_name = combo['game_name']
                    
                    # Skip if we've already processed this title
                    if title_code in seen_titles:
                        continue
                    seen_titles.add(title_code)
                    
                    # Get user-friendly abbreviation for this title
                    user_abbrev = None
                    for abbrev, internal in config.title_mapping.items():
                        if internal == title_code and not abbrev.startswith(('mopane', 'willow2', 'cork', 'oak', 'daffodil')):
                            user_abbrev = abbrev
                            break
                    available_titles.append({'title': title_code, 'game_name': game_name, 'abbrev': user_abbrev})
                
                if available_titles:
                    # Show only abbreviations for concise output
                    abbrev_list = []
                    for title_info in available_titles:
                        if title_info['abbrev']:
                            abbrev_list.append(title_info['abbrev'])
                        else:
                            # Fallback to game name if no abbreviation found
                            abbrev_list.append(title_info['game_name'])
                    error_msg = f"Code valid for: {', '.join(abbrev_list)}"
                else:
                    error_msg = f"Code not valid for any service/title combinations"
                
                for platform in config.allowed_platforms:
                    results.append(RedemptionResult(code, platform, RedemptionStatus.TITLE_MISMATCH, error_msg, timestamp))
                return results
            
            # Redeem for each valid combination
            for combo in valid_combinations:
                service = combo['service']
                title = combo['title']
                game_name = combo['game_name']
                form_data = combo['form_data']
                
                try:
                    # Submit redemption
                    result = self._submit_redemption(form_data)
                    status, message = self._parse_redemption_response(result)

                    # If precheck proved this code has valid combinations, but redemption reports invalid,
                    # only treat as suspicious rate limiting if it's a generic "invalid" without legitimate context.
                    # Do NOT treat legitimate statuses like ALREADY_REDEEMED, EXPIRED as suspicious.
                    if (available_combinations and 
                        status == RedemptionStatus.INVALID and 
                        message and
                        # Only proceed if this isn't a clearly legitimate error
                        status not in [RedemptionStatus.ALREADY_REDEEMED, RedemptionStatus.EXPIRED]):
                        
                        message_lower = message.lower()
                        
                        # Check if this looks like a legitimate specific error vs. a generic rate-limit-induced error
                        legitimate_error_patterns = [
                            'already redeemed', 'already been redeemed', 'this shift code has already been redeemed',
                            'expired', 'this shift code has expired', 'code expired',
                            'invalid shift code', 'this is not a valid shift code',
                            'code not found', 'not found'
                        ]
                        
                        # Generic messages that might indicate rate limiting
                        generic_invalid_patterns = [
                            'invalid code',  # Generic message that could be rate limiting
                            'error occurred'  # Generic error
                        ]
                        
                        is_legitimate_specific = any(pattern in message_lower for pattern in legitimate_error_patterns)
                        is_generic_invalid = any(pattern in message_lower for pattern in generic_invalid_patterns)
                        
                        # Only treat as suspicious if it's a generic invalid message after successful precheck
                        # BUT: Most "Invalid code" responses after successful precheck are actually legitimate errors
                        # (like "already redeemed") that are being misclassified. Don't treat as rate limiting.
                        if is_generic_invalid and not is_legitimate_specific:
                            log_info(f"Generic invalid message '{message}' after successful precheck - likely legitimate error, not rate limiting")
                        else:
                            log_info(f"Legitimate specific error detected: '{message}' - not treating as rate limiting")
                    
                    # Add game name to message for clarity
                    message_with_game = f"{message} ({game_name})"
                    results.append(RedemptionResult(code, service, status, message_with_game, timestamp))
                    
                except Exception as e:
                    error_msg = f"Redemption exception for {game_name}: {e}"
                    results.append(RedemptionResult(code, service, RedemptionStatus.ERROR, error_msg, timestamp))
            
            # Refresh the page to clear any stale alert messages before next code
            self._refresh_redemption_page()
            return results
            
        except Exception as e:
            error_msg = f"Code redemption exception: {e}"
            # Still refresh page even on error to clear stale messages
            self._refresh_redemption_page()
            for platform in config.allowed_platforms:
                results.append(RedemptionResult(code, platform, RedemptionStatus.ERROR, error_msg, timestamp))
            return results
    
    def _precheck_code(self, code: str) -> Dict[str, Any]:
        """Precheck a code to get all available redemption combinations with 429 retry logic"""
        max_retries = 3
        retry_count = 0
        invalid_reconfirm_attempted = False
        
        while retry_count <= max_retries:
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
                
                # Handle status codes first before any parsing
                if resp.status_code == 429:
                    self._last_429_ts = time.time()
                    retry_count += 1
                    if retry_count <= max_retries:
                        log_warning(f"Rate limited (429) for code {code}, waiting 60 seconds before retry {retry_count}/{max_retries}")
                        time.sleep(60)  # Wait 60 seconds before retry
                        continue
                    else:
                        log_error(f"Rate limited (429) for code {code} after {max_retries} retries")
                        return {"error": "rate_limited"}
                elif resp.status_code != 200:
                    return {"error": f"Precheck failed with status {resp.status_code}"}
                
                # Check for empty response (can happen with 429 returning {})
                if not resp.text or resp.text.strip() in ["{}", ""]:
                    log_warning(f"Empty response for code {code}, treating as rate limited")
                    self._last_429_ts = time.time()
                    retry_count += 1
                    if retry_count <= max_retries:
                        log_warning(f"Empty response, waiting 60 seconds before retry {retry_count}/{max_retries}")
                        time.sleep(60)
                        continue
                    else:
                        return {"error": "rate_limited"}
                
                # Only parse response if status is 200 and we have content
                parsed = self._parse_precheck_response(resp.text)
                # If we got combinations, record recent valid precheck time
                if isinstance(parsed, dict) and parsed.get("combinations"):
                    self._last_valid_precheck_ts = time.time()
                    return parsed
                
                # If parsed as error, check for rate limiting first
                if isinstance(parsed, dict) and "error" in parsed:
                    err = parsed["error"].lower()
                    
                    # If we got a rate_limited error, back off for 60 seconds
                    if err == "rate_limited":
                        log_warning(f"Rate limited detected during precheck for {code}, backing off for 60 seconds")
                        time.sleep(60)
                        # Try once more after backoff
                        if retry_count < max_retries:
                            retry_count += 1
                            continue
                        else:
                            return parsed
                    
                    # Handle suspicious invalid after recent valid activity
                    recently_rate_limited = (time.time() - self._last_429_ts) < 180
                    recently_had_valid = (time.time() - self._last_valid_precheck_ts) < 120
                    looks_invalid = ("invalid" in err) or ("not a valid shift code" in err)
                    if looks_invalid and (recently_rate_limited or recently_had_valid):
                        if not invalid_reconfirm_attempted:
                            invalid_reconfirm_attempted = True
                            log_warning("Precheck returned invalid right after recent valid activity; waiting 30s and retrying to avoid throttle misclassification")
                            time.sleep(30)
                            continue
                        else:
                            log_warning("Confirming invalid after retry; still marking invalid")
                    return parsed
                
                return parsed
                
            except Exception as e:
                if retry_count < max_retries:
                    retry_count += 1
                    log_warning(f"Precheck exception for code {code}, retry {retry_count}/{max_retries}: {e}")
                    time.sleep(5)  # Short wait for general exceptions
                    continue
                else:
                    return {"error": f"Precheck exception: {e}"}
    
    def _parse_precheck_response(self, html: str, platform: str = None) -> Dict[str, Any]:
        """Parse precheck response for all available service/title combinations"""
        # Check if response is plain text (no HTML tags) - happens for expired codes
        # Example: "      This SHiFT code has expired"
        trimmed = html.strip()
        if trimmed and '<' not in trimmed and '>' not in trimmed:
            # Plain text response - likely an error message
            normalized = " ".join(trimmed.split())
            if "expired" in normalized.lower():
                return {"error": "This SHiFT code has expired"}
            elif "invalid" in normalized.lower():
                return {"error": "Invalid SHiFT code"}
            else:
                return {"error": normalized}
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Check for flash messages first
        flash_msg = self._extract_flash_message(html)
        if flash_msg:
            # If flash message looks like an invalid message, keep it.
            # But if it doesn't match known invalid/redeemed/expired and no forms exist,
            # prefer returning a rate-limited signal to trigger backoff.
            fl = flash_msg.lower()
            if any(k in fl for k in ["invalid", "redeemed", "expired", "not a valid shift code"]):
                return {"error": flash_msg}
            else:
                return {"error": "rate_limited"}
        
        # Look for redemption forms
        forms = soup.find_all('form', action='/code_redemptions')
        if not forms:
            # Check for common error indicators in main content areas
            main_content = soup.find(['main', '.main-content', '.content', '#content']) or soup
            content_lower = main_content.get_text().lower()
            full_content = main_content.get_text().strip()
            
            # Look for specific SHiFT error messages first
            recently_rate_limited = (time.time() - self._last_429_ts) < 180
            if "this shift code has already been redeemed" in content_lower and not recently_rate_limited:
                return {"error": "Already redeemed"}
            elif "this shift code has expired" in content_lower and not recently_rate_limited:
                return {"error": "Code expired"}  
            elif "this is not a valid shift code" in content_lower and not recently_rate_limited:
                return {"error": "Invalid SHiFT code"}
            elif "expired" in content_lower and "code" in content_lower:
                if not recently_rate_limited:
                    return {"error": "Code expired"}
                else:
                    return {"error": "rate_limited"}
            elif "not found" in content_lower or "invalid" in content_lower:
                if not recently_rate_limited:
                    return {"error": "Code not found or invalid"}
                else:
                    return {"error": "rate_limited"}
            elif len(full_content) < 20:
                # Very sparse content often occurs during throttling issues
                return {"error": "rate_limited"}
            
            return {"error": "No redemption form found"}
        
        # Extract all available service/title combinations
        available_combinations = []
        
        for form in forms:
            service_input = form.find('input', {'name': 'archway_code_redemption[service]'})
            title_input = form.find('input', {'name': 'archway_code_redemption[title]'})
            
            if service_input and title_input:
                service = service_input.get('value', '')
                title = title_input.get('value', '')
                
                # Extract all form data for this combination
                form_data = {}
                for input_tag in form.find_all('input'):
                    name = input_tag.get('name')
                    value = input_tag.get('value', '')
                    if name:
                        form_data[name] = value
                
                # Get game name from preceding header
                game_name = 'Unknown Game'
                header = form.find_previous(['h1', 'h2', 'h3', 'h4'])
                if header:
                    game_name = header.get_text().strip()
                
                available_combinations.append({
                    'service': service,
                    'title': title,
                    'game_name': game_name,
                    'form_data': form_data
                })
        
        if not available_combinations:
            return {"error": "No valid service/title combinations found"}
        
        return {"combinations": available_combinations}
    
    def _extract_flash_message(self, html: str) -> Optional[str]:
        """Extract meaningful flash/status message from SHiFT rewards HTML."""
        if not html:
            return None

        if self.debug:
            # Persist HTML snapshot for analysis
            timestamp = datetime.now().strftime("%m%d%S_%f")[:-3]
            content_hash = hashlib.md5(html.encode()).hexdigest()[:8]
            filename = f"rewards_page_{timestamp}_{content_hash}.html"
            filepath = self.debug_dir / filename

            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(html)
                log_warning(f"DEBUG: Saved HTML to {filepath}")
            except Exception as exc:
                log_warning(f"DEBUG: Failed to persist HTML snapshot: {exc}")

            # Provide structured analysis for manual review
            analysis = self.html_debugger.analyze_alerts(html)
            analysis_report = self.html_debugger.format_analysis(analysis)
            log_warning(f"DEBUG HTML ANALYSIS:\n{analysis_report}")

        trimmed = html.strip()
        if not trimmed:
            if self.debug:
                log_warning("DEBUG: Flash extraction received empty HTML after trimming")
            return None

        candidates: List[Tuple[str, str, bool]] = []  # (text, source, is_plain_text)

        # Case 1: Turbo stream often returns plain text snippets with no HTML tags
        if '<' not in trimmed and '>' not in trimmed:
            candidates.append((trimmed, "plain_text_response", True))

        soup = BeautifulSoup(html, 'html.parser')

        # Case 2: Turbo Stream template payloads
        for idx, turbo_stream in enumerate(soup.find_all('turbo-stream')):
            template = turbo_stream.find('template')
            text = template.get_text(" ", strip=True) if template else turbo_stream.get_text(" ", strip=True)
            if text:
                candidates.append((text, f"turbo-stream[{idx}]", False))

        # Case 3: Dedicated flash containers (non-hidden)
        # Prioritize the exact pattern used by SHiFT: <div class="alert notice"><p>...</p></div>
        flash_selectors = [
            'div.alert.notice p',  # Specific: paragraph inside alert notice div
            'div.alert p',         # Paragraph inside any alert div
            '.alert.notice',       # The alert notice div itself
            '.alert',              # Any alert div
            '.flash',
            '.flash-message',
            '.notice',
            '.error',
            '.message',
            '#flash_messages',
            '#flash',
            '[data-flash]'
        ]

        excluded_ids = {'shift_code_instructions', 'shift_code_error', 'status_page_notification'}

        for selector in flash_selectors:
            for elem in soup.select(selector):
                elem_id = elem.get('id') or ''
                if elem_id in excluded_ids:
                    continue

                # Check if element itself is hidden
                style_attr = (elem.get('style') or '').lower()
                if 'display:none' in style_attr or 'display: none' in style_attr:
                    continue
                
                # Also check if any parent element is hidden
                # This handles cases where expired message is in a hidden container
                is_hidden = False
                for parent in elem.parents:
                    if parent.name:  # Skip NavigableString parents
                        parent_style = (parent.get('style') or '').lower()
                        parent_id = parent.get('id') or ''
                        if 'display:none' in parent_style or 'display: none' in parent_style:
                            is_hidden = True
                            break
                        if parent_id in excluded_ids:
                            is_hidden = True
                            break
                
                if is_hidden:
                    continue

                text = elem.get_text(" ", strip=True)
                if text:
                    candidates.append((text, f"selector:{selector}", False))

        seen: Set[str] = set()
        for text, source, is_plain in candidates:
            normalized = " ".join(text.split())
            if not normalized:
                continue

            key = normalized.lower()
            if key in seen:
                continue
            seen.add(key)

            message = self._normalize_flash_message_text(normalized, is_plain)
            if message:
                if self.debug:
                    log_warning(f"DEBUG: Flash message detected from {source}: '{message}'")
                return message

        if self.debug:
            log_warning("DEBUG: No actionable flash message detected")

        return None

    def _normalize_flash_message_text(self, text: str, allow_generic: bool) -> Optional[str]:
        """Return canonical flash message if the text matches a known pattern."""
        if not text:
            return None

        normalized = " ".join(text.split())
        lower = normalized.lower()

        # Prioritise success detection before broader keyword matching so
        # instructional copy that mentions multiple outcomes doesn't override
        # a genuine success alert.
        if 'successfully redeemed' in lower or (
            'your code' in lower and 'successfully' in lower
        ) or (
            'redeemed' in lower and 'success' in lower
        ):
            return "Successfully redeemed"

        pattern_map: List[Tuple[List[str], str]] = [
            (['already', 'redeemed'], "This SHiFT code has already been redeemed"),
            (['has', 'expired'], "This SHiFT code has expired"),
            (['not a valid shift code'], "This is not a valid SHiFT code"),
            (['invalid shift code'], "Invalid SHiFT code"),
            (['invalid code'], "Invalid code"),
            (['code', 'not', 'found'], "Code not found"),
            (['unexpected error occurred'], "Unexpected error occurred"),
            (['too many requests'], "Rate limited - too many requests"),
            (['rate limit'], "Rate limited - too many requests"),
            (['temporarily unavailable'], "Rate limited - too many requests"),
            (['launch', 'shift-enabled'], "Launch SHiFT-enabled game required"),
            (['launch', 'shift enabled'], "Launch SHiFT-enabled game required"),
            (['not available for your selected platform'], "Not available for your selected platform"),
            (['not available for your platform'], "Not available for your selected platform"),
            (['service is currently unavailable'], "Service unavailable"),
            (['temporarily', 'disabled'], "Service unavailable"),
            (['server', 'error'], "Server error"),
        ]

        for keywords, canonical in pattern_map:
            if all(keyword in lower for keyword in keywords):
                return canonical

        # Allow generic return only when the response was plain text (no HTML tags)
        if allow_generic:
            return normalized

        # Ignore lengthy instructional text that doesn't map to actionable status
        if len(normalized) > 160:
            return None

        # As a fallback, recognize short messages that include redemption keywords
        keyword_whitelist = ['redeem', 'code', 'shift', 'reward']
        if any(word in lower for word in keyword_whitelist) and len(normalized) <= 140:
            return normalized

        return None
    
    def _refresh_redemption_page(self) -> bool:
        """Refresh the redemption page to clear stale messages and alerts"""
        try:
            response = self.session.get(
                f"{config.base_url}/code_redemptions",
                timeout=(config.connection_timeout, config.read_timeout)
            )
            return response.status_code == 200
        except Exception as e:
            log_error(f"Failed to refresh redemption page: {e}")
            return False
    
    def _submit_redemption(self, form_data: Dict[str, str]) -> requests.Response:
        """Submit redemption form with 429 retry logic"""
        max_retries = 2
        retry_count = 0
        
        while retry_count <= max_retries:
            # The form_data already contains the correct authenticity_token from the precheck
            # Don't overwrite it with a different token
            # csrf_token = self.session.get_csrf_token()
            # if csrf_token:
            #     form_data["authenticity_token"] = csrf_token
            
            # Deduplicate cookies before making request
            # This prevents the "multiple cookies with name" error
            self._deduplicate_cookies()
            
            # Log the form data and cookies if debug mode is enabled
            if config.debug:
                log_warning(f"DEBUG: Submitting form_data keys: {list(form_data.keys())}")
                log_warning(f"DEBUG: authenticity_token present: {'authenticity_token' in form_data}")
                log_warning(f"DEBUG: code: {form_data.get('archway_code_redemption[code]', 'NOT FOUND')}")
                log_warning(f"DEBUG: service: {form_data.get('archway_code_redemption[service]', 'NOT FOUND')}")
                log_warning(f"DEBUG: title: {form_data.get('archway_code_redemption[title]', 'NOT FOUND')}")
                
                cookies_dict = dict(self.session.session.cookies)
                log_warning(f"DEBUG: Session has {len(cookies_dict)} cookies after dedup")
                log_warning(f"DEBUG: _session_id present: {'_session_id' in cookies_dict}")
                log_warning(f"DEBUG: si present: {'si' in cookies_dict}")
            
            # Mimic a real browser form submission, NOT an AJAX request
            # This ensures we get proper HTML responses, not Turbo Stream responses
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': config.base_url,
                'Referer': f"{config.base_url}/rewards",
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
            }
            
            # First, do the POST without following redirects to see what we get
            resp = self.session.post(
                f"{config.base_url}/code_redemptions",
                data=form_data,
                headers=headers,
                timeout=(config.connection_timeout, config.read_timeout),
                allow_redirects=False  # Handle redirects manually
            )

            if config.debug:
                log_warning(
                    f"DEBUG: Redemption POST status={resp.status_code} "
                    f"content_type={resp.headers.get('Content-Type')} "
                    f"location={resp.headers.get('Location')}"
                )
            
            # Check for 429 rate limit
            if resp.status_code == 429:
                self._last_429_ts = time.time()
                if retry_count < max_retries:
                    retry_count += 1
                    log_warning(f"Rate limited (429) during redemption, waiting 60 seconds before retry {retry_count}/{max_retries}")
                    time.sleep(60)
                    continue
                else:
                    # All retries exhausted, still 429 - log it and return the response
                    log_warning(f"Rate limited (429) after {max_retries} retries, returning rate limit status")
                    return resp
            
            # If we got a redirect (302), follow it manually
            if resp.status_code == 302:
                redirect_url = resp.headers.get('Location')
                if redirect_url:
                    # Make it absolute if it's relative
                    from urllib.parse import urljoin
                    redirect_url = urljoin(config.base_url, redirect_url)
                    
                    if config.debug:
                        log_warning(f"DEBUG: Following redirect to {redirect_url}")
                    
                    # Follow the first redirect (to UUID)
                    uuid_resp = self.session.get(
                        redirect_url,
                        headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'},
                        timeout=(config.connection_timeout, config.read_timeout),
                        allow_redirects=False
                    )
                    
                    if config.debug:
                        log_warning(f"DEBUG: UUID redirect status={uuid_resp.status_code}, location={uuid_resp.headers.get('Location')}")
                    
                    # Check if there's another redirect (to /rewards)
                    if uuid_resp.status_code == 302:
                        final_redirect = uuid_resp.headers.get('Location')
                        if final_redirect:
                            final_redirect = urljoin(config.base_url, final_redirect)
                            
                            if config.debug:
                                log_warning(f"DEBUG: Following final redirect to {final_redirect}")
                            
                            # Get the final rewards page
                            final_resp = self.session.get(
                                final_redirect,
                                headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'},
                                timeout=(config.connection_timeout, config.read_timeout),
                                allow_redirects=False
                            )
                            
                            if config.debug:
                                log_warning(f"DEBUG: Final response status={final_resp.status_code}, content-length={len(final_resp.text)}")
                            
                            return final_resp
                    
                    # If no second redirect, return the UUID response
                    return uuid_resp
            
            return resp
    
    def _parse_redemption_response(self, resp: requests.Response) -> Tuple[RedemptionStatus, str]:
        """Parse redemption response by checking the updated rewards page"""
        # Check status code first 
        if resp.status_code == 429:
            log_warning(f"Rate limited (429) response received")
            return RedemptionStatus.RATE_LIMITED, "Rate limited - too many requests"
        elif resp.status_code >= 400:
            log_warning(f"HTTP error {resp.status_code} received, not parsing content")
            return RedemptionStatus.ERROR, f"HTTP {resp.status_code} error"
        
        # We should now have the final rewards page after following all redirects manually
        if resp.status_code == 200:
            if config.debug:
                log_warning(f"DEBUG: Parsing final response, URL={resp.url}, length={len(resp.text)}")
            
            # Save HTML for analysis (only in debug mode)
            if config.debug:
                timestamp = datetime.now().strftime("%H%M%S_%f")[:-3]
                filepath = self.debug_dir / f"redemption_response_{timestamp}.html"
                try:
                    filepath.parent.mkdir(parents=True, exist_ok=True)
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(resp.text)
                    log_warning(f"DEBUG: Saved response HTML to {filepath}")
                except Exception as e:
                    log_warning(f"DEBUG: Failed to save HTML: {e}")
            
            # Extract the alert message from the final page
            flash_msg = self._extract_flash_message(resp.text)
            if flash_msg:
                if config.debug:
                    log_warning(f"DEBUG: Found status message: '{flash_msg}'")
                return self._classify_flash_message(flash_msg), flash_msg
            else:
                if config.debug:
                    log_warning("DEBUG: No status message found on final page")
                    
                    # Do a detailed analysis of what we got
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    alert_divs = soup.select('div.alert')
                    log_warning(f"DEBUG: Found {len(alert_divs)} div.alert elements")
                    for i, div in enumerate(alert_divs):
                        log_warning(f"DEBUG: Alert {i}: classes={div.get('class')}, text='{div.get_text().strip()[:100]}'")
                    
                    notice_divs = soup.select('.notice')
                    log_warning(f"DEBUG: Found {len(notice_divs)} .notice elements")
                    
                    paragraphs = soup.select('div.alert p, div.notice p')
                    log_warning(f"DEBUG: Found {len(paragraphs)} paragraphs in alert/notice divs")
                    for i, p in enumerate(paragraphs):
                        log_warning(f"DEBUG: Paragraph {i}: '{p.get_text().strip()}'")
                
                return RedemptionStatus.UNKNOWN, "No status message found"
        
        # Default fallback for unexpected status codes
        return RedemptionStatus.ERROR, f"Unexpected response: HTTP {resp.status_code}"
    
    def _classify_error(self, error_msg: str) -> RedemptionStatus:
        """Classify error message into status"""
        error_lower = error_msg.lower()
        if "rate_limited" in error_lower or error_lower.strip() == "rate limited" or error_lower.strip() == "rate limited - too many requests":
            return RedemptionStatus.RATE_LIMITED
        elif "expired" in error_lower:
            return RedemptionStatus.EXPIRED
        elif "redeemed" in error_lower:
            return RedemptionStatus.ALREADY_REDEEMED
        elif "not available" in error_lower or "platform" in error_lower:
            return RedemptionStatus.PLATFORM_UNAVAILABLE
        elif "invalid" in error_lower or "not a valid shift code" in error_lower:
            return RedemptionStatus.INVALID
        elif "launch" in error_lower and "shift-enabled" in error_lower:
            return RedemptionStatus.GAME_REQUIRED
        else:
            return RedemptionStatus.ERROR
    
    def _classify_flash_message(self, flash_msg: str) -> RedemptionStatus:
        """Classify flash message into status"""
        flash_lower = flash_msg.lower()
        
        # Check specific messages first before checking keywords to avoid misclassification
        if flash_msg == "RATE_LIMITED" or "too many requests" in flash_lower or "rate limit" in flash_lower or "unexpected error occurred" in flash_lower:
            return RedemptionStatus.RATE_LIMITED
        elif "already" in flash_lower and "redeemed" in flash_lower:
            # "This SHiFT code has already been redeemed"
            return RedemptionStatus.ALREADY_REDEEMED
        elif "successfully redeemed" in flash_lower or "your code was successfully redeemed" in flash_lower or ("success" in flash_lower and "redeemed" in flash_lower):
            # "Your code was successfully redeemed"
            return RedemptionStatus.SUCCESS
        elif "expired" in flash_lower:
            return RedemptionStatus.EXPIRED
        elif "invalid" in flash_lower or "not a valid shift code" in flash_lower or "code not found" in flash_lower:
            return RedemptionStatus.INVALID
        elif "launch" in flash_lower and "shift-enabled" in flash_lower:
            return RedemptionStatus.GAME_REQUIRED
        elif "not available" in flash_lower and "platform" in flash_lower:
            return RedemptionStatus.PLATFORM_UNAVAILABLE
        elif "service unavailable" in flash_lower or "temporarily unavailable" in flash_lower:
            return RedemptionStatus.ERROR
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
        # Only show config if verbose mode is enabled
        if config.verbose:
            log_config()
        
        # Process new codes
        new_count = self._process_new_codes()
        
        # Get codes that haven't been redeemed yet
        unredeemed_codes = db.get_unredeemed_codes(config.allowed_platforms)
        
        # Also check for codes we know work with current config
        matching_codes = db.get_codes_matching_current_config(config.allowed_platforms)
        
        # Combine and deduplicate codes (keep reward from unredeemed_codes if available)
        all_codes_dict = {}
        for code, exp_date, reward in unredeemed_codes:
            all_codes_dict[code] = (exp_date, reward)
        for code, exp_date in matching_codes:
            if code not in all_codes_dict:
                all_codes_dict[code] = (exp_date, None)
        all_codes_to_redeem = [(code, exp_date, reward) for code, (exp_date, reward) in all_codes_dict.items()]
        
        if new_count > 0 or all_codes_to_redeem:
            self._redeem_pending_codes(all_codes_to_redeem)
        else:
            log_info("No new codes found, no pending redemptions")
        
        log_success("Completed successfully")
    
    def _process_new_codes(self) -> int:
        """Scrape and process new codes"""        
        # Scrape all sources concurrently
        all_results = self.scraper.scrape_all_sources()
        
        # Create a mapping from source names to URLs for logging
        source_url_map = {name: url for url, name in self.scraper.sources}
        
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
                source_url = source_url_map.get(source_name, source_name)
                log_success(f"{len(source_new_codes)} new codes from {source_url}")
        
        # Batch add all new codes
        new_count = db.add_codes_batch(all_new_codes)
        
        if new_count > 0:
            log_section("New Codes Found")
            log_success(f"Added {new_count} new codes to database")
        # Don't log when no new codes - reduces noise
        
        return new_count
    
    def _redeem_pending_codes(self, unredeemed_codes: List[Tuple[str, Optional[datetime], Optional[str]]]):
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
            
            # Send Discord notification
            if config.discord_webhook_url:
                notifier = DiscordNotifier(config.discord_webhook_url)
                notifier.authentication_failed("Failed to authenticate with SHiFT")
            
            return
        
        if not unredeemed_codes:
            log("No codes need redemption")
            return
        
        # Pre-filter expired codes before redemption attempts
        valid_codes = []
        valid_rewards = {}  # Store rewards for valid codes
        expired_codes = []
        
        for code, exp_date, reward in unredeemed_codes:
            code_info = CodeInfo(code=code, source="", expiration_date=exp_date, reward=reward)
            if code_info.is_expired():
                expired_codes.append(code)
                # Only log expired codes in verbose mode
                if config.verbose:
                    exp_str = exp_date.strftime("%Y-%m-%d") if exp_date else "unknown"
                    log_code(code, "SKIPPED", f"(expired: {exp_str})", Colors.YELLOW)
            else:
                valid_codes.append((code, exp_date))
                if reward:
                    valid_rewards[code] = reward
        
        # Mark expired codes in database without attempting redemption
        if expired_codes:
            db.mark_codes_expired_batch(expired_codes, config.allowed_platforms)
            if config.verbose:
                log_info(f"Marked {len(expired_codes)} expired codes in database")
        
        if not valid_codes:
            log_info("No valid codes to redeem (all expired)")
            return
        
        log_section("Code Redemption")
        friendly_titles = [config.title_display_names.get(title, title) for title in config.allowed_titles]
        print(f"  {Colors.CYAN}Processing:{Colors.END} {Colors.BOLD}{len(valid_codes)} codes{Colors.END} on {Colors.BOLD}{', '.join(config.allowed_services)}{Colors.END} for {Colors.BOLD}{', '.join(friendly_titles)}{Colors.END}")
        
        # Redeem valid codes
        results = self.redeemer.redeem_codes_batch(valid_codes)
        
        # Summary
        if results:
            success_count = sum(1 for r in results if r.status == RedemptionStatus.SUCCESS)
            
            if success_count > 0:
                log_success(f"{success_count} codes successfully redeemed")
                
                # Send Discord notification for successful redemptions
                if config.discord_webhook_url:
                    # Collect unique successful codes (may have multiple results per code for different services)
                    successful_codes = {}
                    for r in results:
                        if r.status == RedemptionStatus.SUCCESS:
                            if r.code not in successful_codes:
                                successful_codes[r.code] = {
                                    'services': [],
                                    'game_names': set()
                                }
                            successful_codes[r.code]['services'].append(r.platform)
                            # Extract game name from the message if present
                            if '(' in r.message and ')' in r.message:
                                game_name = r.message[r.message.rfind('(')+1:r.message.rfind(')')]
                                successful_codes[r.code]['game_names'].add(game_name)
                    
                    # Build redemption list with rewards
                    redemptions = []
                    for code, data in successful_codes.items():
                        # Get reward for this code
                        reward = valid_rewards.get(code, 'N/A')
                        
                        # Get game names from collected data or fall back to config
                        titles = list(data['game_names']) if data['game_names'] else friendly_titles
                        
                        redemptions.append({
                            'code': code,
                            'titles': titles,
                            'services': data['services'],
                            'rewards': reward
                        })
                    
                    # Send Discord notification
                    if redemptions:
                        notifier = DiscordNotifier(config.discord_webhook_url)
                        notifier.codes_redeemed_report(redemptions)
            
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
    
    # Print version at startup with timestamp
    log_section(f"BL4 AutoSHiFT v{__version__}", show_time=True)
    
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
        elif "--debug-html" in sys.argv:
            # Special HTML debugging mode
            log("DEBUG HTML MODE: Testing HTML parsing with enhanced debugging")
            config.verbose = True
            app = ShiftCodeManager()
            
            if not app.session.login(config.email, config.password):
                log("ERROR: Authentication failed")
                return
            
            # Test with one code
            codes = db.get_unredeemed_codes(config.allowed_platforms)[:1]
            if codes:
                log(f"Testing HTML parsing with code: {codes[0][0]}")
                # Extract just code and expiration for batch processing
                codes_for_batch = [(code, exp_date) for code, exp_date, _ in codes]
                results = app.redeemer.redeem_codes_batch(codes_for_batch)
                for result in results:
                    log(f"Result: {result.status} - {result.message}")
            else:
                log("No codes available for testing")
            return
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