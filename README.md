# BL4 AutoSHiFT

Automated Borderlands SHiFT code scraper and redeemer. This tool automatically scrapes SHiFT codes from websites and redeems them to your account across multiple Borderlands games and platforms.

**Docker Hub:** https://hub.docker.com/r/slipping/bl4-autoshift

**Multi-Game Support**: Works with all Borderlands games (BL1, BL2, BL:TPS, BL3, TTW, BL4)

## Code Sources

The application scrapes SHiFT codes from these gaming websites:
- https://mentalmars.com/game-news/borderlands-4-shift-codes/
- https://mentalmars.com/game-news/borderlands-golden-keys/
- https://gaming.news/codex/borderlands-4-shift-codes-list-guide-and-troubleshooting/
- https://thegamepost.com/borderlands-4-all-shift-codes/
- https://www.polygon.com/borderlands-4-active-shift-codes-redeem/
- https://www.ign.com/wikis/borderlands-4/Borderlands_4_SHiFT_Codes
- https://www.gamespot.com/articles/borderlands-4-shift-codes-all-active-keys-and-how-to-redeem-them/1100-6533833/

All codes are redeemed via the official SHiFT website: https://shift.gearboxsoftware.com

## Quick Start

1. **Create docker-compose.yml:**

```yaml
services:
  autoshift:
    image: slipping/bl4-autoshift:latest
    container_name: bl4-autoshift
    restart: unless-stopped
    env_file: .env
    volumes:
      - bl4-autoshift-data:/app/data
    user: "1000:1000"
volumes:
  bl4-autoshift-data:
```

2. **Create .env file:**

```env
# Your SHiFT account credentials (required)
SHIFT_EMAIL=your_email@example.com
SHIFT_PASSWORD=your_password

# Platforms to redeem codes for (required - must be linked to your SHiFT account)
ALLOWED_SERVICES=steam

# Games to redeem codes for (required)
ALLOWED_TITLES=bl4

# Optional settings
SCHEDULE_INTERVAL=3600      # Run every hour
VERBOSE=1                   # Enable detailed logging
DISCORD_WEBHOOK_URL=        # Discord notifications (optional, this is used for code lockouts, where the site required you to open a game before you can redeem a code)
TZ=UTC                      # Container timezone
```

3. **Start the container:**

```bash
docker compose up -d
```

## Configuration

### Required Environment Variables

- **SHIFT_EMAIL** - Your SHiFT account email address
- **SHIFT_PASSWORD** - Your SHiFT account password
- **ALLOWED_SERVICES** - Comma-separated list of platforms to redeem codes for
- **ALLOWED_TITLES** - Comma-separated list of Borderlands games to redeem codes for

### Supported Services/Platforms

- `steam` - Steam (PC)
- `epic` - Epic Games Store (PC)
- `psn` - PlayStation Network (PS4/PS5)
- `xboxlive` - Xbox Live (Xbox One/Series)
- `nintendo` - Nintendo Switch (not tested)

**Important**: These platforms must be linked to your SHiFT account at https://shift.gearboxsoftware.com/associations

### Supported Games/Titles

- `bl1` - Borderlands: Game of the Year Edition
- `bl2` - Borderlands 2
- `blps` - Borderlands: The Pre-Sequel
- `bl3` - Borderlands 3
- `ttw` - Tiny Tina's Wonderlands
- `bl4` - Borderlands 4

### Optional Environment Variables

- **SCHEDULE_INTERVAL** - Seconds between runs (default: 3600 = 1 hour)
- **VERBOSE** - Enable detailed logging: 1 or 0 (default: 1)
- **DISCORD_WEBHOOK_URL** - Discord webhook for notifications when the shift site requires you to open a game before you can redeem another key.
- **TZ** - Container timezone (default: UTC, i.e.: America/New_York)

## Platform Linking Setup

Before using this script, you must link your gaming platforms to your SHiFT account:

1. Visit https://shift.gearboxsoftware.com/associations
2. Sign into your SHiFT account
3. Link each platform you want to use (Steam, Epic, PSN, XboxLive, Nintendo)
4. Configure `ALLOWED_SERVICES` to include only your linked platforms

If you see "Platform not available" errors, the platform isn't properly linked to your account.

## Game Launch Requirement

Gearbox sometimes requires launching a SHiFT-enabled game before allowing code redemption. When this occurs:

1. The scraper detects the requirement and pauses redemption
2. Discord notification is sent (if configured)
3. Launch any Borderlands game and sign into SHiFT in-game
4. The scraper automatically resumes on the next run

### Multiple Game Configuration

To redeem codes for multiple games, in your .env:

```env
ALLOWED_TITLES=bl3,bl4,ttw
ALLOWED_SERVICES=steam,epic
```

## Database

Uses SQLite to store:
- Discovered SHiFT codes with expiration dates
- Redemption history and results
- Code availability per service/title combination
- Configuration change tracking

The database is stored in the Docker volume and persists between container updates.

## Troubleshooting

### Common Issues

**"Platform not available" errors**
- Ensure the platform is linked at https://shift.gearboxsoftware.com/associations
- Check `ALLOWED_SERVICES` matches your linked platforms

**"Code valid for: bl1" messages**
- This is normal - codes are filtered by your `ALLOWED_TITLES` setting
- Only codes matching your configured games will be redeemed

**Rate limiting (429 errors)**
- Built-in retry logic handles temporary rate limits
- Consider increasing `SCHEDULE_INTERVAL` if persistent

**Game launch required**
- Launch any Borderlands game and sign into SHiFT
- Use `--clear-game-required` command to reset blocks

## License

MIT License - see LICENSE file for details.