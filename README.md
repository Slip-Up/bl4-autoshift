# BL4 AutoSHiFT

Automated Borderlands 4 SHiFT code scraper and redeemer. This tool automatically scrapes SHiFT codes from gaming websites and redeems them on your behalf.

**Docker Hub:** https://hub.docker.com/r/slipping/bl4-autoshift

## Code Sources

This tool pulls codes from the following websites:
- https://mentalmars.com/game-news/borderlands-4-shift-codes/
- https://gaming.news/codex/borderlands-4-shift-codes-list-guide-and-troubleshooting/
- https://thegamepost.com/borderlands-4-all-shift-codes/
- https://www.polygon.com/borderlands-4-active-shift-codes-redeem/
- https://www.ign.com/wikis/borderlands-4/Borderlands_4_SHiFT_Codes
- https://www.gamespot.com/articles/borderlands-4-shift-codes-all-active-keys-and-how-to-redeem-them/1100-6533833/

Codes are automatically redeemed at: https://shift.gearboxsoftware.com

By default, it runs every hour to check for new codes and redeem them.

## Setup

### Docker Compose

Create a `docker-compose.yml` file:

```yaml
services:
  bl4-autoshift:
    image: slipping/bl4-autoshift:latest
    container_name: bl4-autoshift
    restart: unless-stopped
    env_file: .env
    volumes:
      - bl4_data:/app/data
      - bl4_logs:/app/logs
    user: "1000:1000"
volumes:
  bl4_data:
    driver: local
  bl4_logs:
    driver: local
```

### Environment File

Create a `.env` file in the same directory as the `docker-compose.yml`:

```env
# BL4 AutoSHiFT - Configuration

# REQUIRED: Your SHiFT account
SHIFT_EMAIL=your_email@example.com
SHIFT_PASSWORD=your_password

# REQUIRED: Which platforms to redeem codes for (they must be linked on your shift account)
ALLOWED_PLATFORMS=steam,epic,psn,xboxlive,nintendo

# OPTIONAL: How often to run (in seconds)
SCHEDULE_INTERVAL=3600  # 3600 = 1 hour

# OPTIONAL: Discord webhook for notifications
DISCORD_WEBHOOK_URL=

# OPTIONAL: Enable detailed logging
VERBOSE=1

# OPTIONAL: Your timezone
TZ=UTC
```

### Start the Container

```bash
docker compose up -d
```

## Environment Variables

- `SHIFT_EMAIL` - Your SHiFT account email (required)
- `SHIFT_PASSWORD` - Your SHiFT account password (required)
- `ALLOWED_PLATFORMS` - Comma-separated platforms: steam, epic, psn, xboxlive (required)
  - These platforms must be linked to your SHiFT account at https://shift.gearboxsoftware.com/associations
  - If you get "Platform not available" errors, the platform isn't linked to your account
  - Supported platforms: steam, epic, psn, xboxlive, nintendo?
- `SCHEDULE_INTERVAL` - How often to run in seconds (default: 3600 = 1 hour)
- `DISCORD_WEBHOOK_URL` - Discord webhook URL to notify you if you need to open the game before a code can be redeemed (optional)
- `VERBOSE` - Enable detailed logging: 1 or 0 (default: 1)
- `TZ` - Container timezone (default: UTC)

## Platform Linking

**Important:** Before running the scraper, make sure your gaming platforms are linked to your SHiFT account:

1. Visit https://shift.gearboxsoftware.com/associations
2. Log into your SHiFT account
3. Link the platforms you want to redeem codes for (Steam, Epic, PSN, Xbox Live)
4. Only include linked platforms in your `ALLOWED_PLATFORMS` environment variable

If you see an error like `Platform not available, it means that platform isn't linked to your SHiFT account.

**Tested Platforms:**
- Steam - Working
- Epic Games Store - Working  
- PlayStation Network (PSN) - Working
- Xbox Live - Working
- Nintendo - Untested

## Game Launch Requirement

Sometimes the SHiFT website requires you to launch a Borderlands game before allowing code redemption. When this happens, the scraper will detect it and pause further redemption attempts. 

If you configure a Discord webhook URL, you will receive notifications when this game launch requirement is triggered, allowing you to manually launch a game and resolve the block.

## Commands

```bash
# View logs
docker logs bl4-autoshift

# Follow logs in real-time
docker logs -f bl4-autoshift

# Restart container
docker compose restart

# Stop container
docker compose down

# Update to latest version
docker compose pull && docker compose up -d
```

## License

MIT License