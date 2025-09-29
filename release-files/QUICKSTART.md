# BL4 AutoSHiFT v0.2 - Quick Start Guide

## What's Included

- `docker-compose.yml` - Docker Compose configuration
- `env.example` - Environment configuration template

## Setup Steps

1. **Copy the environment file:**
   ```bash
   cp env.example .env
   ```

2. **Edit .env file with your details:**
   - Set your SHiFT email and password
   - Configure ALLOWED_SERVICES (steam, epic, psn, xboxlive, nintendo)
   - Configure ALLOWED_TITLES (bl1, bl2, blps, bl3, ttw, bl4)

3. **Start the container:**
   ```bash
   docker compose up -d
   ```

4. **View logs:**
   ```bash
   docker logs -f bl4-autoshift
   ```

## Important Notes

- Link your gaming platforms at https://shift.gearboxsoftware.com/associations
- Only include platforms you have linked in ALLOWED_SERVICES
- The application supports all Borderlands games (BL1, BL2, BL:TPS, BL3, TTW, BL4)

## Full Documentation

See the main README.md for complete configuration options and troubleshooting.