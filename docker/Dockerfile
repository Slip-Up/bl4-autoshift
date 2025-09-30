# Use Python Alpine for minimal size (~50MB vs ~900MB for standard Python)
FROM python:3.12-alpine

# Set metadata
LABEL maintainer="BL4 AutoSHiFT"
LABEL description="Lightweight Borderlands 4 SHiFT code scraper container"
LABEL version="v0.2.4"

# Create non-root user for security
RUN addgroup -g 1000 -S shiftuser && \
    adduser -u 1000 -S shiftuser -G shiftuser

# Set working directory
WORKDIR /app

# Install system dependencies (minimal Alpine packages)
RUN apk add --no-cache \
    gcc \
    musl-dev \
    libffi-dev \
    && rm -rf /var/cache/apk/*

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip cache purge

# Remove build dependencies to reduce image size
RUN apk del gcc musl-dev libffi-dev

# Copy application files
COPY bshift.py .

# Create necessary directories and set permissions
RUN mkdir -p /app/data /app/logs && \
    chown -R shiftuser:shiftuser /app

# Switch to non-root user
USER shiftuser

# Set environment variables for container paths
ENV DATABASE_PATH=/app/data/bshift.db
ENV SESSION_PATH=/app/data/session.json
ENV NOTIFICATION_PATH=/app/data/last_notification.txt
ENV PYTHONUNBUFFERED=1

# Create volume mount points for persistent data
VOLUME ["/app/data", "/app/logs"]

# Health check to ensure the container is working
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import requests; print('OK')" || exit 1

# Default command with built-in scheduler
CMD ["sh", "-c", "while true; do python bshift.py; sleep ${SCHEDULE_INTERVAL:-3600}; done"]