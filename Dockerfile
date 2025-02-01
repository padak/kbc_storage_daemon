# Use lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    WATCHED_DIRECTORY=/watch \
    LOG_DIR=/logs

# Create directories for volumes
RUN mkdir -p /watch /logs \
    && chown -R nobody:nogroup /watch /logs

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY daemon/ ./daemon/
COPY config.*.template ./

# Switch to non-root user
USER nobody

# Set up volumes
VOLUME ["/watch", "/logs"]

# Run the daemon
CMD ["python", "-m", "daemon.main"] 