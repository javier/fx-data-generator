FROM python:3.11-slim-bookworm

RUN addgroup --gid 10000 fxuser && \
    adduser --disabled-password --uid 10000 --gid 10000 fxuser

WORKDIR /app
RUN chown fxuser:fxuser /app

# Install dependencies first for build cache
COPY --chown=fxuser:fxuser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=fxuser:fxuser . .

# Copy entrypoint before switching user, and make executable
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

USER fxuser

# Set defaults for environment variables (used by entrypoint script)
ENV MODE=real-time
ENV SHORT_TTL=True
ENV MARKET_DATA_MIN_EPS=8200
ENV MARKET_DATA_MAX_EPS=11000
ENV CORE_MIN_EPS=700
ENV CORE_MAX_EPS=1000
ENV PROTOCOL=tcp
ENV HOST=host.docker.internal
ENV PG_USER=admin
ENV PG_PASSWORD=quest
ENV PG_PORT=8812
ENV TOKEN=""
ENV TOKEN_X=""
ENV TOKEN_Y=""
ENV ILP_USER=admin
ENV YAHOO_REFRESH_SECS=30

ENTRYPOINT ["/app/docker-entrypoint.sh"]
