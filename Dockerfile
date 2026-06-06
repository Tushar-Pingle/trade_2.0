FROM python:3.12-slim

WORKDIR /app

# Install dependencies directly (no poetry needed)
RUN pip install --no-cache-dir \
    hyperliquid-python-sdk \
    anthropic \
    python-dotenv \
    aiohttp \
    requests

# Copy source
COPY src ./src

# API defaults
ENV APP_PORT=3000
EXPOSE 3000

ENTRYPOINT ["python", "-m", "src.main"]
