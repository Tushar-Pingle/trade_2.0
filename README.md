# Hyperliquid AI Trading Agent

An AI-powered trading agent that uses Claude to analyze markets and execute perpetual futures trades on Hyperliquid. Supports crypto, stocks, commodities, indices, and forex via HIP-3 markets.

## What It Does

1. Fetches real-time candle data and computes technical indicators (EMA, RSI, MACD, ATR, BBands, ADX, OBV, VWAP) locally from Hyperliquid
2. Sends full market context to Claude, which decides buy/sell/hold for each asset
3. Executes trades with take-profit and stop-loss orders
4. Hard-coded safety guards enforce position limits, leverage caps, and loss protection

## Tradeable Markets

All 229+ Hyperliquid perp markets plus HIP-3 tradfi assets:

- **Crypto**: BTC, ETH, SOL, HYPE, AVAX, SUI, ARB, LINK, and 200+ more
- **Stocks**: xyz:TSLA, xyz:NVDA, xyz:AAPL, xyz:GOOGL, xyz:AMZN, xyz:META, xyz:MSFT, xyz:COIN, xyz:PLTR...
- **Commodities**: xyz:GOLD, xyz:SILVER, xyz:BRENTOIL, xyz:CL, xyz:COPPER, xyz:NATGAS, xyz:PLATINUM
- **Indices**: xyz:SP500, xyz:XYZ100
- **Forex**: xyz:EUR, xyz:JPY

## Safety Guards

All enforced in code, not just LLM prompts. Configurable via `.env`:

| Guard | Default | Description |
|-------|---------|-------------|
| Max Position Size | 10% | Single position capped at 10% of portfolio |
| Force Close | -20% | Auto-close positions at 20% loss |
| Max Leverage | 10x | Hard leverage cap |
| Total Exposure | 50% | All positions combined capped at 50% |
| Daily Circuit Breaker | -10% | Stops new trades at 10% daily drawdown |
| Mandatory Stop-Loss | 5% | Auto-set SL if LLM doesn't provide one |
| Max Positions | 10 | Concurrent position limit |
| Balance Reserve | 20% | Don't trade below 20% of initial balance |

## Setup

### Prerequisites
- Python 3.12+
- Anthropic API key
- Hyperliquid wallet (agent wallet as signer + main wallet with funds)

### Configuration

```bash
cp .env.example .env
# Edit .env with your keys
```

Required environment variables:
- `ANTHROPIC_API_KEY` — Claude API key
- `HYPERLIQUID_PRIVATE_KEY` — Agent/API wallet private key (signer only)
- `HYPERLIQUID_VAULT_ADDRESS` — Main wallet address (holds funds)
- `ASSETS` — Space-separated list of assets to trade
- `INTERVAL` — Trading loop interval (e.g. `5m`, `1h`)

### Install & Run

```bash
pip install hyperliquid-python-sdk anthropic python-dotenv aiohttp requests
python3 src/main.py
```

Or with CLI args:
```bash
python3 src/main.py --assets "BTC ETH SOL xyz:GOLD xyz:TSLA" --interval 5m
```

### Agent Wallet Setup

1. Go to app.hyperliquid.xyz → Settings → API Wallets
2. Add your agent wallet address as an authorized signer
3. Set `HYPERLIQUID_VAULT_ADDRESS` to your main wallet address in `.env`

The agent wallet signs trades on behalf of your main wallet. It cannot withdraw funds.

## Structure

```
src/
  main.py                  # Entry point, trading loop, API server
  config_loader.py         # Environment config with defaults
  risk_manager.py          # Safety guards (position limits, loss protection)
  agent/
    decision_maker.py      # Claude API integration, tool calling
  indicators/
    local_indicators.py    # EMA, RSI, MACD, ATR, BBands, ADX, OBV, VWAP
    taapi_client.py        # Legacy (unused) — kept for reference
  trading/
    hyperliquid_api.py     # Order execution, candles, state queries
  utils/
    formatting.py          # Number formatting
    prompt_utils.py        # JSON serialization helpers
```

## How It Works

Each loop iteration:
1. Fetches account state (balance, positions, PnL)
2. Force-closes any position at >= 20% loss
3. Gathers candle data and computes indicators for all assets
4. Sends everything to Claude with risk limits
5. Claude returns buy/sell/hold decisions with allocation, TP/SL
6. Risk manager validates each trade (caps allocation, enforces SL)
7. Executes approved trades (market or limit orders)

## API Endpoints

When running, serves a local API:
- `GET /diary` — Recent trade diary entries as JSON
- `GET /logs` — LLM request logs

## Dashboard

A separate Next.js dashboard is available for real-time PnL and trade monitoring. See the `dashboard/` directory or deploy to Vercel.

## License

Use at your own risk. No guarantee of returns. This code has not been audited.
