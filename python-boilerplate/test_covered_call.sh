#!/bin/bash
# Test configuration for 0-DTE covered call strategy

# Conservative settings (higher quality, fewer results)
echo "=== Conservative Configuration ==="
python3 src/backtesting/apy.py screen \
  --symbol SPY \
  --expiration-days 0 \
  --min-otm-pct 0.00 \
  --max-otm-pct 0.02 \
  --delta-lo 0.20 \
  --delta-hi 0.40 \
  --min-bid 0.10 \
  --min-open-interest 50 \
  --min-volume 10 \
  --max-spread-to-mid 0.30 \
  --min-premium-yield 0.001 \
  --rank-metric premium_yield \
  --outdir ./data

echo ""
echo "=== Moderate Configuration (Balanced) ==="
python3 src/backtesting/apy.py screen \
  --symbol SPY \
  --expiration-days 0 \
  --min-otm-pct 0.00 \
  --max-otm-pct 0.03 \
  --delta-lo 0.15 \
  --delta-hi 0.35 \
  --min-bid 0.05 \
  --min-open-interest 20 \
  --min-volume 1 \
  --max-spread-to-mid 0.50 \
  --min-premium-yield 0.0005 \
  --rank-metric premium_yield \
  --outdir ./data

echo ""
echo "=== Aggressive Configuration (More results) ==="
python3 src/backtesting/apy.py screen \
  --symbol SPY \
  --expiration-days 0 \
  --min-otm-pct 0.01 \
  --max-otm-pct 0.05 \
  --delta-lo 0.10 \
  --delta-hi 0.30 \
  --min-bid 0.03 \
  --min-open-interest 1 \
  --min-volume 0 \
  --max-spread-to-mid 0.75 \
  --min-premium-yield 0.0 \
  --rank-metric pop_est \
  --outdir ./data

