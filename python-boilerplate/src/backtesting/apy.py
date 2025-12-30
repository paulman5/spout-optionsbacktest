import os
import sys
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import databento as db
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from scipy.optimize import brentq
from scipy.stats import norm

from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import ContractType
from alpaca.trading.requests import GetCalendarRequest

# Environment variable setup
if "google.colab" in sys.modules:
    # In Google Colab environment, we will fetch API keys from Secrets.
    # Please set ALPACA_API_KEY, ALPACA_SECRET_KEY, DATABENTO_API_KEY in Google Colab's Secrets from the left sidebar
    from google.colab import userdata

    ALPACA_API_KEY = userdata.get("ALPACA_API_KEY")
    ALPACA_SECRET_KEY = userdata.get("ALPACA_SECRET_KEY")
    DATABENTO_API_KEY = userdata.get("DATABENTO_API_KEY")
else:
    # Please safely store your API keys and never commit them to the repository (use .gitignore)
    # Load environment variables from environment file (e.g., .env)
    load_dotenv()
    # API credentials for Alpaca's Trading API and Databento API
    ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
    ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
    DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")

# We use paper environment for this example
ALPACA_PAPER_TRADE = True  # Please do not modify this. This example is for paper trading only.

# Below are the variables for development this documents (Please do not change these variables)
TRADE_API_URL = None
TRADE_API_WSS = None
DATA_API_URL = None
OPTION_STREAM_DATA_WSS = None

# Signed Alpaca clients
from alpaca import __version__ as alpacapy_version

class _ClientMixin:
    def _get_default_headers(self):
        headers = self._get_auth_headers()
        headers["User-Agent"] = "APCA-PY/" + alpacapy_version + "-ZERO-DTE-NOTEBOOK"
        return headers

class TradingClientSigned(_ClientMixin, TradingClient): pass
class OptionHistoricalDataClientSigned(_ClientMixin, OptionHistoricalDataClient): pass
class StockHistoricalDataClientSigned(_ClientMixin, StockHistoricalDataClient): pass



