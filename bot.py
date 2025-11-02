"""
TED Telegram Bot - Render.com Web Service Version
Uses webhooks + Flask to stay alive 24/7 on free tier
With stock ticker lookup for winners
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from threading import Thread
import telebot
import requests
from typing import List, Dict, Tuple, Optional
from flask import Flask, request

# Setup logging
logging.basicConfig(
level=logging.INFO,
format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
handlers=[
logging.FileHandler('bot.log'),
logging.StreamHandler(sys.stdout)
]
)
logger = logging.getLogger(__name__)

# Flask app for webhooks
app = Flask(__name__)


class StockLookup:
"""Lookup stock tickers and prices for companies"""

def __init__(self):
self.logger = logging.getLogger(__name__)
try:
import yfinance as yf
            from fuzzywuzzy import fuzz
            from rapidfuzz import fuzz
self.yf = yf
self.fuzz = fuzz
self.enabled = True
            self.logger.info("✓ Stock lookup enabled (yfinance + fuzzywuzzy)")
            self.logger.info("✓ Stock lookup enabled (yfinance + rapidfuzz)")
except ImportError:
self.enabled = False
            self.logger.warning("⚠ Stock lookup disabled - install yfinance and fuzzywuzzy")
            self.logger.warning("⚠ Stock lookup disabled - install yfinance and rapidfuzz")

# Cache to avoid repeated lookups
self.cache = {}
