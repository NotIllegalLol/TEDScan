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
            from rapidfuzz import fuzz
            self.yf = yf
            self.fuzz = fuzz
            self.enabled = True
            self.logger.info("✓ Stock lookup enabled (yfinance + rapidfuzz)")
        except ImportError:
            self.enabled = False
            self.logger.warning("⚠ Stock lookup disabled - install yfinance and rapidfuzz")
        
        # Cache to avoid repeated lookups
        self.cache = {}
    
    def find_ticker(self, company_name: str, country: str = None) -> Optional[str]:
        """Find stock ticker using fuzzy matching"""
        if not self.enabled or not company_name or company_name == "N/A":
            return None
        
        # Check cache
        cache_key = company_name.lower()
        if cache_key in self.cache:
            self.logger.info(f"Cache hit for {company_name}: {self.cache[cache_key]}")
            return self.cache[cache_key]
        
        self.logger.info(f"Looking up ticker for: {company_name} (Country: {country})")
        
        try:
            # Clean company name - remove common suffixes first
            clean_name = company_name.strip()
            
            # Remove common legal suffixes
            remove_suffixes = [' AB', ' AS', ' OYJ', ' SPA', ' S.P.A.', ' SA', ' S.A.', 
                              ' AG', ' A.G.', ' PLC', ' LTD', ' LIMITED', ' INC', 
                              ' INCORPORATED', ' CORP', ' CORPORATION', ' NV', ' BV',
                              ' GMBH', ' SE', ' ASA', ' OY']
            
            for suffix in remove_suffixes:
                if clean_name.upper().endswith(suffix):
                    clean_name = clean_name[:-len(suffix)].strip()
                    break
            
            self.logger.info(f"Cleaned name: {clean_name}")
            
            # Get exchanges to search
            exchanges = self._get_exchanges_for_country(country)
            self.logger.info(f"Searching exchanges: {exchanges}")
            
            # Try each exchange
            for exchange in exchanges:
                if exchange:
                    search_term = f"{clean_name}.{exchange}"
                else:
                    search_term = clean_name
                
                self.logger.info(f"Trying: {search_term}")
                
                try:
                    ticker = self.yf.Ticker(search_term)
                    # Force fetch to validate ticker exists
                    info = ticker.info
                    
                    # Check if we got valid data
                    if info and len(info) > 1 and info.get('symbol'):
                        symbol = info['symbol']
                        self.logger.info(f"✓ Found ticker: {symbol}")
                        self.cache[cache_key] = symbol
                        return symbol
                except Exception as e:
                    self.logger.debug(f"Failed {search_term}: {e}")
                    continue
            
            # Cache negative result
            self.logger.info(f"✗ No ticker found for {company_name}")
            self.cache[cache_key] = None
            return None
            
        except Exception as e:
            self.logger.error(f"Ticker lookup error for {company_name}: {e}")
            return None
    
    def _get_exchanges_for_country(self, country: str) -> List[str]:
        """Get likely stock exchanges for a country"""
        if not country:
            return ['', 'US', 'L', 'PA']
        
        country = country.upper()
        exchange_map = {
            'SE': ['ST'],  # Sweden
            'FI': ['HE'],  # Finland
            'NO': ['OL'],  # Norway
            'DK': ['CO'],  # Denmark
            'DE': ['DE', 'F'],  # Germany
            'FR': ['PA'],  # France
            'GB': ['L'],   # UK
            'IT': ['MI'],  # Italy
            'ES': ['MC'],  # Spain
            'NL': ['AS'],  # Netherlands
            'CH': ['SW'],  # Switzerland
            'US': [''],    # US (no suffix)
        }
        
        return exchange_map.get(country, ['', 'US', 'L'])
    
    def get_stock_info(self, ticker: str) -> Optional[Dict]:
        """Get stock price and 5-day performance"""
        if not self.enabled or not ticker:
            return None
        
        try:
            stock = self.yf.Ticker(ticker)
            hist = stock.history(period='5d')
            
            if hist.empty:
                return None
            
            current_price = hist['Close'].iloc[-1]
            price_5d_ago = hist['Close'].iloc[0]
            change_pct = ((current_price - price_5d_ago) / price_5d_ago) * 100
            
            info = stock.info
            currency = info.get('currency', 'USD')
            
            return {
                'ticker': ticker,
                'price': current_price,
                'change_5d': change_pct,
                'currency': currency
            }
            
        except Exception as e:
            self.logger.debug(f"Failed to get stock info for {ticker}: {e}")
            return None


class TEDDataCollector:
    """Complete TED collector with 3-step logic"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key
        self.search_api_url = "https://api.ted.europa.eu/v3/notices/search"
        self.logger = logging.getLogger(__name__)

        # Exchange rates (Nov 2024)
        self.fallback_rates = {
            'USD': 0.92, 'GBP': 1.17, 'CHF': 1.06,
            'SEK': 0.088, 'DKK': 0.134, 'NOK': 0.086,
            'PLN': 0.23, 'CZK': 0.040, 'HUF': 0.0025,
            'RON': 0.20, 'BGN': 0.51, 'HRK': 0.13,
            'ISK': 0.0066, 'TRY': 0.027, 'RUB': 0.010
        }

    def convert_to_eur(self, amount: float, currency: str) -> float:
        """Convert amount to EUR"""
        if currency == "EUR" or not currency:
            return amount

        if currency in self.fallback_rates:
            return amount * self.fallback_rates[currency]

        self.logger.warning(f"Unknown currency: {currency}")
        return amount

    def fetch_all_contracts(self, days_back: int = 2) -> List[Dict]:
        """Fetch contracts from TED API"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            start_str = start_date.strftime("%Y%m%d")
            end_str = end_date.strftime("%Y%m%d")

            self.logger.info(f"Fetching contracts from {start_str} to {end_str}")

            all_notices = []
            page = 1
            limit = 50
            max_pages = 10

            while page <= max_pages:
                self.logger.info(f"Fetching page {page}...")

                query = f"PD={start_str}" if start_str == end_str else f"PD>={start_str} AND PD<={end_str}"

                request_body = {
                    "query": query,
                    "fields": [
                        "publication-number", "publication-date", "form-type", "notice-type",
                        "buyer-name", "buyer-country", "buyer-city",
                        "notice-title", "contract-nature", "announcement-url",
                        "identifier-lot", "estimated-value-lot", "estimated-value-cur-lot",
                        "winner-name", "winner-country", "winner-city",
                        "tender-value", "tender-value-cur", "links"
                    ],
                    "page": page,
                    "limit": limit,
                    "scope": "ALL",
                    "paginationMode": "PAGE_NUMBER"
                }

                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }

                if self.api_key:
                    headers["apikey"] = self.api_key

                try:
                    response = requests.post(
                        self.search_api_url,
                        json=request_body,
                        headers=headers,
                        timeout=30
                    )

                    if response.status_code != 200:
                        self.logger.error(f"API status {response.status_code}")
                        break

                    data = response.json()
                    notices = data.get("notices", [])
                    total = data.get("total", 0)

                    if not notices:
                        break

                    all_notices.extend(notices)
                    self.logger.info(f"Got {len(notices)} notices (total: {len(all_notices)}/{total})")

                    if len(all_notices) >= total or len(notices) < limit:
                        break

                    page += 1
                    time.sleep(0.5)

                except requests.exceptions.RequestException as e:
                    self.logger.error(f"Request error: {e}")
                    break

            self.logger.info(f"Total fetched: {len(all_notices)}")
            return all_notices

        except Exception as e:
            self.logger.error(f"Fetch error: {e}", exc_info=True)
            return []

    def match_winners_to_lots(self, notice: Dict) -> List[Tuple[Dict, str, str]]:
        """Match winners to lots"""
        lots = []

        def to_list(data):
            if data is None:
                return []
            return data if isinstance(data, list) else [data]

        def extract_from_dict(data):
            if data is None:
                return None
            if isinstance(data, dict):
                for lang in ['eng', 'swe', 'deu', 'fra']:
                    if lang in data and data[lang]:
                        val = data[lang]
                        return val[0] if isinstance(val, list) else val
                first_val = next(iter(data.values())) if data else None
                return first_val[0] if isinstance(first_val, list) else first_val
            return data

        lot_ids = to_list(notice.get("identifier-lot"))
        lot_estimated_values = to_list(notice.get("estimated-value-lot"))
        lot_estimated_currencies = to_list(notice.get("estimated-value-cur-lot"))

        winner_names_raw = notice.get("winner-name")
        winner_names = []
        if isinstance(winner_names_raw, dict):
            winner_name_val = extract_from_dict(winner_names_raw)
            winner_names = to_list(winner_name_val)
        else:
            winner_names = to_list(winner_names_raw)

        winner_countries = to_list(notice.get("winner-country"))

        winner_cities_raw = notice.get("winner-city")
        winner_cities = []
        if isinstance(winner_cities_raw, dict):
            winner_city_val = extract_from_dict(winner_cities_raw)
            winner_cities = to_list(winner_city_val)
        else:
            winner_cities = to_list(winner_cities_raw)

        tender_values = to_list(notice.get("tender-value"))
        tender_currencies = to_list(notice.get("tender-value-cur"))

        num_lots = max(len(lot_ids), len(lot_estimated_values), len(tender_values), 1)

        for i in range(num_lots):
            lot_data = {
                "lot_id": lot_ids[i] if i < len(lot_ids) else f"LOT-{i+1}",
                "estimated_value": lot_estimated_values[i] if i < len(lot_estimated_values) else None,
                "tender_value": tender_values[i] if i < len(tender_values) else None,
            }

            est_currency = (lot_estimated_currencies[0] if len(lot_estimated_currencies) == 1
                           else lot_estimated_currencies[i] if i < len(lot_estimated_currencies)
                           else "EUR")

            tender_currency = (tender_currencies[0] if len(tender_currencies) == 1
                              else tender_currencies[i] if i < len(tender_currencies)
                              else "EUR")

            lot_data["winner_name"] = (winner_names[i] if len(winner_names) == num_lots and i < len(winner_names)
                                      else winner_names[0] if len(winner_names) == 1
                                      else "N/A")

            lot_data["winner_country"] = (winner_countries[i] if len(winner_countries) == num_lots and i < len(winner_countries)
                                         else winner_countries[0] if len(winner_countries) == 1
                                         else "N/A")

            lot_data["winner_city"] = (winner_cities[i] if len(winner_cities) == num_lots and i < len(winner_cities)
                                      else winner_cities[0] if len(winner_cities) == 1
                                      else "N/A")

            lots.append((lot_data, est_currency, tender_currency))

        return lots

    def filter_high_value_results(self, notices: List[Dict], min_value_eur: float = 15_000_000) -> List[Dict]:
        """Filter for result contracts >= 15M EUR"""
        self.logger.info("STEP 3: FILTERING HIGH-VALUE RESULTS")
        self.logger.info(f"Filter: Form='result' AND Value >= €{min_value_eur:,.0f}")
        self.logger.info(f"Total notices to filter: {len(notices)}")

        high_value_contracts = []
        
        # Debug counters
        result_count = 0
        has_value_count = 0

        for notice in notices:
            try:
                form_type = notice.get("form-type", "")
                
                # Count result types
                if form_type and 'result' in str(form_type).lower():
                    result_count += 1
                else:
                    continue

                lots = self.match_winners_to_lots(notice)
                if not lots:
                    continue

                total_eur = 0
                converted_lots = []

                for lot_data, est_currency, tender_currency in lots:
                    tender_value = lot_data.get('tender_value')

                    if tender_value is not None:
                        try:
                            tender_float = float(tender_value)
                            eur_value = self.convert_to_eur(tender_float, tender_currency)
                            total_eur += eur_value

                            converted_lots.append({
                                "lot_id": lot_data['lot_id'],
                                "winner_name": lot_data['winner_name'],
                                "winner_country": lot_data['winner_country'],
                                "winner_city": lot_data['winner_city'],
                                "tender_value": tender_float,
                                "tender_currency": tender_currency,
                                "eur_value": eur_value
                            })
                        except (ValueError, TypeError):
                            continue

                if total_eur > 0:
                    has_value_count += 1
                    self.logger.info(f"Result contract: {notice.get('publication-number')} - €{total_eur:,.0f}")

                if total_eur >= min_value_eur:
                    high_value_contracts.append({
                        "publication_number": notice.get("publication-number", "N/A"),
                        "publication_date": notice.get("publication-date", "N/A"),
                        "form_type": form_type,
                        "buyer_name": notice.get("buyer-name", "N/A"),
                        "buyer_country": notice.get("buyer-country", "N/A"),
                        "buyer_city": notice.get("buyer-city", "N/A"),
                        "title": notice.get("notice-title", "N/A"),
                        "url": notice.get("announcement-url", "N/A"),
                        "total_eur": total_eur,
                        "lots": converted_lots
                    })

                    self.logger.info(f"✓ HIGH VALUE: {notice.get('publication-number')} - €{total_eur:,.0f}")

            except Exception as e:
                self.logger.warning(f"Error processing: {e}")
                continue

        self.logger.info(f"Debug: Found {result_count} result-type contracts")
        self.logger.info(f"Debug: Found {has_value_count} with tender values")
        self.logger.info(f"Found {len(high_value_contracts)} high-value contracts (>= €{min_value_eur:,.0f})")
        return high_value_contracts


class TEDTelegramBot:
    """Telegram bot with webhook support and stock lookup"""

    def __init__(self, bot_token: str, chat_id: str, ted_api_key: str):
        self.bot = telebot.TeleBot(bot_token)
        self.chat_id = chat_id
        self.collector = TEDDataCollector(api_key=ted_api_key)
        self.stock_lookup = StockLookup()
        self.is_running = False
        self.notified = set()

        self._setup_handlers()

    def _setup_handlers(self):

        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            text = """
🤖 *TED Contract Monitor Bot*

Running 24/7 on Render.com!

*Commands:*
/start - Show this message
/status - Bot status
/scan - Scan now
/stop - Stop auto-scan
/resume - Resume auto-scan

*Monitoring:*
✅ Scans every 10 minutes
✅ Form type: result
✅ Min value: €15,000,000
✅ Stock ticker lookup
✅ 5-day price change

You'll receive instant alerts for large contracts!
            """
            self.bot.reply_to(message, text, parse_mode='Markdown')

        @self.bot.message_handler(commands=['status'])
        def send_status(message):
            status = "🟢 Running" if self.is_running else "🔴 Stopped"
            stock_status = "✅ Enabled" if self.stock_lookup.enabled else "❌ Disabled"
            text = f"""
*Status:* {status}
*Interval:* 10 minutes
*Notified:* {len(self.notified)} contracts
*Threshold:* €15,000,000
*Stock Lookup:* {stock_status}
*Platform:* Render.com
            """
            self.bot.reply_to(message, text, parse_mode='Markdown')

        @self.bot.message_handler(commands=['scan'])
        def run_scan(message):
            self.bot.reply_to(message, "🔍 Scanning TED...")
            count = self._scan()
            if count == 0:
                self.bot.reply_to(message, "✅ No new contracts found")
            else:
                self.bot.reply_to(message, f"✅ Found {count} new contracts!")
        
        @self.bot.message_handler(commands=['test'])
        def test_scan(message):
            """Test with lower threshold to see what's available"""
            self.bot.reply_to(message, "🔍 Testing with €5M threshold...")
            try:
                notices = self.collector.fetch_all_contracts(days_back=2)
                if not notices:
                    self.bot.reply_to(message, "No contracts fetched")
                    return
                
                # Test with 5M threshold
                test_contracts = self.collector.filter_high_value_results(notices, min_value_eur=5_000_000)
                
                if test_contracts:
                    self.bot.reply_to(message, f"Found {len(test_contracts)} contracts >= €5M")
                else:
                    self.bot.reply_to(message, "No contracts found even at €5M")
            except Exception as e:
                self.bot.reply_to(message, f"Error: {str(e)[:100]}")

        @self.bot.message_handler(commands=['stop'])
        def stop(message):
            self.is_running = False
            self.bot.reply_to(message, "⏸️ Stopped. Use /resume to restart")

        @self.bot.message_handler(commands=['resume'])
        def resume(message):
            if not self.is_running:
                self.is_running = True
                self.bot.reply_to(message, "▶️ Resumed!")
            else:
                self.bot.reply_to(message, "Already running")

    def _scan(self) -> int:
        """Run 3-step scan"""
        try:
            logger.info("="*60)
            logger.info("STARTING TED SCAN")
            logger.info("="*60)

            notices = self.collector.fetch_all_contracts(days_back=7)
            if not notices:
                logger.info("No contracts found")
                return 0

            high_value_contracts = self.collector.filter_high_value_results(notices)

            new_count = 0
            for contract in high_value_contracts:
                pub_num = str(contract["publication_number"])

                if pub_num in self.notified:
                    continue

                self._notify(contract)
                self.notified.add(pub_num)
                new_count += 1

            logger.info(f"COMPLETE: {new_count} new alerts sent")
            logger.info("="*60)
            return new_count

        except Exception as e:
            logger.error(f"Scan error: {e}", exc_info=True)
            self._send(f"❌ Error: {str(e)[:100]}")
            return 0

    def _notify(self, contract: dict):
        """Send trimmed contract notification with stock info"""
        try:
            # Build lots section with stock info
            lots_text = ""
            for lot in contract["lots"]:
                winner_name = lot['winner_name']
                winner_country = lot['winner_country']
                
                # Format value
                if lot['tender_currency'] == 'EUR':
                    value_text = f"€{lot['eur_value']:,.0f}"
                else:
                    value_text = f"€{lot['eur_value']:,.0f} (from {lot['tender_value']:,.0f} {lot['tender_currency']})"
                
                lots_text += f"\n*Lot {lot['lot_id']}* - {value_text}\n"
                lots_text += f"Winner: {winner_name}\n"
                
                # Try to find stock ticker
                ticker = self.stock_lookup.find_ticker(winner_name, winner_country)
                if ticker:
                    stock_info = self.stock_lookup.get_stock_info(ticker)
                    if stock_info:
                        change_emoji = "📈" if stock_info['change_5d'] > 0 else "📉"
                        lots_text += f"Stock: `{ticker}` {change_emoji}\n"
                        lots_text += f"Price: {stock_info['price']:.2f} {stock_info['currency']}\n"
                        lots_text += f"5d Change: {stock_info['change_5d']:+.2f}%\n"
                    else:
                        lots_text += f"Stock: `{ticker}` (no data)\n"
                
                lots_text += "\n"

            msg = f"""
🚨 *HIGH-VALUE CONTRACT* 🚨

*Publication:* {contract['publication_number']}
*Date:* {contract['publication_date']}
*Form:* {contract['form_type']}

💰 *TOTAL VALUE: €{contract['total_eur']:,.0f}*

*Buyer:* {contract['buyer_name']}
*Country:* {contract['buyer_country']}

*LOTS:*{lots_text}
🔗 [View Contract]({contract['url']})
            """

            self._send(msg)

        except Exception as e:
            logger.error(f"Notify error: {e}")

    def _send(self, message: str):
        """Send Telegram message"""
        try:
            self.bot.send_message(
                self.chat_id,
                message,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"Send error: {e}")
            try:
                self.bot.send_message(self.chat_id, message)
            except:
                pass

    def _monitoring_loop(self):
        """Auto-scan every 10 minutes"""
        logger.info("Monitoring started")
        self._send("✅ *TED Monitor Started!*\n\nScanning every 10 minutes.\nYou'll receive alerts for contracts ≥€15M.")

        while self.is_running:
            try:
                self._scan()

                if self.is_running:
                    logger.info("Waiting 10 minutes...")
                    time.sleep(600)

            except Exception as e:
                logger.error(f"Loop error: {e}", exc_info=True)
                time.sleep(600)

    def process_update(self, update):
        """Process webhook update"""
        try:
            self.bot.process_new_updates([update])
        except Exception as e:
            logger.error(f"Update processing error: {e}")


# Global bot instance
bot_instance = None


@app.route('/', methods=['GET'])
def home():
    """Home endpoint"""
    return """
    <h1>🤖 TED Contract Monitor</h1>
    <p>Status: Running on Render.com</p>
    <p>Mode: Webhook + Background Monitoring</p>
    <p>Scan Interval: 10 minutes</p>
    <p>Features: Stock ticker lookup + 5-day performance</p>
    """, 200


@app.route('/health', methods=['GET'])
def health():
    """Health check for UptimeRobot"""
    status = "running" if bot_instance and bot_instance.is_running else "stopped"
    return {
        'status': 'ok',
        'monitoring': status,
        'timestamp': datetime.now().isoformat()
    }, 200


@app.route('/webhook', methods=['POST'])
def webhook():
    """Telegram webhook endpoint"""
    if bot_instance:
        try:
            json_string = request.get_data().decode('utf-8')
            update = telebot.types.Update.de_json(json_string)
            bot_instance.process_update(update)
            return '', 200
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            return '', 500
    return '', 404


def main():
    """Main entry point"""
    global bot_instance

    # Get credentials from environment
    BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8395744940:AAGmZVdj1l-QfZ4zqGP_9XOOvO9EbsnyWLw')
    CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '2133274440')
    TED_KEY = os.environ.get('TED_API_KEY', '0f0d8c2f68bb46bab7afa51c46053433')
    WEBHOOK_URL = os.environ.get('WEBHOOK_URL')

    logger.info("="*60)
    logger.info("TED TELEGRAM BOT - RENDER.COM WEBHOOK MODE")
    logger.info("="*60)
    logger.info(f"Chat ID: {CHAT_ID}")
    logger.info(f"Webhook: {WEBHOOK_URL}")
    logger.info("="*60)

    # Initialize bot
    bot_instance = TEDTelegramBot(
        bot_token=BOT_TOKEN,
        chat_id=CHAT_ID,
        ted_api_key=TED_KEY
    )

    # Set webhook if URL provided
    if WEBHOOK_URL:
        try:
            webhook_endpoint = f"{WEBHOOK_URL}/webhook"
            bot_instance.bot.remove_webhook()
            time.sleep(1)
            bot_instance.bot.set_webhook(url=webhook_endpoint)
            logger.info(f"Webhook set to: {webhook_endpoint}")
        except Exception as e:
            logger.error(f"Webhook setup failed: {e}")

    # Start monitoring in background thread
    bot_instance.is_running = True
    Thread(target=bot_instance._monitoring_loop, daemon=True).start()

    # Run Flask app
    port = int(os.environ.get('PORT', 10000))
    logger.info(f"Starting Flask on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == "__main__":
    main()
