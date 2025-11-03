"""
TED Telegram Bot - FIXED VERSION
Sends notifications for contracts >= €15M
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
            self.logger.info("✓ Stock lookup enabled")
        except ImportError:
            self.enabled = False
            self.logger.warning("⚠ Stock lookup disabled")
        
        self.cache = {}
    
    def find_ticker(self, company_name: str, country: str = None) -> Optional[str]:
        """Find stock ticker"""
        if not self.enabled or not company_name or company_name == "N/A":
            return None
        
        cache_key = company_name.lower()
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            clean_name = company_name.strip()
            
            # Remove legal suffixes
            remove_suffixes = [' AB', ' AS', ' OYJ', ' SPA', ' S.P.A.', ' SA', ' S.A.', 
                              ' AG', ' PLC', ' LTD', ' LIMITED', ' INC', ' CORP', ' NV', ' BV',
                              ' GMBH', ' SE', ' ASA', ' OY']
            
            for suffix in remove_suffixes:
                if clean_name.upper().endswith(suffix):
                    clean_name = clean_name[:-len(suffix)].strip()
                    break
            
            exchanges = self._get_exchanges_for_country(country)
            
            for exchange in exchanges:
                search_term = f"{clean_name}.{exchange}" if exchange else clean_name
                
                try:
                    ticker = self.yf.Ticker(search_term)
                    info = ticker.info
                    
                    if info and len(info) > 1 and info.get('symbol'):
                        symbol = info['symbol']
                        self.cache[cache_key] = symbol
                        return symbol
                except:
                    continue
            
            self.cache[cache_key] = None
            return None
            
        except Exception as e:
            self.logger.error(f"Ticker lookup error: {e}")
            return None
    
    def _get_exchanges_for_country(self, country: str) -> List[str]:
        """Get stock exchanges for country"""
        if not country:
            return ['', 'US', 'L', 'PA']
        
        country = country.upper()
        exchange_map = {
            'SE': ['ST'], 'FI': ['HE'], 'NO': ['OL'], 'DK': ['CO'],
            'DE': ['DE', 'F'], 'FR': ['PA'], 'GB': ['L'], 'IT': ['MI'],
            'ES': ['MC'], 'NL': ['AS'], 'CH': ['SW'], 'US': [''],
        }
        
        return exchange_map.get(country, ['', 'US', 'L'])
    
    def get_stock_info(self, ticker: str) -> Optional[Dict]:
        """Get stock price and 5-day performance"""
        if not self.enabled or not ticker:
            return None
        
        try:
            stock = self.yf.Ticker(ticker)
            hist = stock.history(period='5d')
            
            if hist.empty or len(hist) < 2:
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
            self.logger.error(f"Stock info error: {e}")
            return None


class TEDDataCollector:
    """TED collector with currency conversion"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key
        self.search_api_url = "https://api.ted.europa.eu/v3/notices/search"
        self.logger = logging.getLogger(__name__)

        # Exchange rates
        self.rates = {
            'EUR': 1.0, 'USD': 0.92, 'GBP': 1.17, 'CHF': 1.06,
            'SEK': 0.088, 'DKK': 0.134, 'NOK': 0.086,
            'PLN': 0.23, 'CZK': 0.040, 'HUF': 0.0025,
            'RON': 0.20, 'BGN': 0.51, 'HRK': 0.13,
        }

    def convert_to_eur(self, amount: float, currency: str) -> float:
        """Convert to EUR"""
        if not currency or currency == "EUR":
            return amount
        return amount * self.rates.get(currency.upper(), 1.0)

    def fetch_all_contracts(self, days_back: int = 1) -> List[Dict]:
        """Fetch contracts from TED API - ONLY LAST 1 DAY"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            start_str = start_date.strftime("%Y%m%d")
            end_str = end_date.strftime("%Y%m%d")

            self.logger.info(f"📡 Fetching from {start_str} to {end_str} (last {days_back} day)")

            all_notices = []
            page = 1
            limit = 50
            max_pages = 10  # Reduced for 1 day

            while page <= max_pages:
                query = f"PD>={start_str} AND PD<={end_str}"

                request_body = {
                    "query": query,
                    "fields": [
                        "publication-number", "publication-date", "form-type",
                        "buyer-name", "buyer-country", "buyer-city",
                        "notice-title", "contract-nature",
                        "identifier-lot", "estimated-value-lot", "estimated-value-cur-lot",
                        "winner-name", "winner-country", "winner-city",
                        "tender-value", "tender-value-cur", "links"
                    ],
                    "page": page,
                    "limit": limit,
                    "scope": "ALL",
                    "paginationMode": "PAGE_NUMBER"
                }

                headers = {"Content-Type": "application/json", "Accept": "application/json"}
                if self.api_key:
                    headers["apikey"] = self.api_key

                response = requests.post(self.search_api_url, json=request_body, 
                                        headers=headers, timeout=30)

                if response.status_code != 200:
                    self.logger.error(f"API error {response.status_code}")
                    break

                data = response.json()
                notices = data.get("notices", [])
                
                if not notices:
                    break

                all_notices.extend(notices)
                self.logger.info(f"Page {page}: {len(notices)} notices (total: {len(all_notices)})")

                if len(notices) < limit:
                    break

                page += 1
                time.sleep(0.5)

            # ADDITIONAL FILTER: Only keep contracts from last 24 hours
            cutoff_datetime = datetime.now() - timedelta(hours=24)
            filtered_notices = []
            
            for notice in all_notices:
                pub_date_str = notice.get("publication-date")
                if pub_date_str:
                    try:
                        # Parse "2025-11-03+01:00" format
                        pub_date_clean = pub_date_str.split('+')[0].split('T')[0]
                        pub_date = datetime.strptime(pub_date_clean, "%Y-%m-%d")
                        
                        # Only include if published in last 24 hours
                        if pub_date >= cutoff_datetime.replace(hour=0, minute=0, second=0):
                            filtered_notices.append(notice)
                            self.logger.debug(f"Including: {notice.get('publication-number')} from {pub_date_clean}")
                        else:
                            self.logger.debug(f"Skipping old: {notice.get('publication-number')} from {pub_date_clean}")
                    except Exception as e:
                        # If parsing fails, include it to be safe
                        self.logger.debug(f"Date parse error, including anyway: {e}")
                        filtered_notices.append(notice)
                else:
                    # No date, include it
                    filtered_notices.append(notice)

            self.logger.info(f"✓ Fetched {len(all_notices)} total, filtered to {len(filtered_notices)} from last 24 hours")
            return filtered_notices

        except Exception as e:
            self.logger.error(f"Fetch error: {e}", exc_info=True)
            return []

    def match_winners_to_lots(self, notice: Dict) -> List[Dict]:
        """Match winners to lots with currency"""
        lots = []

        def to_list(data):
            return [] if data is None else (data if isinstance(data, list) else [data])

        def extract_from_dict(data):
            if not isinstance(data, dict):
                return data
            for lang in ['eng', 'swe', 'deu', 'fra', 'spa']:
                if lang in data and data[lang]:
                    val = data[lang]
                    return val[0] if isinstance(val, list) else val
            first_val = next(iter(data.values())) if data else None
            return first_val[0] if isinstance(first_val, list) else first_val

        lot_ids = to_list(notice.get("identifier-lot"))
        lot_est_values = to_list(notice.get("estimated-value-lot"))
        lot_est_currencies = to_list(notice.get("estimated-value-cur-lot"))

        winner_names_raw = notice.get("winner-name")
        if isinstance(winner_names_raw, dict):
            winner_names = to_list(extract_from_dict(winner_names_raw))
        else:
            winner_names = to_list(winner_names_raw)

        winner_countries = to_list(notice.get("winner-country"))
        
        winner_cities_raw = notice.get("winner-city")
        if isinstance(winner_cities_raw, dict):
            winner_cities = to_list(extract_from_dict(winner_cities_raw))
        else:
            winner_cities = to_list(winner_cities_raw)

        tender_values = to_list(notice.get("tender-value"))
        tender_currencies = to_list(notice.get("tender-value-cur"))

        num_lots = max(len(lot_ids), len(tender_values), 1)

        for i in range(num_lots):
            # Get currency
            tender_cur = (tender_currencies[i] if i < len(tender_currencies) 
                         else tender_currencies[0] if len(tender_currencies) == 1 
                         else "EUR")
            
            est_cur = (lot_est_currencies[i] if i < len(lot_est_currencies)
                      else lot_est_currencies[0] if len(lot_est_currencies) == 1
                      else "EUR")

            # Get winner
            winner_name = (winner_names[i] if len(winner_names) == num_lots and i < len(winner_names)
                          else winner_names[0] if len(winner_names) == 1
                          else "N/A")
            
            winner_country = (winner_countries[i] if len(winner_countries) == num_lots and i < len(winner_countries)
                             else winner_countries[0] if len(winner_countries) == 1
                             else "N/A")
            
            winner_city = (winner_cities[i] if len(winner_cities) == num_lots and i < len(winner_cities)
                          else winner_cities[0] if len(winner_cities) == 1
                          else "N/A")

            lot_data = {
                "lot_id": lot_ids[i] if i < len(lot_ids) else f"LOT-{i+1}",
                "estimated_value": lot_est_values[i] if i < len(lot_est_values) else None,
                "estimated_currency": est_cur,
                "tender_value": tender_values[i] if i < len(tender_values) else None,
                "tender_currency": tender_cur,
                "winner_name": winner_name,
                "winner_country": winner_country,
                "winner_city": winner_city,
            }
            lots.append(lot_data)

        return lots

    def filter_high_value_results(self, notices: List[Dict], min_value_eur: float = 15_000_000) -> List[Dict]:
        """
        STEP-BY-STEP FILTERING:
        1. Get ALL contracts
        2. Filter for form-type = "result" 
        3. Convert ALL currencies to EUR
        4. Filter for total >= €15M
        """
        self.logger.info("="*60)
        self.logger.info("FILTERING PROCESS")
        self.logger.info("="*60)
        self.logger.info(f"Step 1: Input contracts: {len(notices)}")

        # Helper to extract from i18n dicts
        def extract_from_dict(data):
            if not isinstance(data, dict):
                return data
            for lang in ['eng', 'swe', 'deu', 'fra', 'spa', 'ita', 'nld', 'pol', 'ces', 'hun']:
                if lang in data and data[lang]:
                    val = data[lang]
                    return val[0] if isinstance(val, list) else val
            first = next(iter(data.values())) if data else None
            return first[0] if isinstance(first, list) else first

        # STEP 2: Filter for "result" form type
        result_notices = []
        for notice in notices:
            form_type = notice.get("form-type", "")
            if form_type == "result":  # EXACT match
                result_notices.append(notice)
        
        self.logger.info(f"Step 2: Result contracts: {len(result_notices)}")

        # STEP 3 & 4: Convert to EUR and filter for >= €15M
        high_value = []
        
        for notice in result_notices:
            try:
                lots = self.match_winners_to_lots(notice)
                if not lots:
                    continue

                # Calculate total EUR value
                total_eur = 0
                converted_lots = []

                for lot in lots:
                    tender_value = lot.get('tender_value')
                    tender_currency = lot.get('tender_currency', 'EUR')
                    
                    if tender_value:
                        try:
                            tender_float = float(tender_value)
                            eur_value = self.convert_to_eur(tender_float, tender_currency)
                            total_eur += eur_value

                            converted_lots.append({
                                "lot_id": lot['lot_id'],
                                "winner_name": lot['winner_name'],
                                "winner_country": lot['winner_country'],
                                "winner_city": lot['winner_city'],
                                "tender_value": tender_float,
                                "tender_currency": tender_currency,
                                "eur_value": eur_value
                            })
                        except (ValueError, TypeError):
                            continue

                # Check if meets €15M threshold
                if total_eur >= min_value_eur:
                    # Extract URL from links object
                    links = notice.get('links', {})
                    html_links = links.get('html', {}) if isinstance(links, dict) else {}
                    
                    url = "N/A"
                    for lang_code in ['ENG', 'SWE', 'DEU', 'FRA', 'SPA', 'ITA', 'NLD']:
                        if lang_code in html_links:
                            url = html_links[lang_code]
                            break
                    
                    # Extract buyer info
                    buyer_name = extract_from_dict(notice.get("buyer-name", "N/A"))
                    
                    buyer_country_raw = notice.get("buyer-country", "N/A")
                    if isinstance(buyer_country_raw, list):
                        buyer_country = buyer_country_raw[0] if buyer_country_raw else "N/A"
                    else:
                        buyer_country = buyer_country_raw
                    
                    buyer_city = extract_from_dict(notice.get("buyer-city", "N/A"))
                    title = extract_from_dict(notice.get("notice-title", "N/A"))
                    
                    high_value.append({
                        "publication_number": notice.get("publication-number", "N/A"),
                        "publication_date": notice.get("publication-date", "N/A"),
                        "form_type": "result",
                        "buyer_name": buyer_name,
                        "buyer_country": buyer_country,
                        "buyer_city": buyer_city,
                        "title": title,
                        "url": url,
                        "total_eur": total_eur,
                        "lots": converted_lots
                    })

                    self.logger.info(f"  ✅ FOUND: {notice.get('publication-number')} = €{total_eur:,.0f}")

            except Exception as e:
                self.logger.error(f"Processing error: {e}", exc_info=True)
                continue

        self.logger.info(f"Step 4: High-value contracts (>= €{min_value_eur:,.0f}): {len(high_value)}")
        self.logger.info("="*60)
        
        return high_value


class TEDTelegramBot:
    """Telegram bot with webhook support"""

    def __init__(self, bot_token: str, chat_id: str, ted_api_key: str):
        self.bot = telebot.TeleBot(bot_token)
        self.chat_id = chat_id
        self.collector = TEDDataCollector(api_key=ted_api_key)
        self.stock_lookup = StockLookup()
        self.is_running = False
        self.notified = {}  # Changed to dict with timestamps

        self._setup_handlers()

    def _setup_handlers(self):

        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            text = """
🤖 *TED Contract Monitor Bot*

*Commands:*
/start - Show this message
/status - Bot status
/scan - Scan now (24 hours)
/test - Test scan (lower threshold)
/stock Company Name - Lookup stock
/clear - Clear notification history
/stop - Stop monitoring
/resume - Resume monitoring

*Monitoring:*
✅ Auto-scans every 10 minutes
✅ Last 24 hours only (fresh contracts)
✅ Result contracts only
✅ Threshold: €15,000,000
✅ Stock ticker lookup enabled
            """
            self.bot.reply_to(message, text, parse_mode='Markdown')

        @self.bot.message_handler(commands=['status'])
        def send_status(message):
            status = "🟢 Running" if self.is_running else "🔴 Stopped"
            text = f"""
*Status:* {status}
*Notified:* {len(self.notified)} contracts
*Threshold:* €15,000,000
*Stock:* {"✅" if self.stock_lookup.enabled else "❌"}
            """
            self.bot.reply_to(message, text, parse_mode='Markdown')

        @self.bot.message_handler(commands=['scan'])
        def run_scan(message):
            self.bot.reply_to(message, "🔍 Scanning last 24 hours...")
            count = self._scan(days_back=1)
            self.bot.reply_to(message, f"✅ Scan complete! Sent {count} new alerts")
        
        @self.bot.message_handler(commands=['test'])
        def test_scan(message):
            """Test with €5M threshold"""
            self.bot.reply_to(message, "🧪 Testing with €5M threshold (24 hours)...")
            try:
                notices = self.collector.fetch_all_contracts(days_back=1)
                test_contracts = self.collector.filter_high_value_results(notices, min_value_eur=5_000_000)
                
                msg = f"Found {len(test_contracts)} contracts >= €5M\n\n"
                for c in test_contracts[:5]:
                    msg += f"• {c['publication_number']}: €{c['total_eur']:,.0f}\n"
                
                self.bot.reply_to(message, msg)
            except Exception as e:
                self.bot.reply_to(message, f"Error: {e}")
        
        @self.bot.message_handler(commands=['clear'])
        def clear_notified(message):
            """Clear notification history"""
            count = len(self.notified)
            self.notified.clear()
            self.bot.reply_to(message, f"🗑️ Cleared {count} notifications")
        
        @self.bot.message_handler(commands=['stock'])
        def lookup_stock(message):
            """Lookup stock"""
            parts = message.text.split(maxsplit=1)
            if len(parts) < 2:
                self.bot.reply_to(message, "Usage: /stock Company Name")
                return
            
            company = parts[1]
            ticker = self.stock_lookup.find_ticker(company)
            
            if ticker:
                stock_info = self.stock_lookup.get_stock_info(ticker)
                if stock_info:
                    emoji = "📈" if stock_info['change_5d'] > 0 else "📉"
                    msg = f"`{ticker}` {emoji}\n{stock_info['price']:.2f} {stock_info['currency']}\n5d: {stock_info['change_5d']:+.2f}%"
                    self.bot.reply_to(message, msg, parse_mode='Markdown')
                else:
                    self.bot.reply_to(message, f"Found `{ticker}` but no data", parse_mode='Markdown')
            else:
                self.bot.reply_to(message, f"No ticker found for: {company}")

        @self.bot.message_handler(commands=['stop'])
        def stop(message):
            self.is_running = False
            self.bot.reply_to(message, "⏸️ Stopped")

        @self.bot.message_handler(commands=['resume'])
        def resume(message):
            self.is_running = True
            self.bot.reply_to(message, "▶️ Resumed")

    def _scan(self, days_back: int = 1) -> int:
        """Run scan - ONLY LAST 24 HOURS"""
        try:
            logger.info("="*60)
            logger.info("🔍 STARTING TED SCAN (LAST 24 HOURS)")
            logger.info("="*60)

            notices = self.collector.fetch_all_contracts(days_back=days_back)
            
            if not notices:
                logger.warning("❌ No contracts fetched!")
                return 0

            high_value = self.collector.filter_high_value_results(notices)

            if not high_value:
                logger.info("ℹ️ No high-value contracts found")
                return 0

            new_count = 0
            for contract in high_value:
                pub_num = str(contract["publication_number"])

                # Check if already notified (keep for 1 day only)
                if pub_num in self.notified:
                    age = datetime.now() - self.notified[pub_num]
                    if age.total_seconds() < 86400:  # 24 hours
                        logger.info(f"Skipping {pub_num} (already notified)")
                        continue

                self._notify(contract)
                self.notified[pub_num] = datetime.now()
                new_count += 1

            logger.info(f"✅ COMPLETE: {new_count} new notifications sent")
            logger.info("="*60)
            return new_count

        except Exception as e:
            logger.error(f"Scan error: {e}", exc_info=True)
            self._send(f"❌ Scan error: {str(e)[:200]}")
            return 0

    def _notify(self, contract: dict):
        """Send notification - SHORTENED to avoid Telegram length limit"""
        try:
            # Shorten title if too long
            title = contract.get('title', 'N/A')
            if isinstance(title, dict):
                title = title.get('eng', title.get('swe', 'N/A'))
            if isinstance(title, list):
                title = title[0] if title else 'N/A'
            title_short = str(title)[:80] + "..." if len(str(title)) > 80 else str(title)

            # Shorten buyer name if too long
            buyer_name = str(contract.get('buyer_name', 'N/A'))[:60]
            
            # Build lots section - LIMIT to 3 lots max to avoid length issues
            lots_text = ""
            max_lots_to_show = 3
            lots_shown = 0
            
            for lot in contract["lots"][:max_lots_to_show]:
                winner = str(lot['winner_name'])[:50]  # Limit winner name length
                country = lot['winner_country']
                
                # Format value
                if lot['tender_currency'] == 'EUR':
                    value_text = f"€{lot['eur_value']:,.0f}"
                else:
                    value_text = f"€{lot['eur_value']:,.0f}"  # Removed conversion detail to save space
                
                lots_text += f"\n*{lot['lot_id']}* • {value_text}\n"
                lots_text += f"🏆 {winner}\n"
                
                # Stock lookup - ONLY if enabled
                if self.stock_lookup.enabled:
                    ticker = self.stock_lookup.find_ticker(winner, country)
                    if ticker:
                        stock_info = self.stock_lookup.get_stock_info(ticker)
                        if stock_info:
                            emoji = "📈" if stock_info['change_5d'] > 0 else "📉"
                            lots_text += f"📊 `{ticker}` {emoji} {stock_info['price']:.2f} ({stock_info['change_5d']:+.1f}%)\n"
                
                lots_shown += 1
            
            # Add "more lots" indicator if needed
            remaining_lots = len(contract["lots"]) - lots_shown
            if remaining_lots > 0:
                lots_text += f"\n_+ {remaining_lots} more lot(s)_\n"

            # Build message - COMPACT VERSION
            msg = f"""
🚨 *HIGH-VALUE CONTRACT*

*ID:* {contract['publication_number']}
*Date:* {contract['publication_date'][:10]}

💰 *TOTAL: €{contract['total_eur']:,.0f}*

*Buyer:* {buyer_name}
*Country:* {contract['buyer_country']}

*LOTS:*{lots_text}
🔗 [View Contract]({contract['url']})
            """

            # Check length and truncate if needed
            if len(msg) > 4000:  # Leave margin for safety
                # Ultra-compact version
                msg = f"""
🚨 *HIGH-VALUE CONTRACT*

*ID:* {contract['publication_number']}
💰 *€{contract['total_eur']:,.0f}*

*Buyer:* {buyer_name[:40]}
*Country:* {contract['buyer_country']}

*Lots:* {len(contract["lots"])}
🔗 [View]({contract['url']})
                """

            self._send(msg)
            logger.info(f"✅ Sent notification for {contract['publication_number']}")

        except Exception as e:
            logger.error(f"Notify error: {e}", exc_info=True)
            # Fallback: Send minimal notification
            try:
                minimal_msg = f"🚨 *CONTRACT ALERT*\n\nID: {contract.get('publication_number')}\nValue: €{contract.get('total_eur', 0):,.0f}\n\n[View]({contract.get('url', '#')})"
                self._send(minimal_msg)
            except:
                pass

    def _send(self, message: str):
        """Send message"""
        try:
            self.bot.send_message(self.chat_id, message, parse_mode='Markdown', disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"Send error: {e}")

    def _monitoring_loop(self):
        """Auto-scan every 10 minutes"""
        logger.info("🔄 Monitoring loop started")
        self._send("✅ *Monitor Started!*\n\nScanning every 10 minutes\n📅 Last 24 hours only\n📋 Result contracts\n💰 ≥€15M")

        while self.is_running:
            try:
                self._scan(days_back=1)  # Only last 24 hours
                
                if self.is_running:
                    logger.info("⏳ Waiting 10 minutes...")
                    time.sleep(600)

            except Exception as e:
                logger.error(f"Loop error: {e}", exc_info=True)
                time.sleep(600)

    def process_update(self, update):
        """Process webhook update"""
        try:
            self.bot.process_new_updates([update])
        except Exception as e:
            logger.error(f"Update error: {e}")


# Global bot
bot_instance = None


@app.route('/', methods=['GET'])
def home():
    return "<h1>TED Monitor Running</h1><p>Status: Active</p>", 200


@app.route('/health', methods=['GET'])
def health():
    status = "running" if bot_instance and bot_instance.is_running else "stopped"
    return {'status': 'ok', 'monitoring': status, 'timestamp': datetime.now().isoformat()}, 200


@app.route('/webhook', methods=['POST'])
def webhook():
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
    """Main"""
    global bot_instance

    BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8395744940:AAGmZVdj1l-QfZ4zqGP_9XOOvO9EbsnyWLw')
    CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '2133274440')
    TED_KEY = os.environ.get('TED_API_KEY', '0f0d8c2f68bb46bab7afa51c46053433')
    WEBHOOK_URL = os.environ.get('WEBHOOK_URL')

    logger.info("="*60)
    logger.info("🤖 TED TELEGRAM BOT STARTING")
    logger.info("="*60)

    bot_instance = TEDTelegramBot(bot_token=BOT_TOKEN, chat_id=CHAT_ID, ted_api_key=TED_KEY)

    # Set webhook
    if WEBHOOK_URL:
        try:
            endpoint = f"{WEBHOOK_URL}/webhook"
            bot_instance.bot.remove_webhook()
            time.sleep(1)
            bot_instance.bot.set_webhook(url=endpoint)
            logger.info(f"✅ Webhook: {endpoint}")
        except Exception as e:
            logger.error(f"Webhook error: {e}")

    # Start monitoring
    bot_instance.is_running = True
    Thread(target=bot_instance._monitoring_loop, daemon=True).start()

    # Run Flask
    port = int(os.environ.get('PORT', 10000))
    logger.info(f"🚀 Starting on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == "__main__":
    main()
