"""
TED Telegram Bot - Render.com Compatible Version
Complete 3-step processing: Fetch, Convert, Filter
"""

import os
import time
import logging
from datetime import datetime, timedelta
from threading import Thread
import telebot
import requests
import json
import re
from typing import List, Dict, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TEDDataCollector:
    """Complete TED collector with all original Scan.py logic"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key
        self.search_api_url = "https://api.ted.europa.eu/v3/notices/search"
        self.logger = logging.getLogger(__name__)
        
        # Fallback exchange rates (Nov 2024)
        self.fallback_rates = {
            'USD': 0.92, 'GBP': 1.17, 'CHF': 1.06, 
            'SEK': 0.088, 'DKK': 0.134, 'NOK': 0.086,
            'PLN': 0.23, 'CZK': 0.040, 'HUF': 0.0025,
            'RON': 0.20, 'BGN': 0.51, 'HRK': 0.13,
            'ISK': 0.0066, 'TRY': 0.027, 'RUB': 0.010
        }
    
    def convert_to_eur(self, amount: float, currency: str) -> float:
        """Convert amount to EUR with fallback rates"""
        if currency == "EUR" or not currency:
            return amount
        
        if currency in self.fallback_rates:
            return amount * self.fallback_rates[currency]
        
        self.logger.warning(f"Unknown currency: {currency}, cannot convert")
        return amount
    
    def fetch_all_contracts(self, days_back: int = 1) -> List[Dict]:
        """Fetch all contracts from TED Search API - ORIGINAL LOGIC"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            start_str = start_date.strftime("%Y%m%d")
            end_str = end_date.strftime("%Y%m%d")
            
            self.logger.info(f"Fetching contracts from {start_str} to {end_str}")
            
            all_notices = []
            page = 1
            limit = 50
            
            while True:
                self.logger.info(f"Fetching page {page}...")
                
                if start_str == end_str:
                    query = f"PD={start_str}"
                else:
                    query = f"PD>={start_str} AND PD<={end_str}"
                
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
                
                response = requests.post(self.search_api_url, json=request_body, headers=headers, timeout=30)
                
                if response.status_code != 200:
                    self.logger.error(f"API returned status {response.status_code}: {response.text}")
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
            
            self.logger.info(f"Total contracts fetched: {len(all_notices)}")
            return all_notices
            
        except Exception as e:
            self.logger.error(f"Error fetching TED data: {e}", exc_info=True)
            return []
    
    def match_winners_to_lots(self, notice: Dict) -> List[Tuple[Dict, str, str]]:
        """Match winners to lots with currency info - ORIGINAL LOGIC"""
        lots = []
        
        def to_list(data):
            if data is None:
                return []
            if isinstance(data, list):
                return data
            return [data]
        
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
            
            if len(lot_estimated_currencies) == 1:
                est_currency = lot_estimated_currencies[0]
            elif i < len(lot_estimated_currencies):
                est_currency = lot_estimated_currencies[i]
            else:
                est_currency = "EUR"
            
            if len(tender_currencies) == 1:
                tender_currency = tender_currencies[0]
            elif i < len(tender_currencies):
                tender_currency = tender_currencies[i]
            else:
                tender_currency = "EUR"
            
            if len(winner_names) == num_lots:
                lot_data["winner_name"] = winner_names[i] if i < len(winner_names) else "N/A"
            elif len(winner_names) == 1:
                lot_data["winner_name"] = winner_names[0]
            else:
                lot_data["winner_name"] = "N/A"
            
            if len(winner_countries) == num_lots:
                lot_data["winner_country"] = winner_countries[i] if i < len(winner_countries) else "N/A"
            elif len(winner_countries) == 1:
                lot_data["winner_country"] = winner_countries[0]
            else:
                lot_data["winner_country"] = "N/A"
            
            if len(winner_cities) == num_lots:
                lot_data["winner_city"] = winner_cities[i] if i < len(winner_cities) else "N/A"
            elif len(winner_cities) == 1:
                lot_data["winner_city"] = winner_cities[0]
            else:
                lot_data["winner_city"] = "N/A"
            
            lots.append((lot_data, est_currency, tender_currency))
        
        return lots
    
    def filter_high_value_results(self, notices: List[Dict], min_value_eur: float = 15_000_000) -> List[Dict]:
        """
        Filter for Form Type: result AND EUR Value >= 15M
        COMPLETE 3-STEP LOGIC: Fetch → Convert → Filter
        """
        self.logger.info("STEP 3: FILTERING HIGH-VALUE RESULTS")
        self.logger.info(f"Filter: Form Type='result' AND EUR Value >= €{min_value_eur:,.0f}")
        
        high_value_contracts = []
        
        for notice in notices:
            try:
                # Check Form Type
                form_type = notice.get("form-type", "")
                if not form_type or 'result' not in str(form_type).lower():
                    continue
                
                # Get lots with currency conversion
                lots = self.match_winners_to_lots(notice)
                
                if not lots:
                    continue
                
                # Calculate total EUR value
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
                
                # Check if meets threshold
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
                    
                    self.logger.info(f"Found: {notice.get('publication-number')} - €{total_eur:,.0f}")
                    
            except Exception as e:
                self.logger.warning(f"Error processing notice: {e}")
                continue
        
        self.logger.info(f"Found {len(high_value_contracts)} high-value contracts")
        return high_value_contracts


class TEDTelegramBot:
    """Telegram bot with full Scan.py logic"""
    
    def __init__(self, bot_token: str, chat_id: str, ted_api_key: str):
        self.bot = telebot.TeleBot(bot_token)
        self.chat_id = chat_id
        self.collector = TEDDataCollector(api_key=ted_api_key)
        self.is_running = False
        self.notified = set()
        
        self._setup_handlers()
    
    def _setup_handlers(self):
        
        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            text = """
🤖 *TED Contract Monitor Bot*

Full 3-step scanning with currency conversion!

*Commands:*
/start - Show this message
/status - Check bot status
/scan - Run full 3-step scan now
/stop - Stop auto-scan
/resume - Resume auto-scan

*3-Step Process:*
1️⃣ Fetch all contracts from TED
2️⃣ Convert currencies to EUR
3️⃣ Filter results ≥€15M

Running on Render.com ☁️
Scans every 10 minutes.
            """
            self.bot.reply_to(message, text, parse_mode='Markdown')
        
        @self.bot.message_handler(commands=['status'])
        def send_status(message):
            status = "🟢 Running" if self.is_running else "🔴 Stopped"
            text = f"""
*Status:* {status}
*Scan Interval:* 10 minutes
*Notified Contracts:* {len(self.notified)}
*Threshold:* €15,000,000
*Logic:* Full 3-step processing ✔
            """
            self.bot.reply_to(message, text, parse_mode='Markdown')
        
        @self.bot.message_handler(commands=['scan'])
        def run_scan(message):
            self.bot.reply_to(message, "🔍 Starting full 3-step scan...\n1️⃣ Fetching contracts\n2️⃣ Converting currencies\n3️⃣ Filtering high-value results")
            count = self._scan()
            if count == 0:
                self.bot.reply_to(message, "✅ Scan complete. No new high-value contracts found.")
            else:
                self.bot.reply_to(message, f"✅ Found {count} new high-value contracts!")
        
        @self.bot.message_handler(commands=['stop'])
        def stop(message):
            self.is_running = False
            self.bot.reply_to(message, "⏸️ Auto-scan stopped. Use /resume to restart.")
        
        @self.bot.message_handler(commands=['resume'])
        def resume(message):
            if not self.is_running:
                self.is_running = True
                Thread(target=self._monitoring_loop, daemon=True).start()
                self.bot.reply_to(message, "▶️ Auto-scan resumed!")
            else:
                self.bot.reply_to(message, "ℹ️ Already running.")
    
    def _scan(self) -> int:
        """Run full 3-step scan"""
        try:
            logger.info("="*60)
            logger.info("STARTING 3-STEP TED SCAN")
            logger.info("="*60)
            
            # STEP 1: Fetch all contracts
            logger.info("STEP 1: FETCHING ALL CONTRACTS")
            notices = self.collector.fetch_all_contracts(days_back=1)
            
            if not notices:
                logger.info("No contracts found")
                return 0
            
            # STEP 2 & 3: Convert currencies and filter (combined in filter_high_value_results)
            logger.info("STEP 2: CONVERTING CURRENCIES TO EUR")
            high_value_contracts = self.collector.filter_high_value_results(notices)
            
            # Send notifications for new contracts
            new_count = 0
            for contract in high_value_contracts:
                pub_num = str(contract["publication_number"])
                
                if pub_num in self.notified:
                    continue
                
                self._notify(contract)
                self.notified.add(pub_num)
                new_count += 1
            
            logger.info(f"SCAN COMPLETE: {new_count} new contracts notified")
            logger.info("="*60)
            return new_count
            
        except Exception as e:
            logger.error(f"Scan error: {e}", exc_info=True)
            self._send(f"❌ Scan error: {str(e)}")
            return 0
    
    def _notify(self, contract: dict):
        """Send detailed notification about contract"""
        try:
            # Build lots information
            lots_text = ""
            for lot in contract["lots"]:
                lots_text += f"\n*Lot:* {lot['lot_id']}\n"
                lots_text += f"  Winner: {lot['winner_name']}\n"
                lots_text += f"  Country: {lot['winner_country']}\n"
                
                if lot['tender_currency'] == 'EUR':
                    lots_text += f"  Value: €{lot['eur_value']:,.0f}\n"
                else:
                    lots_text += f"  Value: €{lot['eur_value']:,.0f} (converted from {lot['tender_value']:,.0f} {lot['tender_currency']})\n"
            
            # Truncate title if too long
            title = str(contract['title'])
            if len(title) > 150:
                title = title[:150] + "..."
            
            msg = f"""
🚨 *HIGH-VALUE CONTRACT FOUND!* 🚨

*Publication Number:* {contract['publication_number']}
*Publication Date:* {contract['publication_date']}
*Form Type:* {contract['form_type']}

*TOTAL VALUE: €{contract['total_eur']:,.0f}*

*Buyer Information:*
• Name: {contract['buyer_name']}
• Country: {contract['buyer_country']}
• City: {contract['buyer_city']}

*Contract Title:*
{title}

*Lots & Winners:*{lots_text}

🔗 [View Full Contract]({contract['url']})

_Detected: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_
            """
            
            self._send(msg)
            
        except Exception as e:
            logger.error(f"Notify error: {e}")
    
    def _send(self, message: str):
        """Send message to Telegram"""
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
                # Fallback without markdown
                self.bot.send_message(self.chat_id, message)
            except:
                pass
    
    def _monitoring_loop(self):
        """Auto-scan every 10 minutes"""
        logger.info("Monitoring loop started")
        self._send("✅ *TED Monitoring Started!*\n\nFull 3-step scanning:\n1️⃣ Fetch contracts\n2️⃣ Convert to EUR\n3️⃣ Filter ≥€15M\n\nScanning every 10 minutes.")
        
        while self.is_running:
            try:
                self._scan()
                
                if self.is_running:
                    logger.info("Waiting 10 minutes until next scan...")
                    time.sleep(600)  # 10 minutes
                    
            except Exception as e:
                logger.error(f"Loop error: {e}", exc_info=True)
                time.sleep(600)
    
    def start(self):
        """Start the bot"""
        logger.info("Starting Telegram bot with full Scan.py logic...")
        
        # Auto-start monitoring
        self.is_running = True
        Thread(target=self._monitoring_loop, daemon=True).start()
        
        logger.info("Bot running. Polling for messages...")
        self.bot.infinity_polling()


def main():
    """Main entry point - WITH FALLBACK TO HARDCODED VALUES"""
    
    # Try to read from environment variables first, fallback to hardcoded
    BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8395744940:AAGmZVdj1l-QfZ4zqGP_9XOOvO9EbsnyWLw')
    CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '2133274440')
    TED_KEY = os.environ.get('TED_API_KEY', '0f0d8c2f68bb46bab7afa51c46053433')
    
    # Log which source we're using
    if os.environ.get('TELEGRAM_BOT_TOKEN'):
        logger.info("Using credentials from environment variables ✓")
    else:
        logger.warning("Using hardcoded credentials (set environment variables for better security)")
    
    logger.info(f"Starting TED Bot for chat: {CHAT_ID}")
    logger.info(f"Bot token: {BOT_TOKEN[:20]}... (truncated)")
    logger.info(f"TED API key: {TED_KEY[:10]}... (truncated)")
    
    bot = TEDTelegramBot(
        bot_token=BOT_TOKEN,
        chat_id=CHAT_ID,
        ted_api_key=TED_KEY
    )
    
    bot.start()


if __name__ == "__main__":
    main()
