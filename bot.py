"""
TED Telegram Bot - Cloud Deployment Version
Monitors TED contracts and sends Telegram notifications
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TEDCollector:
    """Simplified TED collector for cloud deployment"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.search_api_url = "https://api.ted.europa.eu/v3/notices/search"
        
        # Fallback exchange rates (Nov 2024)
        self.rates = {
            'USD': 0.92, 'GBP': 1.17, 'CHF': 1.06, 
            'SEK': 0.088, 'DKK': 0.134, 'NOK': 0.086,
            'PLN': 0.23, 'CZK': 0.040, 'HUF': 0.0025,
            'RON': 0.20, 'BGN': 0.51, 'HRK': 0.13,
        }
    
    def convert_to_eur(self, amount: float, currency: str) -> float:
        """Convert to EUR"""
        if currency == "EUR" or not currency:
            return amount
        return amount * self.rates.get(currency, 1.0)
    
    def fetch_contracts(self, days_back: int = 1):
        """Fetch contracts from TED API"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            start_str = start_date.strftime("%Y%m%d")
            end_str = end_date.strftime("%Y%m%d")
            
            logger.info(f"Fetching TED contracts from {start_str} to {end_str}")
            
            all_notices = []
            page = 1
            
            while page <= 5:  # Limit to 5 pages for cloud efficiency
                query = f"PD>={start_str} AND PD<={end_str}"
                
                request_body = {
                    "query": query,
                    "fields": [
                        "publication-number", "publication-date", "form-type",
                        "buyer-name", "buyer-country", "notice-title",
                        "announcement-url", "identifier-lot", "winner-name",
                        "winner-country", "tender-value", "tender-value-cur"
                    ],
                    "page": page,
                    "limit": 100,
                    "scope": "ALL",
                    "paginationMode": "PAGE_NUMBER"
                }
                
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "apikey": self.api_key
                }
                
                response = requests.post(
                    self.search_api_url, 
                    json=request_body, 
                    headers=headers, 
                    timeout=30
                )
                
                if response.status_code != 200:
                    logger.error(f"API error: {response.status_code}")
                    break
                
                data = response.json()
                notices = data.get("notices", [])
                
                if not notices:
                    break
                
                all_notices.extend(notices)
                logger.info(f"Page {page}: Got {len(notices)} notices")
                
                if len(notices) < 100:
                    break
                
                page += 1
                time.sleep(0.5)
            
            logger.info(f"Total fetched: {len(all_notices)} contracts")
            return all_notices
            
        except Exception as e:
            logger.error(f"Error fetching: {e}", exc_info=True)
            return []
    
    def find_high_value_contracts(self, notices, min_eur: float = 15_000_000):
        """Find contracts >= 15M EUR"""
        high_value = []
        
        for notice in notices:
            try:
                form_type = str(notice.get("form-type", ""))
                if 'result' not in form_type.lower():
                    continue
                
                tender_values = notice.get("tender-value", [])
                tender_currencies = notice.get("tender-value-cur", [])
                
                if not tender_values:
                    continue
                
                # Ensure lists
                if not isinstance(tender_values, list):
                    tender_values = [tender_values]
                if not isinstance(tender_currencies, list):
                    tender_currencies = [tender_currencies]
                
                # Calculate total EUR
                total_eur = 0
                lots = []
                
                for i, val in enumerate(tender_values):
                    if val is None:
                        continue
                    
                    try:
                        value = float(val)
                        currency = tender_currencies[i] if i < len(tender_currencies) else "EUR"
                        eur_val = self.convert_to_eur(value, currency)
                        total_eur += eur_val
                        
                        # Get lot info
                        lot_ids = notice.get("identifier-lot", [])
                        winners = notice.get("winner-name", [])
                        
                        if not isinstance(lot_ids, list):
                            lot_ids = [lot_ids]
                        if not isinstance(winners, list):
                            winners = [winners]
                        
                        lot_id = lot_ids[i] if i < len(lot_ids) else f"LOT-{i+1}"
                        winner = winners[i] if i < len(winners) else "N/A"
                        
                        lots.append({
                            "lot_id": lot_id,
                            "winner": winner,
                            "eur_value": eur_val,
                            "original_value": value,
                            "currency": currency
                        })
                    except:
                        continue
                
                # Check threshold
                if total_eur >= min_eur:
                    high_value.append({
                        "pub_number": notice.get("publication-number", "N/A"),
                        "pub_date": notice.get("publication-date", "N/A"),
                        "form_type": form_type,
                        "buyer": notice.get("buyer-name", "N/A"),
                        "buyer_country": notice.get("buyer-country", "N/A"),
                        "title": notice.get("notice-title", "N/A"),
                        "url": notice.get("announcement-url", "N/A"),
                        "total_eur": total_eur,
                        "lots": lots
                    })
                    
            except Exception as e:
                logger.warning(f"Error analyzing notice: {e}")
                continue
        
        logger.info(f"Found {len(high_value)} high-value contracts")
        return high_value


class TEDBot:
    """Telegram bot for TED monitoring"""
    
    def __init__(self, bot_token: str, chat_id: str, ted_api_key: str):
        self.bot = telebot.TeleBot(bot_token)
        self.chat_id = chat_id
        self.collector = TEDCollector(ted_api_key)
        self.is_running = False
        self.notified = set()
        
        self._setup_handlers()
    
    def _setup_handlers(self):
        
        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            text = """
🤖 *TED Contract Monitor Bot*

Monitors TED for high-value contracts ≥€15M

*Commands:*
/start - Show this message
/status - Check bot status
/scan - Scan now
/stop - Stop auto-scan
/resume - Resume auto-scan

*Status:* Bot is running on Render.com ☁️
Scans every 10 minutes automatically.
            """
            self.bot.reply_to(message, text, parse_mode='Markdown')
        
        @self.bot.message_handler(commands=['status'])
        def send_status(message):
            status = "🟢 Running" if self.is_running else "🔴 Stopped"
            text = f"""
*Status:* {status}
*Interval:* 10 minutes
*Notified:* {len(self.notified)} contracts
*Threshold:* €15,000,000
            """
            self.bot.reply_to(message, text, parse_mode='Markdown')
        
        @self.bot.message_handler(commands=['scan'])
        def run_scan(message):
            self.bot.reply_to(message, "🔍 Scanning TED...")
            count = self._scan()
            if count == 0:
                self.bot.reply_to(message, "✅ Scan complete. No new high-value contracts.")
            else:
                self.bot.reply_to(message, f"✅ Found {count} new contracts!")
        
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
        """Run scan and return count of new contracts found"""
        try:
            logger.info("Starting scan...")
            notices = self.collector.fetch_contracts(days_back=1)
            contracts = self.collector.find_high_value_contracts(notices)
            
            new_count = 0
            for contract in contracts:
                pub_num = str(contract["pub_number"])
                
                if pub_num in self.notified:
                    continue
                
                self._notify(contract)
                self.notified.add(pub_num)
                new_count += 1
            
            logger.info(f"Scan complete. {new_count} new contracts")
            return new_count
            
        except Exception as e:
            logger.error(f"Scan error: {e}", exc_info=True)
            self._send(f"❌ Error: {str(e)}")
            return 0
    
    def _notify(self, contract: dict):
        """Send notification about contract"""
        try:
            lots_text = ""
            for lot in contract["lots"]:
                lots_text += f"\n• {lot['lot_id']}\n"
                lots_text += f"  Winner: {lot['winner']}\n"
                lots_text += f"  Value: €{lot['eur_value']:,.0f}"
                if lot['currency'] != 'EUR':
                    lots_text += f" (from {lot['original_value']:,.0f} {lot['currency']})"
                lots_text += "\n"
            
            # Truncate title if too long
            title = str(contract['title'])
            if len(title) > 200:
                title = title[:200] + "..."
            
            msg = f"""
🚨 *HIGH-VALUE CONTRACT!* 🚨

*Publication:* {contract['pub_number']}
*Date:* {contract['pub_date']}
*Total:* €{contract['total_eur']:,.0f}

*Buyer:*
{contract['buyer']} ({contract['buyer_country']})

*Title:*
{title}

*Lots:*{lots_text}

[View Contract]({contract['url']})

_Found: {datetime.now().strftime('%Y-%m-%d %H:%M')}_
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
                self.bot.send_message(self.chat_id, message)
            except:
                pass
    
    def _monitoring_loop(self):
        """Auto-scan loop"""
        logger.info("Monitoring started")
        self._send("✅ TED monitoring started!\nScanning every 10 minutes for contracts ≥€15M")
        
        while self.is_running:
            try:
                self._scan()
                
                if self.is_running:
                    logger.info("Waiting 10 minutes...")
                    time.sleep(600)  # 10 minutes
                    
            except Exception as e:
                logger.error(f"Loop error: {e}", exc_info=True)
                time.sleep(600)
    
    def start(self):
        """Start bot"""
        logger.info("Starting bot...")
        
        # Auto-start monitoring
        self.is_running = True
        Thread(target=self._monitoring_loop, daemon=True).start()
        
        logger.info("Bot running. Polling for messages...")
        self.bot.infinity_polling()


def main():
    """Main entry point"""
    
    # Get from environment variables
    BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
    CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
    TED_KEY = os.environ.get('TED_API_KEY')
    
    if not BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN not set!")
    if not CHAT_ID:
        raise ValueError("TELEGRAM_CHAT_ID not set!")
    if not TED_KEY:
        raise ValueError("TED_API_KEY not set!")
    
    logger.info(f"Starting bot for chat ID: {CHAT_ID}")
    
    bot = TEDBot(
        bot_token=BOT_TOKEN,
        chat_id=CHAT_ID,
        ted_api_key=TED_KEY
    )
    
    bot.start()


if __name__ == "__main__":
    main()
