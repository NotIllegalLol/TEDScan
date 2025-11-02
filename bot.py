"""
TED Contract Data Collector - With Currency Conversion and Filtering
Collects contracts, converts to EUR, and filters high-value results
"""

import requests
import json
import logging
from datetime import datetime, timedelta
import time
from typing import List, Dict, Any, Tuple
from pathlib import Path
import re
import shutil

class TEDDataCollector:
    """Collect and save all TED contracts using official Search API v3"""
    
    def __init__(self, api_key: str = None, output_dir: str = r"E:\Anaconda3\envs\ted\Scripts\Results"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # API key for authentication
        self.api_key = api_key
        
        # Setup logging
        log_file = self.output_dir / f"ted_collector_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Official TED Search API v3 endpoint
        self.search_api_url = "https://api.ted.europa.eu/v3/notices/search"
        
        # Initialize currency converter
        try:
            from forex_python.converter import CurrencyRates
            self.currency_converter = CurrencyRates()
            self.logger.info("✓ Currency converter initialized (forex-python)")
        except:
            try:
                from currency_converter import CurrencyConverter
                self.currency_converter = CurrencyConverter()
                self.logger.info("✓ Currency converter initialized (CurrencyConverter)")
            except:
                self.currency_converter = None
                self.logger.warning("⚠ No currency converter available - will use fallback rates")
        
        self.logger.info("✓ TED Data Collector initialized")
        self.logger.info(f"✓ Using Official TED Search API v3")
        self.logger.info(f"✓ Saving to: {self.output_dir}")
        self.logger.info(f"✓ Log file: {log_file}")
    
    def convert_to_eur(self, amount: float, currency: str) -> float:
        """
        Convert amount to EUR with fallback rates
        
        Args:
            amount: Amount to convert
            currency: Currency code (e.g., 'SEK', 'USD', 'GBP')
            
        Returns:
            Amount in EUR
        """
        if currency == "EUR" or not currency:
            return amount
        
        # Fallback exchange rates (approximate, updated Nov 2024)
        fallback_rates = {
            'USD': 0.92, 'GBP': 1.17, 'CHF': 1.06, 
            'SEK': 0.088, 'DKK': 0.134, 'NOK': 0.086,
            'PLN': 0.23, 'CZK': 0.040, 'HUF': 0.0025,
            'RON': 0.20, 'BGN': 0.51, 'HRK': 0.13,
            'ISK': 0.0066, 'TRY': 0.027, 'RUB': 0.010
        }
        
        # Try using live rates first
        if self.currency_converter:
            try:
                if hasattr(self.currency_converter, 'get_rate'):
                    # forex_python style
                    rate = self.currency_converter.get_rate(currency, 'EUR')
                    return amount * rate
                elif hasattr(self.currency_converter, 'convert'):
                    # CurrencyConverter style
                    return self.currency_converter.convert(amount, currency, 'EUR')
            except Exception as e:
                self.logger.debug(f"Live conversion failed for {currency}: {e}")
        
        # Use fallback rates
        if currency in fallback_rates:
            return amount * fallback_rates[currency]
        
        # If unknown currency, log warning and return original
        self.logger.warning(f"Unknown currency: {currency}, cannot convert")
        return amount
    
    def fetch_all_contracts(self, days_back: int = 1, specific_date: str = None) -> List[Dict]:
        """Fetch all contracts from TED Search API"""
        try:
            if specific_date:
                specific_date = specific_date.replace("-", "")
                start_str = specific_date
                end_str = specific_date
                self.logger.info(f"\n→ Fetching contracts for specific date: {specific_date}...")
            else:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days_back)
                start_str = start_date.strftime("%Y%m%d")
                end_str = end_date.strftime("%Y%m%d")
                self.logger.info(f"\n→ Fetching contracts from {start_str} to {end_str}...")
            
            all_notices = []
            page = 1
            limit = 50
            
            while True:
                self.logger.info(f"  → Fetching page {page}...")
                
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
                    self.logger.error(f"⚠ API returned status {response.status_code}")
                    break
                
                data = response.json()
                notices = data.get("notices", [])
                total = data.get("total", 0)
                
                if not notices:
                    break
                
                all_notices.extend(notices)
                self.logger.info(f"  ✓ Got {len(notices)} notices (total: {len(all_notices)}/{total})")
                
                if len(all_notices) >= total or len(notices) < limit:
                    break
                
                page += 1
                time.sleep(0.5)
            
            self.logger.info(f"✓ Total contracts fetched: {len(all_notices)}")
            return all_notices
            
        except Exception as e:
            self.logger.error(f"⚠ Error fetching TED data: {e}", exc_info=True)
            return []
    
    def match_winners_to_lots(self, notice: Dict) -> List[Tuple[Dict, str, str]]:
        """Match winners to lots with currency info"""
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
    
    def save_contract(self, notice: Dict, index: int):
        """Save individual contract to text file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            pub_number = notice.get("publication-number")
            if pub_number:
                if isinstance(pub_number, list) and pub_number:
                    notice_id = pub_number[0]
                else:
                    notice_id = pub_number
            else:
                notice_id = f"unknown_{index}"
            
            notice_id = str(notice_id).replace("/", "_").replace("\\", "_")
            filename = f"contract_{timestamp}_{index:04d}_{notice_id}.txt"
            filepath = self.output_dir / filename
            
            content = self._format_contract(notice, index)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return filepath
            
        except Exception as e:
            self.logger.error(f"⚠ Error saving contract {index}: {e}", exc_info=True)
            return None
    
    def _format_contract(self, notice: Dict, index: int) -> str:
        """Format contract data as readable text"""
        separator = "=" * 80
        
        def extract_value(data, default="N/A"):
            if data is None:
                return default
            if isinstance(data, list):
                if not data:
                    return default
                return ", ".join(str(x) for x in data if x)
            return str(data)
        
        pub_number = extract_value(notice.get("publication-number"))
        pub_date = extract_value(notice.get("publication-date"))
        notice_type = extract_value(notice.get("notice-type"))
        form_type = extract_value(notice.get("form-type"))
        buyer_name = extract_value(notice.get("buyer-name"))
        buyer_country = extract_value(notice.get("buyer-country"))
        buyer_city = extract_value(notice.get("buyer-city"))
        title = extract_value(notice.get("notice-title"))
        contract_nature = extract_value(notice.get("contract-nature"))
        url = extract_value(notice.get("announcement-url"))
        
        lots = self.match_winners_to_lots(notice)
        
        lot_section = ""
        if lots:
            lot_section = f"""
LOT INFORMATION:
{"-" * 80}
Total Lots: {len(lots)}
"""
            for i, (lot, est_cur, tender_cur) in enumerate(lots, 1):
                est_val = lot.get('estimated_value', 'N/A')
                tender_val = lot.get('tender_value', 'N/A')
                
                if est_val != 'N/A' and est_val is not None:
                    try:
                        est_val = f"{float(est_val):,.2f} {est_cur}"
                    except:
                        est_val = f"{est_val} {est_cur}"
                
                if tender_val != 'N/A' and tender_val is not None:
                    try:
                        tender_val = f"{float(tender_val):,.2f} {tender_cur}"
                    except:
                        tender_val = f"{tender_val} {tender_cur}"
                
                lot_section += f"""
LOT #{i}: {lot['lot_id']}
  
  VALUES:
    Estimated Value:  {est_val}
    Tender Value:     {tender_val}
  
  WINNER/CONTRACTOR:
    Name:             {lot['winner_name']}
    Country:          {lot['winner_country']}
    City:             {lot['winner_city']}
"""
        else:
            lot_section = f"""
LOT INFORMATION:
{"-" * 80}
No lot information available.
"""
        
        content = f"""{separator}
CONTRACT NOTICE #{index} - TED DATABASE
{separator}

BASIC INFORMATION:
{"-" * 80}
Publication Number:      {pub_number}
Publication Date:        {pub_date}
Notice Type:             {notice_type}
Form Type:               {form_type}

BUYER/CONTRACTING AUTHORITY:
{"-" * 80}
Buyer Name:              {buyer_name}
Buyer Country:           {buyer_country}
Buyer City:              {buyer_city}

CONTRACT DETAILS:
{"-" * 80}
Title:                   {title}
Contract Nature:         {contract_nature}
Notice URL:              {url}
{lot_section}
{separator}
RAW JSON DATA:
{separator}
{json.dumps(notice, indent=2, ensure_ascii=False)}

{separator}
"""
        return content
    
    def analyze_and_convert_currencies(self):
        """
        Step 2: Analyze all saved contracts, convert Tender Values to EUR
        """
        self.logger.info("\n" + "="*80)
        self.logger.info("💱 STEP 2: ANALYZING AND CONVERTING CURRENCIES")
        self.logger.info("="*80)
        
        contract_files = list(self.output_dir.glob("contract_*.txt"))
        self.logger.info(f"Found {len(contract_files)} contracts to analyze...")
        
        processed_count = 0
        
        for filepath in contract_files:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Add EUR conversions to the file
                updated_content = self._add_eur_conversions(content)
                
                # Save updated file
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(updated_content)
                
                processed_count += 1
                
            except Exception as e:
                self.logger.warning(f"Could not process {filepath.name}: {e}")
        
        self.logger.info(f"✓ Processed and updated {processed_count} contracts with EUR values")
    
    def _add_eur_conversions(self, content: str) -> str:
        """Add EUR Value conversions to contract content"""
        
        # Find LOT INFORMATION section
        lot_section_match = re.search(r'LOT INFORMATION:.*?(?=RAW JSON DATA|$)', content, re.DOTALL)
        
        if not lot_section_match:
            return content
        
        lot_section = lot_section_match.group(0)
        updated_lot_section = lot_section
        
        # Find all LOT blocks
        lot_blocks = re.findall(r'(LOT #\d+:.*?)(?=LOT #|\Z)', lot_section, re.DOTALL)
        
        for lot_block in lot_blocks:
            # Extract Tender Value with currency
            tender_match = re.search(r'Tender Value:\s*([\d,]+\.?\d*)\s+([A-Z]{3})', lot_block)
            
            if tender_match:
                value_str = tender_match.group(1).replace(',', '')
                currency = tender_match.group(2)
                
                try:
                    value = float(value_str)
                    
                    # Convert to EUR
                    if currency == "EUR":
                        eur_value = value
                        eur_line = f"    EUR Value:        {eur_value:,.2f} EUR (no conversion needed)"
                    else:
                        eur_value = self.convert_to_eur(value, currency)
                        eur_line = f"    EUR Value:        {eur_value:,.2f} EUR (converted from {currency})"
                    
                    # Insert EUR Value line after Tender Value
                    old_block = lot_block
                    new_block = re.sub(
                        r'(Tender Value:\s*[\d,]+\.?\d*\s+[A-Z]{3})',
                        r'\1\n' + eur_line,
                        lot_block
                    )
                    
                    updated_lot_section = updated_lot_section.replace(old_block, new_block)
                    
                except ValueError:
                    pass
        
        # Replace the old LOT section with updated one
        updated_content = content.replace(lot_section, updated_lot_section)
        
        return updated_content
    
    def filter_high_value_results(self, min_value_eur: float = 15_000_000):
        """
        Step 3: Filter for Form Type: result AND EUR Value >= 15M
        Create slim versions with only essential info
        """
        self.logger.info("\n" + "="*80)
        self.logger.info("🔍 STEP 3: FILTERING HIGH-VALUE RESULTS")
        self.logger.info("="*80)
        self.logger.info(f"Filter criteria:")
        self.logger.info(f"  • Form Type: must be 'result'")
        self.logger.info(f"  • EUR Value: >= €{min_value_eur:,.0f}")
        
        # Create filtered folder
        filtered_dir = self.output_dir / "high_value_results"
        filtered_dir.mkdir(exist_ok=True)
        
        contract_files = list(self.output_dir.glob("contract_*.txt"))
        high_value_count = 0
        
        for filepath in contract_files:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Check Form Type
                form_type_match = re.search(r'Form Type:\s+(.+)', content)
                if not form_type_match:
                    continue
                
                form_type = form_type_match.group(1).strip()
                
                # Must be 'result'
                if 'result' not in form_type.lower():
                    continue
                
                # Extract all EUR Values from the contract
                eur_values = re.findall(r'EUR Value:\s+([\d,]+\.?\d*)\s+EUR', content)
                
                if not eur_values:
                    continue
                
                # Calculate total EUR value
                total_eur = sum(float(val.replace(',', '')) for val in eur_values)
                
                # Check if meets threshold
                if total_eur >= min_value_eur:
                    # Extract essential information only
                    pub_number = re.search(r'Publication Number:\s+(.+)', content)
                    pub_date = re.search(r'Publication Date:\s+(.+)', content)
                    
                    pub_number_str = pub_number.group(1).strip() if pub_number else "N/A"
                    pub_date_str = pub_date.group(1).strip() if pub_date else "N/A"
                    
                    # Extract LOT information with EUR Values and Winners
                    lot_section = re.search(r'LOT INFORMATION:.*?(?=RAW JSON DATA|$)', content, re.DOTALL)
                    
                    slim_content = f"""{"="*80}
*** HIGH VALUE RESULT CONTRACT ***
*** TOTAL EUR VALUE: €{total_eur:,.2f} ***
{"="*80}

ESSENTIAL INFORMATION:
--------------------------------------------------------------------------------
Publication Number:      {pub_number_str}
Publication Date:        {pub_date_str}
Form Type:               {form_type}

LOT VALUES (EUR):
--------------------------------------------------------------------------------
"""
                    
                    # Extract each lot with EUR value and winner
                    if lot_section:
                        # Find all lot blocks with their complete info
                        lot_blocks = re.findall(
                            r'(LOT #\d+: [^\n]+).*?EUR Value:\s+([\d,]+\.?\d*)\s+EUR.*?Name:\s+([^\n]+)',
                            lot_section.group(0), 
                            re.DOTALL
                        )
                        
                        for lot_header, eur_val, winner_name in lot_blocks:
                            winner_name = winner_name.strip()
                            slim_content += f"{lot_header}\n"
                            slim_content += f"  Winner: {winner_name}\n"
                            slim_content += f"  EUR Value: €{eur_val}\n\n"
                    
                    slim_content += f"""{"="*80}
TOTAL CONTRACT VALUE: €{total_eur:,.2f}
{"="*80}
"""
                    
                    # Save slim version
                    dest_path = filtered_dir / filepath.name
                    with open(dest_path, 'w', encoding='utf-8') as f:
                        f.write(slim_content)
                    
                    high_value_count += 1
                    self.logger.info(f"  ✓ {filepath.name}: €{total_eur:,.2f}")
                    
            except Exception as e:
                self.logger.warning(f"Could not process {filepath.name}: {e}")
        
        self.logger.info(f"\n✓ Found {high_value_count} high-value result contracts")
        self.logger.info(f"📁 Saved to: {filtered_dir}")
        
        return high_value_count
    
    def collect_and_save_all(self, days_back: int = 1, specific_date: str = None):
        """Main collection process with 3-step filtering"""
        
        self.logger.info("\n" + "="*80)
        self.logger.info("🔍 TED CONTRACT DATA COLLECTION - 3 STEP PROCESS")
        self.logger.info("="*80)
        if specific_date:
            self.logger.info(f"📅 Specific Date: {specific_date}")
        else:
            self.logger.info(f"📅 Timeframe: Last {days_back} day(s)")
        self.logger.info(f"💾 Output Directory: {self.output_dir}")
        self.logger.info("="*80)
        
        # STEP 1: Fetch and save all contracts
        self.logger.info("\n📥 STEP 1: FETCHING ALL CONTRACTS")
        notices = self.fetch_all_contracts(days_back=days_back, specific_date=specific_date)
        
        if not notices:
            self.logger.warning("\n❌ No contracts found!")
            return
        
        self.logger.info(f"\n→ Saving all {len(notices)} contracts...")
        saved_count = 0
        for idx, notice in enumerate(notices, 1):
            filepath = self.save_contract(notice, idx)
            if filepath:
                saved_count += 1
                if saved_count % 50 == 0:
                    self.logger.info(f"  ✓ Saved {saved_count}/{len(notices)}...")
        
        self.logger.info(f"✓ Saved {saved_count} contracts")
        
        # STEP 2: Convert all currencies to EUR
        self.analyze_and_convert_currencies()
        
        # STEP 3: Filter for high-value results
        high_value_count = self.filter_high_value_results(min_value_eur=15_000_000)
        
        # Final summary
        self.logger.info("\n" + "="*80)
        self.logger.info("✅ COLLECTION COMPLETE")
        self.logger.info("="*80)
        self.logger.info(f"📊 Total contracts fetched: {len(notices)}")
        self.logger.info(f"📊 Contracts saved: {saved_count}")
        self.logger.info(f"💰 High-value results (>€15M): {high_value_count}")
        self.logger.info(f"📁 All contracts: {self.output_dir}")
        self.logger.info(f"📁 High-value results: {self.output_dir / 'high_value_results'}")
        self.logger.info("="*80)


def main():
    """Main execution"""
    
    import os
    api_key = os.environ.get('TED_API_KEY', '0f0d8c2f68bb46bab7afa51c46053433')
    
    collector = TEDDataCollector(
        api_key=api_key,
        output_dir=r"E:\Anaconda3\envs\ted\Scripts\Results"
    )
    
    try:
        # 3-STEP PROCESS:
        # 1. Fetch all contracts
        # 2. Convert Tender Values to EUR (adds "EUR Value:" to each contract)
        # 3. Filter for Form Type: result AND EUR Value >= €15M
        
        collector.collect_and_save_all(days_back=1)
        
    except KeyboardInterrupt:
        collector.logger.info("\n\n✓ Collection stopped by user")
    except Exception as e:
        collector.logger.error(f"\n❌ Error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
