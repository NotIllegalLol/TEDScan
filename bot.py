"""
TED Contract Data Collector - Using ONLY Official API Parameters
Collects contracts from last 24 hours and saves each as individual file
"""

import requests
import json
import logging
from datetime import datetime, timedelta
import time
from typing import List, Dict, Any
from pathlib import Path

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
        
        self.logger.info("✓ TED Data Collector initialized")
        self.logger.info(f"✓ Using Official TED Search API v3")
        self.logger.info(f"✓ Saving to: {self.output_dir}")
        self.logger.info(f"✓ Log file: {log_file}")
    
    def fetch_all_contracts(self, days_back: int = 1, specific_date: str = None) -> List[Dict]:
        """
        Fetch all contracts from TED Search API using expert query
        
        Args:
            days_back: Number of days to look back (default: 1)
            specific_date: Specific date in format "YYYYMMDD" or "YYYY-MM-DD" (overrides days_back)
        """
        try:
            # If specific date provided, use that
            if specific_date:
                # Handle both formats: "20241031" or "2024-10-31"
                specific_date = specific_date.replace("-", "")
                start_str = specific_date
                end_str = specific_date
                self.logger.info(f"\n→ Fetching contracts for specific date: {specific_date}...")
            else:
                # Calculate date range
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days_back)
                
                # Format dates for expert query (YYYYMMDD format)
                start_str = start_date.strftime("%Y%m%d")
                end_str = end_date.strftime("%Y%m%d")
                
                self.logger.info(f"\n→ Fetching contracts from {start_str} to {end_str}...")
            
            all_notices = []
            page = 1
            limit = 50  # Reduced to avoid exceeding 10k fields per page limit (50 x ~150 fields = 7500)
            
            while True:
                self.logger.info(f"  → Fetching page {page}...")
                
                # Proper TED expert query syntax
                if start_str == end_str:
                    query = f"PD={end_str}"
                else:
                    query = f"PD>={start_str} AND PD<={end_str}"
                
                self.logger.info(f"  Query: {query}")
                
                # Request body with ONLY official parameters from parameters.txt
                request_body = {
                    "query": query,
                    "fields": [
                        # Core identification
                        "ND", "PD", "TY", "NC", "CY", "TW", "TD", "TI", "AA", "AC",
                        "MA", "OJ", "IA", "AU", "PC", "DD", "DI", "DS", "DT", "FT",
                        "HA", "NL", "OL", "PR", "RC", "RN", "RP", "TV", "TV_CUR",
                        
                        # Notice and form information
                        "notice-type", "form-type", "notice-title", "publication-date",
                        "publication-number", "notice-identifier", "notice-version",
                        "notice-subtype", "notice-purpose",
                        
                        # Buyer/Organization information
                        "buyer-name", "buyer-country", "buyer-city", "buyer-post-code",
                        "buyer-email", "buyer-identifier", "buyer-country-sub",
                        "buyer-legal-type", "buyer-contracting-entity",
                        
                        # Contract details
                        "title-lot", "title-part", "title-proc", "title-glo",
                        "description-lot", "description-part", "description-proc", "description-glo",
                        "contract-nature", "contract-nature-main-lot", "contract-nature-main-proc",
                        "main-activity", "contract-identifier", "contract-title",
                        
                        # Values - overall
                        "total-value", "total-value-cur",
                        "estimated-value-proc", "estimated-value-cur-proc",
                        "estimated-value-part", "estimated-value-cur-part",
                        "estimated-value-lot", "estimated-value-cur-lot",
                        "estimated-value-glo", "estimated-value-cur-glo",
                        
                        # Values - results
                        "result-value-notice", "result-value-cur-notice",
                        "result-value-lot", "result-value-cur-lot",
                        
                        # LOT information
                        "identifier-lot", "identifier-part",
                        "internal-identifier-lot", "internal-identifier-part", "internal-identifier-proc",
                        
                        # Winner information (official parameters only)
                        "winner-name", "winner-identifier", "winner-email", 
                        "winner-country", "winner-country-sub", "winner-city", 
                        "winner-post-code", "winner-partname", "winner-size",
                        "winner-listed", "winner-decision-date", "winner-internet-address",
                        "winner-contact-point", "winner-person", "winner-gateway",
                        
                        # Organization tenderer info
                        "organisation-name-tenderer", "organisation-identifier-tenderer",
                        "organisation-email-tenderer", "organisation-country-tenderer",
                        "organisation-city-tenderer", "organisation-post-code-tenderer",
                        "organisation-partname-tenderer", "organisation-person-tenderer",
                        
                        # Tender information
                        "tender-value", "tender-value-cur",
                        "tender-value-cur-lowest", "tender-value-cur-highest",
                        "tender-value-lowest", "tender-value-highest",
                        "tender-identifier", "tender-lot-identifier",
                        "tender-rank", "tender-variant",
                        
                        # Contract specific
                        "BT-150-Contract", "BT-151-Contract", "BT-1501(c)-Contract",
                        "BT-1501(n)-Contract", "BT-1501(p)-Contract", "BT-1501(s)-Contract",
                        
                        # LotResult specific
                        "BT-709-LotResult", "BT-710-LotResult", "BT-711-LotResult",
                        "BT-712(a)-LotResult", "BT-712(b)-LotResult",
                        "BT-119-LotResult", "BT-13713-LotResult",
                        "result-lot-identifier",
                        
                        # Framework and DPS
                        "framework-agreement-lot", "framework-estimated-value",
                        "framework-estimated-value-cur", "framework-value-notice",
                        "framework-value-cur-notice",
                        "BT-118-NoticeResult",
                        
                        # Deadlines
                        "deadline-receipt-tender-date-lot", "deadline-receipt-tender-time-lot",
                        "deadline-date-lot", "deadline-time-lot",
                        
                        # Additional identifiers
                        "ojs-number", "procedure-identifier",
                        "contract-conclusion-date", "contract-duration-start-date-lot",
                        "contract-duration-end-date-lot",
                        
                        # CPV codes
                        "classification-cpv", "main-classification-lot", "main-classification-part",
                        "main-classification-proc", "additional-classification-lot",
                        
                        # Place of performance
                        "place-of-performance-country-lot", "place-of-performance-city-lot",
                        "place-of-performance-post-code-lot",
                    ],
                    "page": page,
                    "limit": limit,
                    "scope": "ALL",
                    "paginationMode": "PAGE_NUMBER",
                    "onlyLatestVersions": True
                }
                
                # Build headers
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
                
                if self.api_key:
                    headers["apikey"] = self.api_key
                
                response = requests.post(
                    self.search_api_url,
                    json=request_body,
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code == 401:
                    self.logger.error("⚠ Authentication failed - check API key")
                    break
                
                if response.status_code != 200:
                    self.logger.error(f"⚠ API returned status {response.status_code}")
                    self.logger.error(f"Response: {response.text[:1000]}")
                    try:
                        error_data = response.json()
                        self.logger.error(f"Error details: {json.dumps(error_data, indent=2)}")
                    except:
                        pass
                    break
                
                data = response.json()
                notices = data.get("notices", [])
                total = data.get("total", 0)
                
                if not notices:
                    self.logger.info("  No more notices found")
                    break
                
                all_notices.extend(notices)
                self.logger.info(f"  ✓ Got {len(notices)} notices (total so far: {len(all_notices)}/{total})")
                
                if len(all_notices) >= total or len(notices) < limit:
                    break
                
                page += 1
                time.sleep(0.5)
            
            self.logger.info(f"✓ Total contracts fetched: {len(all_notices)}")
            return all_notices
            
        except Exception as e:
            self.logger.error(f"⚠ Error fetching TED data: {e}", exc_info=True)
            return []
    
    def save_contract(self, notice: Dict, index: int):
        """Save individual contract to text file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Try to get notice ID from various possible fields
            notice_id = None
            for field in ["ND", "publication-number", "notice-identifier"]:
                if field in notice:
                    val = notice[field]
                    if isinstance(val, list) and val:
                        notice_id = val[0]
                    else:
                        notice_id = val
                    break
            
            if not notice_id:
                notice_id = f"unknown_{index}"
            
            # Clean notice_id for filename
            notice_id = str(notice_id).replace("/", "_").replace("\\", "_")
            
            filename = f"contract_{timestamp}_{index:04d}_{notice_id}.txt"
            filepath = self.output_dir / filename
            
            # Format contract data nicely
            content = self._format_contract(notice, index)
            
            # Save to file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return filepath
            
        except Exception as e:
            self.logger.error(f"⚠ Error saving contract {index}: {e}", exc_info=True)
            return None
    
    def _extract_and_match_lots(self, notice: Dict) -> List[Dict]:
        """
        Extract lot information and match with winners using parallel array indexing
        This is done AFTER receiving data, not in the API request
        """
        lots = []
        
        # Helper to ensure list
        def to_list(data):
            if data is None:
                return []
            if isinstance(data, list):
                return data
            return [data]
        
        # Extract all lot-related data
        lot_ids = to_list(notice.get("identifier-lot"))
        lot_titles = to_list(notice.get("title-lot"))
        lot_descriptions = to_list(notice.get("description-lot"))
        lot_estimated_values = to_list(notice.get("estimated-value-lot"))
        lot_estimated_currencies = to_list(notice.get("estimated-value-cur-lot"))
        lot_result_values = to_list(notice.get("result-value-lot"))
        lot_result_currencies = to_list(notice.get("result-value-cur-lot"))
        
        # Extract winner data (parallel arrays)
        winner_names = to_list(notice.get("winner-name"))
        if not winner_names:
            winner_names = to_list(notice.get("organisation-name-tenderer"))
        
        winner_identifiers = to_list(notice.get("winner-identifier"))
        if not winner_identifiers:
            winner_identifiers = to_list(notice.get("organisation-identifier-tenderer"))
        
        winner_countries = to_list(notice.get("winner-country"))
        if not winner_countries:
            winner_countries = to_list(notice.get("organisation-country-tenderer"))
        
        winner_cities = to_list(notice.get("winner-city"))
        if not winner_cities:
            winner_cities = to_list(notice.get("organisation-city-tenderer"))
        
        winner_emails = to_list(notice.get("winner-email"))
        if not winner_emails:
            winner_emails = to_list(notice.get("organisation-email-tenderer"))
        
        # Extract tender data
        tender_values = to_list(notice.get("tender-value"))
        tender_currencies = to_list(notice.get("tender-value-cur"))
        tender_lot_ids = to_list(notice.get("tender-lot-identifier"))
        
        # Match lots with winners by index position
        if lot_ids or winner_names:
            max_entries = max(len(lot_ids), len(winner_names), len(lot_result_values))
            
            for i in range(max_entries):
                lot_info = {
                    "lot_id": lot_ids[i] if i < len(lot_ids) else f"LOT-{i+1}",
                    "title": lot_titles[i] if i < len(lot_titles) else "N/A",
                    "description": lot_descriptions[i] if i < len(lot_descriptions) else "N/A",
                    "estimated_value": lot_estimated_values[i] if i < len(lot_estimated_values) else "N/A",
                    "estimated_currency": lot_estimated_currencies[i] if i < len(lot_estimated_currencies) else "N/A",
                    "result_value": lot_result_values[i] if i < len(lot_result_values) else "N/A",
                    "result_currency": lot_result_currencies[i] if i < len(lot_result_currencies) else "N/A",
                    "winner_name": winner_names[i] if i < len(winner_names) else "N/A",
                    "winner_identifier": winner_identifiers[i] if i < len(winner_identifiers) else "N/A",
                    "winner_country": winner_countries[i] if i < len(winner_countries) else "N/A",
                    "winner_city": winner_cities[i] if i < len(winner_cities) else "N/A",
                    "winner_email": winner_emails[i] if i < len(winner_emails) else "N/A",
                }
                
                # Try to match tender value by lot ID
                if i < len(tender_lot_ids) and tender_lot_ids[i] == lot_info["lot_id"]:
                    if i < len(tender_values):
                        lot_info["tender_value"] = tender_values[i]
                    if i < len(tender_currencies):
                        lot_info["tender_currency"] = tender_currencies[i]
                
                lots.append(lot_info)
        
        return lots
    
    def _format_contract(self, notice: Dict, index: int) -> str:
        """Format contract data as readable text"""
        separator = "=" * 80
        
        # Helper function to extract values (handles arrays)
        def extract_value(data, default="N/A"):
            if data is None:
                return default
            if isinstance(data, list):
                if not data:
                    return default
                return ", ".join(str(x) for x in data if x)
            return str(data)
        
        # Extract basic fields
        pub_number = extract_value(notice.get("ND"))
        pub_date = extract_value(notice.get("PD"))
        notice_type = extract_value(notice.get("notice-type"))
        form_type = extract_value(notice.get("form-type"))
        buyer_name = extract_value(notice.get("buyer-name"))
        buyer_country = extract_value(notice.get("buyer-country"))
        buyer_city = extract_value(notice.get("buyer-city"))
        title = extract_value(notice.get("notice-title"))
        contract_nature = extract_value(notice.get("contract-nature"))
        main_activity = extract_value(notice.get("main-activity"))
        total_value = extract_value(notice.get("total-value"))
        total_value_cur = extract_value(notice.get("total-value-cur"))
        
        # Extract and match lots with winners
        lots = self._extract_and_match_lots(notice)
        
        # Build LOT section with matched winners
        lot_section = ""
        if lots:
            lot_section = f"""
LOT INFORMATION (Winners Matched by Index Position):
{"-" * 80}
Total Lots: {len(lots)}
"""
            for i, lot in enumerate(lots, 1):
                lot_section += f"""
LOT #{i}: {lot['lot_id']}
  Title:              {lot['title']}
  Description:        {str(lot['description'])[:100]}{'...' if len(str(lot['description'])) > 100 else ''}
  
  VALUES:
    Estimated:        {lot['estimated_value']} {lot['estimated_currency']}
    Result/Award:     {lot['result_value']} {lot['result_currency']}
"""
                if 'tender_value' in lot:
                    lot_section += f"    Tender:           {lot.get('tender_value', 'N/A')} {lot.get('tender_currency', '')}\n"
                
                lot_section += f"""  
  WINNER/CONTRACTOR:
    Name:             {lot['winner_name']}
    Identifier:       {lot['winner_identifier']}
    Country:          {lot['winner_country']}
    City:             {lot['winner_city']}
    Email:            {lot['winner_email']}
"""
        else:
            lot_section = f"""
LOT INFORMATION:
{"-" * 80}
No lot information available in this notice.
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
Main Activity:           {main_activity}

CONTRACT DETAILS:
{"-" * 80}
Title:                   {title}
Contract Nature:         {contract_nature}
Total Value:             {total_value} {total_value_cur}
{lot_section}
{separator}
RAW JSON DATA (Full API Response):
{separator}
{json.dumps(notice, indent=2, ensure_ascii=False)}

{separator}
"""
        return content
    
    def collect_and_save_all(self, days_back: int = 1, specific_date: str = None):
        """
        Main collection process
        
        Args:
            days_back: Number of days to look back (default: 1)
            specific_date: Specific date in format "YYYYMMDD" or "YYYY-MM-DD"
        """
        self.logger.info("\n" + "="*80)
        self.logger.info("🔍 TED CONTRACT DATA COLLECTION")
        self.logger.info("="*80)
        if specific_date:
            self.logger.info(f"📅 Specific Date: {specific_date}")
        else:
            self.logger.info(f"📅 Timeframe: Last {days_back} day(s)")
        self.logger.info(f"💾 Output Directory: {self.output_dir}")
        self.logger.info(f"🔌 API Endpoint: {self.search_api_url}")
        self.logger.info(f"🕐 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("="*80 + "\n")
        
        # Fetch all contracts
        notices = self.fetch_all_contracts(days_back=days_back, specific_date=specific_date)
        
        if not notices:
            self.logger.warning("\n❌ No contracts found!")
            return
        
        # Save each contract
        self.logger.info(f"\n→ Saving {len(notices)} contracts individually...")
        saved_count = 0
        
        for idx, notice in enumerate(notices, 1):
            filepath = self.save_contract(notice, idx)
            if filepath:
                saved_count += 1
                if saved_count % 25 == 0:
                    self.logger.info(f"  ✓ Saved {saved_count}/{len(notices)} contracts...")
        
        self.logger.info(f"\n✅ Collection complete!")
        self.logger.info(f"📊 Total contracts saved: {saved_count}")
        self.logger.info(f"📁 Location: {self.output_dir}")
        
        # Generate summary report
        self._generate_summary(notices)
    
    def _generate_summary(self, notices: List[Dict]):
        """Generate summary report of collected data"""
        summary_file = self.output_dir / f"SUMMARY_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        def get_value(notice, key):
            val = notice.get(key, "Unknown")
            if isinstance(val, list):
                return str(val[0]) if val else "Unknown"
            return str(val) if val else "Unknown"
        
        notice_types = {}
        form_types = {}
        countries = {}
        winners = {}
        total_lots = 0
        
        for notice in notices:
            ntype = get_value(notice, 'notice-type')
            notice_types[ntype] = notice_types.get(ntype, 0) + 1
            
            ftype = get_value(notice, 'form-type')
            form_types[ftype] = form_types.get(ftype, 0) + 1
            
            country = get_value(notice, 'buyer-country')
            countries[country] = countries.get(country, 0) + 1
            
            lots = self._extract_and_match_lots(notice)
            total_lots += len(lots)
            for lot in lots:
                winner = str(lot.get('winner_name', 'Unknown'))
                if winner != "Unknown" and winner != "N/A":
                    winners[winner] = winners.get(winner, 0) + 1
        
        summary = f"""
{"="*80}
TED CONTRACT COLLECTION SUMMARY
{"="*80}

Collection Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Total Notices: {len(notices)}
Total Lots: {total_lots}

BREAKDOWN BY NOTICE TYPE:
{"-"*80}
"""
        
        for ntype, count in sorted(notice_types.items(), key=lambda x: x[1], reverse=True):
            summary += f"{ntype}: {count}\n"
        
        summary += f"""
{"-"*80}
TOP 10 COUNTRIES:
{"-"*80}
"""
        
        for country, count in sorted(countries.items(), key=lambda x: x[1], reverse=True)[:10]:
            summary += f"{country}: {count}\n"
        
        summary += f"""
{"-"*80}
TOP 10 WINNERS/CONTRACTORS:
{"-"*80}
"""
        
        for winner, count in sorted(winners.items(), key=lambda x: x[1], reverse=True)[:10]:
            if len(winner) > 70:
                winner = winner[:70] + "..."
            summary += f"{winner}: {count} lot(s)\n"
        
        summary += f"""
{"-"*80}
FILES SAVED TO: {self.output_dir}
{"="*80}
"""
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(summary)
        
        print(f"\n{summary}")
        self.logger.info(f"📄 Summary saved to: {summary_file}")


def main():
    """Main execution"""
    
    import os
    api_key = os.environ.get('TED_API_KEY', '0f0d8c2f68bb46bab7afa51c46053433')
    
    collector = TEDDataCollector(
        api_key=api_key,
        output_dir=r"E:\Anaconda3\envs\ted\Scripts\Results"
    )
    
    # ============================================================================
    # CONFIGURATION - CHOOSE HOW TO SEARCH:
    # ============================================================================
    
    # OPTION 1: Get contracts from last X days
    # collector.collect_and_save_all(days_back=1)  # Last 1 day
    # collector.collect_and_save_all(days_back=7)  # Last 7 days
    # collector.collect_and_save_all(days_back=30)  # Last 30 days
    
    # OPTION 2: Get contracts from a specific date
    # collector.collect_and_save_all(specific_date="20241031")  # Oct 31, 2024
    # collector.collect_and_save_all(specific_date="2024-10-31")  # Also works with dashes
    
    # OPTION 3: Get contracts from a date range (manually set in fetch_all_contracts)
    # You can also modify the function to accept start_date and end_date parameters
    
    # ============================================================================
    # DEFAULT: Last 1 day
    # ============================================================================
    try:
        collector.collect_and_save_all(days_back=1)  # Change this line!
        
        # Examples:
        # collector.collect_and_save_all(days_back=7)  # Last week
        # collector.collect_and_save_all(specific_date="20241025")  # Specific date
        
    except KeyboardInterrupt:
        collector.logger.info("\n\n✓ Collection stopped by user")
    except Exception as e:
        collector.logger.error(f"\n❌ Error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
