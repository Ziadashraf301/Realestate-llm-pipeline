"""
AQARMAP Real Estate Scraper - Extracts Complete Property Data From https://aqarmap.com.eg
"""

import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime
import re
import hashlib
from Real_Estate_Data_Pipelines.src.logger import LoggerFactory

class AQARMAPRealEstateScraper:
    """AQARMAP Real Estate Scraper for Egyptian real estate with deep page scraping"""
    
    def __init__(self, db, log_dir='Real_Estate_Data_Pipelines/logs/'):
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'ar,en-US;q=0.9,en;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Referer': 'https://aqarmap.com.eg/',
        })
        
        self.results = []
        self.base_url = "https://aqarmap.com.eg"

        # Initialize logger
        self.log_dir = log_dir
        self.logger = LoggerFactory.create_logger(log_dir=self.log_dir)

        # Check the database connection
        if db.client is None:
            self.logger.error("Database connection failed")
            raise ConnectionError("Database connection failed")
        else:
            self.db_client = db

        self.existing_urls = self.db_client.load_existing_urls_from_database()


    def scrape(self, city='alexandria', listing_type='for-sale', max_pages=2):
        """Main scraping method"""
        self.logger.info(f"🏠 Scraping Aqarmap: {city} - {listing_type}")
        
        new_properties_count = 0
        skipped_properties_count = 0
        error_count = 0
        
        for page in range(1, max_pages + 1):
            try:
                if page == 1:
                    url = f"{self.base_url}/ar/{listing_type}/property-type/{city}/"
                else:
                    url = f"{self.base_url}/ar/{listing_type}/property-type/{city}/?page={page}"
                
                self.logger.info(f"📄 Page {page}: {url}")
                time.sleep(3)
                
                response = self.session.get(url, timeout=20)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find all listing cards
                listing_cards = soup.find_all('div', class_='listing-card')
                self.logger.info(f"   Found {len(listing_cards)} listing cards")
                
                if not listing_cards:
                    self.logger.warning(f"⚠️  No listings found on page {page}")
                    break
                
                # Extract property URLs from cards
                property_urls = []
                for card in listing_cards:
                    link = card.find('a', href=re.compile(r'/ar/listing/'))
                    if link and link.get('href'):
                        href = link['href']
                        if not href.startswith('http'):
                            href = f"{self.base_url}{href}"
                        property_urls.append(href)
                
                # Remove duplicates
                property_urls = list(dict.fromkeys(property_urls))
                self.logger.info(f"   Found {len(property_urls)} unique property URLs")
                
                # Scrape each property detail page
                for idx, prop_url in enumerate(property_urls, 1):
                    try:
                        # Check if already in BigQuery
                        if prop_url in self.existing_urls:
                            self.logger.info(f"   [{idx}/{len(property_urls)}] ⏭️  Skipped (already in BigQuery): {prop_url[:60]}...")
                            skipped_properties_count += 1
                            continue
                        
                        self.logger.info(f"   [{idx}/{len(property_urls)}] 🔄 Scraping: {prop_url[:80]}...")
                        property_data = self._scrape_property_detail_page(prop_url, city, listing_type)
                        
                        if property_data:
                            self.results.append(property_data)
                            self.existing_urls.add(prop_url)
                            new_properties_count += 1
                            self.logger.info(f"      ✅ {property_data['title'][:50]}...")
                        else:
                            error_count += 1
                        
                        # Rate limiting
                        time.sleep(2)
                        
                    except Exception as e:
                        error_count += 1
                        self.logger.error(f"      ❌ Error: {e}")
                        continue
                
            except Exception as e:
                self.logger.error(f"❌ Error on page {page}: {e}")
                break
        
        # Final summary
        self.logger.info("📊 Scraping Summary:")
        self.logger.info(f"   🆕 New properties scraped: {new_properties_count}")
        self.logger.info(f"   ⏭️  Skipped (already in BigQuery): {skipped_properties_count}")
        self.logger.info(f"   ❌ Errors: {error_count}")
        
        return self.results
    
    def _scrape_property_detail_page(self, url, city, listing_type):
        """Scrape individual property detail page for complete information"""
        try:
            response = self.session.get(url, timeout=20)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Generate unique ID
            property_id = hashlib.md5(url.encode()).hexdigest()[:16]
            
            # Extract title
            title = ""
            title_selectors = ['h1', 'h2[class*="title"]', '.property-title']
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    break
            
            # Extract price from JSON-LD
            price_text, price_egp, price_currency = "", None, "EGP"
            script_tags = soup.find_all("script", type="application/ld+json")

            for script_tag in script_tags:
                try:
                    data = json.loads(script_tag.string)
                    
                    if isinstance(data, list):
                        for item in data:
                            if item.get("@type") == "Product":
                                data = item
                                break
                    
                    if data.get("@type") == "Product":
                        offers = data.get("offers", {})
                        price_egp = offers.get("price")
                        price_currency = offers.get("priceCurrency", "EGP")
                        
                        if price_egp:
                            try:
                                price_egp = float(price_egp) if '.' in str(price_egp) else int(price_egp)
                            except (ValueError, TypeError):
                                pass
                            
                            price_text = f"{price_egp} {price_currency}"
                            break
                            
                except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
                    continue
            
            # Extract description from meta tag
            description = ""
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                description = meta_desc.get('content').strip()
            
            # If not found in meta, try og:description
            if not description:
                og_desc = soup.find('meta', attrs={'property': 'og:description'})
                if og_desc and og_desc.get('content'):
                    description = og_desc.get('content').strip()
            
            # Extract specifications
            specs_data = self._extract_specifications(soup)
            
            # Extract images
            images = self._extract_images(soup)
            
            # Extract agent information
            agent_info = self._extract_agent_info(soup)

            # Extract metadata and location
            metadata = self._extract_metadata(soup)

            # Compile all data
            property_data = {
                'property_id': f"aqarmap_{property_id}",
                'source': 'aqarmap',
                'url': url,
                'title': title,
                'description': description,
                'price_egp': price_egp,
                'price_text': price_text,
                'currency': price_currency,
                'property_type': specs_data.get('property_type', 'unknown'),
                'listing_type': 'تمليك' if 'sale' in listing_type else 'ايجار',
                
                # Property details
                'bedrooms': specs_data.get('bedrooms'),
                'bathrooms': specs_data.get('bathrooms'),
                'area_sqm': specs_data.get('area_sqm'),
                'floor_number': specs_data.get('floor_number'),
                
                # Location details
                'location': city,
                'address': metadata['address'],
                'latitude': metadata['latitude'],
                'longitude': metadata['longitude'],
                'last_updated': metadata['last_updated'],
                
                # Images
                'images': images,
                
                # Agent information
                'agent_name': agent_info.get('name'),
                'agent_phone': agent_info.get('phone'),
                'agent_whatsAppNumber': agent_info.get('whatsAppNumber'),
                'agent_type': agent_info.get('type'),
                
                # Timestamp
                'scraped_at': datetime.now().isoformat()
            }
            
            return property_data
            
        except Exception as e:
            self.logger.error(f"Error scraping detail page: {e}")
            return None
    
    def _extract_specifications(self, soup):
        """Extract property specifications from detail page"""
        specs = {}
        
        # Get all text for analysis
        all_text = soup.get_text(separator=' ', strip=True)
        
        # Extract property type
        specs['property_type'] = self._determine_property_type(all_text)
        
        # Extract bedrooms
        bed_patterns = [r'(\d+)\s*(?:غرف|غرفة|bed|bedroom)', r'bedrooms?\s*[:\-]?\s*(\d+)']
        for pattern in bed_patterns:
            match = re.search(pattern, all_text, re.I)
            if match:
                specs['bedrooms'] = int(match.group(1))
                break
        
        # Extract bathrooms
        bath_patterns = [r'(\d+)\s*(?:حمام|bath|bathroom)', r'bathrooms?\s*[:\-]?\s*(\d+)']
        for pattern in bath_patterns:
            match = re.search(pattern, all_text, re.I)
            if match:
                specs['bathrooms'] = int(match.group(1))
                break
        
        # Extract area
        area_patterns = [r'(\d+)\s*(?:متر|م²|sqm|m²)', r'area\s*[:\-]?\s*(\d+)']
        for pattern in area_patterns:
            match = re.search(pattern, all_text, re.I)
            if match:
                specs['area_sqm'] = int(match.group(1))
                break
        
        # Extract floor number
        floor_patterns = [r'(?:الدور|floor|الطابق)\s*(?:ال)?(\d+)', r'(\d+)(?:st|nd|rd|th)?\s*floor']
        for pattern in floor_patterns:
            match = re.search(pattern, all_text, re.I)
            if match:
                specs['floor_number'] = int(match.group(1))
                break
        
        return specs


    def _extract_images(self, soup):
        """Extract property images"""
        images = []
        
        # Find all images
        img_tags = soup.find_all('img', src=True)
        
        for img in img_tags:
            src = img.get('src', '')
            # Filter for property images
            if any(x in src for x in ['aqarmap.com', 'property', 'listing', 'thumb']):
                if src not in images:
                    images.append(src)
        
        return images[:20]  # Limit to 20 images
    
    def _extract_metadata(self, soup):
        """Extract metadata like address, latitude, longitude, and last updated timestamp"""
        metadata = {
            'address': None,
            'latitude': None,
            'longitude': None,
            'last_updated': None
        }
        
        html = soup.decode()
        
        try:
            # Look for address
            address_patterns = [
                r'"address":\s*"([^"]+)"',
                r'\\"address\\":\s*\\"([^"]+)\\"',
            ]
            for pattern in address_patterns:
                address_match = re.search(pattern, html)
                if address_match:
                    address = address_match.group(1)
                    address = address.replace('\\r\\n', ' ').replace('\\n', ' ').replace('\\r', ' ')
                    address = address.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
                    metadata['address'] = address.strip()
                    break
            
            # Look for latitude
            lat_patterns = [
                r'"center_lat":\s*([0-9.]+)',
                r'\\"center_lat\\":\s*([0-9.]+)',
            ]
            for pattern in lat_patterns:
                lat_match = re.search(pattern, html)
                if lat_match:
                    metadata['latitude'] = float(lat_match.group(1))
                    break
            
            # Look for longitude
            lng_patterns = [
                r'"center_lng":\s*([0-9.]+)',
                r'\\"center_lng\\":\s*([0-9.]+)',
            ]
            for pattern in lng_patterns:
                lng_match = re.search(pattern, html)
                if lng_match:
                    metadata['longitude'] = float(lng_match.group(1))
                    break
            
            # Look for updated_at
            updated_patterns = [
                r'"updated_at":\s*"([^"]+)"',
                r'\\"updated_at\\":\s*\\"([^"]+)\\"',
            ]
            for pattern in updated_patterns:
                updated_match = re.search(pattern, html)
                if updated_match:
                    metadata['last_updated'] = updated_match.group(1)
                    break
            
        except Exception as e:
            self.logger.warning(f"⚠️ Could not parse metadata: {e}")
        
        return metadata

    def _extract_agent_info(self, soup):
        """Extract agent/broker information"""
        agent_info = {'name': None, 'phone': None, 'whatsAppNumber': None, 'type': None}
        
        html = soup.decode()
        
        try:
            # Look for full_name
            name_patterns = [
                r'"full_name":\s*"([^"]+)"',
                r'\\"full_name\\":\s*\\"([^"]+)\\"',
            ]
            for pattern in name_patterns:
                name_match = re.search(pattern, html)
                if name_match:
                    agent_info['name'] = name_match.group(1)
                    break
            
            # Look for phone_number
            phone_patterns = [
                r'"phone_number":\s*"([^"]+)"',
                r'\\"phone_number\\":\s*\\"([^"]+)\\"',
            ]
            for pattern in phone_patterns:
                phone_match = re.search(pattern, html)
                if phone_match:
                    agent_info['phone'] = phone_match.group(1)
                    break

            # Look for whatsAppNumber
            whatsapp_patterns = [
                r'"whatsAppNumber":\s*"([^"]+)"',
                r'\\"whatsAppNumber\\":\s*\\"([^"]+)\\"',
            ]
            for pattern in whatsapp_patterns:
                whatsapp_match = re.search(pattern, html)
                if whatsapp_match:
                    agent_info['whatsAppNumber'] = whatsapp_match.group(1)
                    break
            
            # Look for user_type
            type_patterns = [
                r'"user_type":\s*(\d+)',
                r'\\"user_type\\":\s*(\d+)',
            ]
            for pattern in type_patterns:
                type_match = re.search(pattern, html)
                if type_match:
                    user_type = int(type_match.group(1))
                    if user_type == 0:
                        agent_info['type'] = 'owner'
                    elif user_type == 1:
                        agent_info['type'] = 'agent'
                    else:
                        agent_info['type'] = 'unknown'
                    break
            
        except Exception as e:
            self.logger.warning(f"⚠️ Could not parse agent info: {e}")
        
        return agent_info
    
    def _determine_property_type(self, text):
        """Determine property type from text with better priority and context"""
        text = text.lower()
        
        # Priority order: Check more specific types first
        property_types = {
            # Most specific first
            'شقة مفروشة': ['furnished-apartment', 'شقة مفروشة', 'مفروش'],
            'شقة جاردن': ['apartment-with-garden', 'شقة بحديقة', 'شقة جاردن'],
            'دوبلكس جاردن': ['duplex-garden', 'دوبلكس جاردن'],
            'بنتهاوس': ['بنتهاوس', 'penthouse', 'بنت هاوس'],
            'دوبلكس': ['دوبلكس', 'duplex', 'دوبليكس'],
            'استوديو': ['استوديو', 'studio', 'ستوديو'],
            'تاون هاوس': ['تاون هاوس', 'townhouse', 'تاون-هاوس'],
            'شاليه': ['شاليه', 'chalet', 'شالية'],
            'فيلا': ['فيلا', 'villa', 'فيللا'],
            
            # Commercial & Administrative
            'عيادة': ['medical', 'عيادة', 'طبي'],
            'إداري': ['administrative', 'إداري', 'اداري'],
            'تجاري': ['commercial', 'تجاري'],
            'محل': ['محل', 'shop', 'store', 'متجر'],
            'مكتب': ['مكتب', 'office', 'مكاتب'],
            
            # Buildings & Land
            'عمارة': ['building', 'عمارة', 'بناية', 'مبنى'],
            'أرض زراعية': ['land-or-farm', 'أرض زراعية', 'مزرعة'],
            'أرض تجارية': ['land-or-commercial', 'أرض تجارية'],
            'أرض زراعية': ['أرض', 'land', 'plot', 'ارض', 'قطعة أرض'],
            
            # Other specific types
            'سكن مشترك': ['shared-rooms', 'غرف مشتركة', 'سكن مشترك'],
            'روف': ['rwf', 'روف'],
            'دور كامل': ['dwr-kml', 'دور كامل', 'طابق كامل'],
            
            # Generic apartment (check last)
            'شقة': ['شقة', 'apartment', 'flat'],
        }
        
        # Check each type in priority order
        for ptype, keywords in property_types.items():
            for keyword in keywords:
                if f' {keyword} ' in f' {text} ' or text.startswith(keyword) or text.endswith(keyword):
                    return ptype
        
        return 'شقة'
