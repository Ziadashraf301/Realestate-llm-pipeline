"""
BAYUT Real Estate Scraper - Extracts Complete Property Data From https://www.bayut.eg/
"""

import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime
import re
import hashlib
from Real_Estate_Data_Pipelines.src.logger import LoggerFactory

class BAYUTRealEstateScraper:
    """BAYUT Real Estate Scraper for Egyptian real estate with deep page scraping"""
    
    def __init__(self, db, log_dir='Real_Estate_Data_Pipelines/logs/'):
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'ar,en-US;q=0.9,en;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Referer': 'https://www.bayut.eg/',
        })    

        self.results = []
        self.base_url = "https://www.bayut.eg"

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


    def scrape(self, city='الإسكندرية', listing_type='عقارات-للبيع', max_pages=2):
        """Main scraping method"""
        self.logger.info(f"🏠 Scraping BAYUT: {city} - {listing_type}")

        self.logger.info(f"🔍 Mapped Query Params - City: {city}, Listing Type: {listing_type}")
        city, listing_type = self._map_query_params(city, listing_type)
        self.logger.info(f"🔍 Using Query Params - City: {city}, Listing Type: {listing_type}")
        
        new_properties_count = 0
        skipped_properties_count = 0
        error_count = 0     

        for page in range(1, max_pages + 1):
            try:
                if page == 1:
                    url = f"{self.base_url}/{listing_type}/{city}/"
                else:
                    url = f"{self.base_url}/صفحة-{page}/{listing_type}/{city}"
                
                self.logger.info(f"📄 Page {page}: {url}")
                time.sleep(3)
                
                response = self.session.get(url, timeout=20)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find all property listing links using aria-label
                property_links = soup.find_all('a', href=True, attrs={'aria-label': 'Listing link'})
                
                self.logger.info(f"   Found {len(property_links)} property links")
                
                if not property_links:
                    self.logger.warning(f"⚠️  No properties found on page {page}")
                    break
                
                # Extract unique property URLs
                property_urls = []
                for link in property_links:
                    href = link.get('href')
                    if href:
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
                            self.logger.info(f"   [{idx}/{len(property_urls)}] ⏭️  Skipped: {prop_url[:60]}...")
                            skipped_properties_count += 1
                            continue
                        
                        self.logger.info(f"   [{idx}/{len(property_urls)}] 🔄 Scraping: {prop_url[:80]}...")
                        property_data = self._scrape_property_detail_page(prop_url, city, listing_type)
                        
                        if property_data:
                            self.results.append(property_data)
                            self.existing_urls.add(prop_url)
                            new_properties_count += 1
                            self.logger.info(f"      ✅ Property scraped successfully")
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
            
            # Generate unique ID from URL
            property_id = hashlib.md5(url.encode()).hexdigest()[:16]

            # Initialize variables
            title = ""
            description = ""
            price_egp = None
            price_text = None
            price_currency = "EGP"
            property_type = "unknown"
            bedrooms = None
            bathrooms = None
            area_sqm = None
            floor_number = None
            address = ""
            latitude = None
            longitude = None
            last_updated = None
            images = []
            agent_name = None
            agent_phone = None
            agent_whatsapp = None
            agent_type = None

            # Extract data from JSON-LD schema
            script_tags = soup.find_all("script", type="application/ld+json")
            for script_tag in script_tags:
                try:
                    data = json.loads(script_tag.string)
                    
                    # Navigate through the nested structure
                    if isinstance(data, dict) and '@graph' in data:
                        for item in data['@graph']:
                            if item.get('@type') == 'RealEstateListing':
                                # Basic listing info
                                title = item.get('name', '').split('|')[0].strip()
                                last_updated = item.get('datePosted')
                                
                                # Main entity contains property details
                                main_entity = item.get('mainEntity', {})
                                description = main_entity.get('description', '')
                                
                                # Handle different property types (sale vs rental)
                                # Check if it's a rental (RentAction) or sale
                                entity_types = main_entity.get('@type', [])
                                if isinstance(entity_types, list):
                                    is_rental = 'RentAction' in entity_types
                                else:
                                    is_rental = 'RentAction' in str(entity_types)
                                
                                # Extract price information based on property type
                                if is_rental:
                                    # Rental properties have priceSpecification
                                    price_spec = main_entity.get('priceSpecification', {})
                                    price_egp = price_spec.get('price')
                                    price_currency = price_spec.get('priceCurrency', 'EGP')
                                    # Check unit text to ensure it's monthly
                                    unit_text = price_spec.get('unitText', '').lower()
                                    if 'monthly' not in unit_text and price_egp:
                                        # Could be annual or other, handle accordingly
                                        pass
                                else:
                                    # Sale properties have direct price
                                    price_egp = main_entity.get('price')
                                    price_currency = main_entity.get('priceCurrency', 'EGP')
                                
                                price_text = f"{price_egp} {price_currency}" if price_egp else None
                                
                                # Property type
                                property_type = main_entity.get('accommodationCategory', 'unknown')
                                
                                # Property details
                                bedrooms = main_entity.get('numberOfBedrooms')
                                bathrooms = main_entity.get('numberOfBathroomsTotal')
                                
                                # Area
                                floor_size = main_entity.get('floorSize', {})
                                area_value = floor_size.get('value')
                                if area_value:
                                    try:
                                        # Remove any non-numeric characters except decimal point
                                        import re
                                        area_clean = re.sub(r'[^\d.]', '', str(area_value))
                                        area_sqm = float(area_clean)
                                    except:
                                        area_sqm = None
                                
                                # Extract floor number from description
                                floor_number = self._extract_floor_number(description)
                                
                                # Location details
                                geo = main_entity.get('geo', {})
                                latitude = geo.get('latitude')
                                longitude = geo.get('longitude')
                                
                                address_data = main_entity.get('address', {})
                                locality = address_data.get('addressLocality', '')
                                region = address_data.get('addressRegion', '')
                                country = address_data.get('addressCountry', {}).get('name', '')
                                address = f"{locality}, {region}, {country}".strip(', ')
                                
                                # Images
                                images = main_entity.get('image', [])
                                if isinstance(images, str):
                                    images = [images]
                                
                                # Agent information - different for rentals vs sales
                                if is_rental:
                                    # Rental properties use realEstateAgent
                                    agent_data = main_entity.get('realEstateAgent', {})
                                    agent_name = agent_data.get('name')
                                    agent_phone = agent_data.get('telephone')
                                    
                                    # Check for memberOf to determine agency
                                    agency = agent_data.get('memberOf', {})
                                    agent_type = 'agency' if agency.get('name') else 'individual'
                                else:
                                    # Sale properties use seller
                                    seller = main_entity.get('seller', {})
                                    agent_name = seller.get('name')
                                    agent_phone = seller.get('telephone')
                                    
                                    # Check for memberOf to determine agency
                                    agency = seller.get('memberOf', {})
                                    agent_type = 'agency' if agency.get('name') else 'individual'
                                
                                # Extract WhatsApp from description if not found
                                if not agent_phone and description:
                                    import re
                                    whatsapp_match = re.search(r'https://wa\.me/(\d+)', description)
                                    if whatsapp_match:
                                        agent_whatsapp = f"+{whatsapp_match.group(1)}"
                                
                                break
                except Exception as e:
                    self.logger.debug(f"Error parsing JSON-LD: {e}")
                    continue

            # Fallback: Extract title from h1 if not found in JSON-LD
            if not title:
                title_elem = soup.select_one('h1')
                if title_elem:
                    title = title_elem.get_text(strip=True)
            
            # Fallback: Extract price from description if not found in JSON-LD
            if not price_egp and description:
                import re
                # Look for price patterns in description
                price_patterns = [
                    r'بسعر\s*:\s*([\d,]+)\s*ج',
                    r'بسعر\s*:\s*([\d,]+)',
                    r'سعر\s*:\s*([\d,]+)\s*ج',
                    r'مطلوب\s*([\d,]+)\s*ج',
                    r'(\d{4,})[,\s]*جنيهاً?'
                ]
                
                for pattern in price_patterns:
                    match = re.search(pattern, description)
                    if match:
                        price_str = match.group(1).replace(',', '')
                        try:
                            price_egp = int(price_str)
                            price_text = f"{price_egp} EGP"
                            break
                        except:
                            continue

            # Fallback: Extract address from breadcrumb if needed
            if not address and city:
                address = f"{city}, مَصر"

            # Prepare specs_data dictionary
            specs_data = {
                'property_type': property_type,
                'bedrooms': bedrooms,
                'bathrooms': bathrooms,
                'area_sqm': area_sqm,
                'floor_number': floor_number
            }
            
            # Prepare metadata dictionary
            metadata = {
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'last_updated': last_updated
            }
            
            # Prepare agent_info dictionary
            agent_info = {
                'name': agent_name,
                'phone': agent_phone,
                'whatsAppNumber': agent_whatsapp or agent_phone,  # Use phone if whatsapp not found
                'type': agent_type or 'individual'
            }

            # Determine listing type
            is_rental_listing = any(word in listing_type.lower() for word in ['ايجار', 'للايجار', 'إيجار'])
            
            # Compile all data
            property_data = {
                'property_id': f"bayut_{property_id}",
                'source': 'bayut',
                'url': url,
                'title': title,
                'description': description,
                'price_egp': price_egp,
                'price_text': price_text,
                'currency': price_currency,
                'property_type': self._determine_property_type(specs_data.get('property_type', 'unknown')),
                'listing_type': 'ايجار' if is_rental_listing else 'تمليك',
                
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
            self.logger.error(f"Failed to scrape {url}: {e}")
            return None


    def _extract_floor_number(self, description):
        """Extract floor number from description text"""
        floor_number = None
        
        # Arabic number words mapping
        arabic_numbers = {
            'الأول': 1, 'الاول': 1, 'أول': 1, 'اول': 1,
            'الثاني': 2, 'الثانى': 2, 'ثاني': 2, 'ثانى': 2,
            'الثالث': 3, 'ثالث': 3,
            'الرابع': 4, 'رابع': 4,
            'الخامس': 5, 'خامس': 5,
            'السادس': 6, 'سادس': 6,
            'السابع': 7, 'سابع': 7,
            'الثامن': 8, 'ثامن': 8,
            'التاسع': 9, 'تاسع': 9,
            'العاشر': 10, 'عاشر': 10,
            'الحادي عشر': 11, 'حادي عشر': 11,
            'الثاني عشر': 12, 'ثاني عشر': 12
        }
        
        if not description:
            return None
        
        # Check if any Arabic number word exists in description
        for word, number in arabic_numbers.items():
            if word in description:
                floor_number = number
                break
        
        # If not found, try numeric patterns
        if floor_number is None:
            floor_patterns = [
                r'(?:الدور|الطابق)\s*:?\s*(\d+)',  # الدور:4 or الدور: 4
                r'floor\s*:?\s*(\d+)',                # floor: 4
                r'(\d+)(?:st|nd|rd|th)\s*floor',      # 4th floor
                r'floor\s*(?:number|no\.?)?\s*(\d+)'  # floor number 4
            ]
            
            for pattern in floor_patterns:
                match = re.search(pattern, description, re.IGNORECASE)
                if match:
                    try:
                        floor_number = int(match.group(1))
                        break
                    except (ValueError, IndexError):
                        continue
        
        return floor_number
    
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
            'شاليه': ['شاليه', 'chalet', 'شالية', 'كبينة'],
            'فيلا': ['فيلا', 'villa', 'فيللا', "اي فيلا"],
            
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
            'أرض سكنية': ['land-or-residential', 'أرض سكنية'],
            
            # Other specific types
            'سكن مشترك': ['shared-rooms', 'غرف مشتركة', 'سكن مشترك'],
            'روف': ['rwf', 'روف', 'سطح', 'roof', 'سطج'],
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
    

    def _map_query_params(self, city: str, listing_type: str):
        """Map city and listing type to BAYUT query parameters"""
        city_mapping = {
            'alexandria': 'الإسكندرية',
            'cairo': 'القاهرة',
            # Add more city mappings as needed
        }
        
        listing_type_mapping = {
            'for-sale': 'عقارات-للبيع',
            'for-rent': 'عقارات-للايجار',
            # Add more listing type mappings as needed
        }
        
        mapped_city = city_mapping.get(city, city)
        mapped_listing_type = listing_type_mapping.get(listing_type, listing_type)
        
        return mapped_city, mapped_listing_type