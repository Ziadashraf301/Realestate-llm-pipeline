"""
High-Performance Asynchronous BAYUT Scraper (Enterprise M2).
Extracts complete, deep property data (JSON-LD RealEstateListing, specifications,
images, agent details, floor numbers, exact coordinates, addresses) from https://www.bayut.eg/
Powered by httpx.AsyncClient, asyncio.Semaphore(5), Redis-backed deduplication,
and direct ClickHouse ReplacingMergeTree warehouse ingestion.
"""

import asyncio
import hashlib
import json
import random
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from bs4 import BeautifulSoup
import httpx

from real_estate.core.logger import logger
from real_estate.core.settings import settings
from real_estate.repositories.cache_repository import RedisCacheRepository
from real_estate.repositories.warehouse_repository import ClickHouseWarehouseRepository


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0",
]


class AsyncBayutScraper:
    """Enterprise Async Scraper for Egyptian Real Estate on Bayut (https://www.bayut.eg)."""

    def __init__(
        self,
        warehouse: Optional[ClickHouseWarehouseRepository] = None,
        cache_repo: Optional[RedisCacheRepository] = None,
        max_concurrency: int = 5,
        logger: Optional[Any] = None,
    ):
        self.base_url = "https://www.bayut.eg"
        self.warehouse = warehouse
        self.cache_repo = cache_repo
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.in_memory_seen_urls: Set[str] = set()
        self.dagster_logger = logger

    def _log(self, level: str, msg: str, **kwargs) -> None:
        """Emits to Dagster context.log (if provided) and to standard structlog."""
        extra_str = " | ".join(f"{k}={v}" for k, v in kwargs.items())
        display_msg = f"{msg} ({extra_str})" if extra_str else msg
        if self.dagster_logger:
            try:
                fn = getattr(self.dagster_logger, level, None) or self.dagster_logger.info
                fn(display_msg)
            except Exception:
                pass
        struct_func = getattr(logger, level, None) or logger.info
        struct_func(msg, **kwargs)

    def _get_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Referer": "https://www.bayut.eg/",
            "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }

    def _map_query_params(self, city: str, listing_type: str) -> tuple[str, str]:
        """Maps query parameters to Bayut Arabic directory routes."""
        city_mapping = {
            "alexandria": "الإسكندرية",
            "cairo": "القاهرة",
            "giza": "الجيزة",
            "الإسكندرية": "الإسكندرية",
            "القاهرة": "القاهرة",
            "الجيزة": "الجيزة",
        }
        listing_type_mapping = {
            "for-sale": "عقارات-للبيع",
            "for-rent": "عقارات-للايجار",
            "sale": "عقارات-للبيع",
            "rent": "عقارات-للايجار",
            "عقارات-للبيع": "عقارات-للبيع",
            "عقارات-للايجار": "عقارات-للايجار",
        }
        mapped_city = city_mapping.get(city.lower(), city)
        mapped_listing = listing_type_mapping.get(listing_type.lower(), listing_type)
        return mapped_city, mapped_listing

    async def _is_url_scraped(self, url: str) -> bool:
        """Checks Redis SET or local set to prevent duplicate requests."""
        if url in self.in_memory_seen_urls:
            return True

        if self.cache_repo:
            try:
                if await self.cache_repo.is_url_scraped(url):
                    self.in_memory_seen_urls.add(url)
                    return True
            except Exception:
                pass
        return False

    async def _mark_url_scraped(self, url: str) -> None:
        self.in_memory_seen_urls.add(url)
        if self.cache_repo:
            try:
                await self.cache_repo.mark_url_scraped(url)
            except Exception:
                pass

    def _extract_floor_number(self, description: str) -> Optional[int]:
        """Extracts floor number with Arabic words and numeric regexes."""
        if not description:
            return None

        arabic_numbers = {
            "الأول": 1, "الاول": 1, "أول": 1, "اول": 1,
            "الثاني": 2, "الثانى": 2, "ثاني": 2, "ثانى": 2,
            "الثالث": 3, "ثالث": 3,
            "الرابع": 4, "رابع": 4,
            "الخامس": 5, "خامس": 5,
            "السادس": 6, "سادس": 6,
            "السابع": 7, "سابع": 7,
            "الثامن": 8, "ثامن": 8,
            "التاسع": 9, "تاسع": 9,
            "العاشر": 10, "عاشر": 10,
            "الحادي عشر": 11, "حادي عشر": 11,
            "الثاني عشر": 12, "ثاني عشر": 12,
        }

        for word, number in arabic_numbers.items():
            if word in description:
                return number

        floor_patterns = [
            r"(?:الدور|الطابق)\s*:?\s*(\d+)",
            r"floor\s*:?\s*(\d+)",
            r"(\d+)(?:st|nd|rd|th)\s*floor",
            r"floor\s*(?:number|no\.?)?\s*(\d+)",
        ]
        for pattern in floor_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1))
                except (ValueError, IndexError):
                    continue
        return None

    def _determine_property_type(self, title: str, text: str = "") -> str:
        """Accurately determines property type with priority given to title keywords."""
        title_lower = title.lower()
        if any(w in title_lower for w in ["مكتب", "مقر إداري", "مقر اداري", "اداري", "إداري", "مكاتب", "office"]):
            return "مكتب"
        if any(w in title_lower for w in ["عيادة", "مركز طبي", "طبي", "clinic", "medical"]):
            return "عيادة"
        if any(w in title_lower for w in ["محل", "متجر", "معرض", "تجاري", "shop", "store", "commercial"]):
            return "محل"
        if any(w in title_lower for w in ["تاون هاوس", "townhouse", "توين هاوس", "twin house"]):
            return "تاون هاوس"
        if any(w in title_lower for w in ["فيلا", "villa", "فيلات", "قصور", "قصر", "standalone"]):
            return "فيلا"
        if any(w in title_lower for w in ["دوبلكس", "duplex", "دوبليكس"]):
            return "دوبلكس"
        if any(w in title_lower for w in ["بنتهاوس", "penthouse", "بنت هاوس", "روف", "سطح"]):
            return "بنتهاوس"
        if any(w in title_lower for w in ["شاليه", "chalet", "شالية"]):
            return "شاليه"
        if any(w in title_lower for w in ["استوديو", "studio", "ستوديو"]):
            return "استوديو"
        if any(w in title_lower for w in ["مفروش", "مفروشة", "furnished"]):
            return "شقة مفروشة"
        if any(w in title_lower for w in ["شقة", "apartment", "flat"]):
            return "شقة"
        if any(w in title_lower for w in ["عمارة", "building", "بناية", "مبنى"]):
            return "عمارة"
        if any(w in title_lower for w in ["أرض", "ارض", "land", "plot"]):
            return "أرض"

        combined = f"{title} {text}".lower()
        for ptype, keywords in [
            ("مكتب", ["مكتب", "مقر إداري", "إداري"]),
            ("عيادة", ["عيادة", "طبي"]),
            ("محل", ["محل", "تجاري"]),
            ("فيلا", ["فيلا", "villa"]),
            ("دوبلكس", ["دوبلكس", "duplex"]),
            ("بنتهاوس", ["بنتهاوس", "penthouse"]),
            ("شاليه", ["شاليه", "chalet"]),
            ("شقة مفروشة", ["شقة مفروشة", "مفروش"]),
            ("شقة", ["شقة", "apartment"]),
        ]:
            if any(f" {k} " in f" {combined} " for k in keywords):
                return ptype
        return "شقة"

    async def scrape_detail_page(
        self,
        client: httpx.AsyncClient,
        url: str,
        city: str,
        listing_type: str
    ) -> Optional[Dict[str, Any]]:
        """Scrapes an individual Bayut property detail page with JSON-LD schema parsing and full metadata."""
        async with self.semaphore:
            await asyncio.sleep(random.uniform(0.3, 0.8))

            try:
                resp = await client.get(url, headers=self._get_headers(), timeout=25.0)
                if resp.status_code != 200:
                    logger.warning("bayut_detail_page_non_200", url=url, status=resp.status_code)
                    return None

                soup = BeautifulSoup(resp.content, "html.parser")
                property_id = hashlib.md5(url.encode()).hexdigest()[:16]

                title = ""
                description = ""
                price_egp = None
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
                images: List[str] = []
                agent_name = None
                agent_phone = None
                agent_whatsapp = None
                agent_type = None

                # 1. Parse JSON-LD Graph Schema
                script_tags = soup.find_all("script", type="application/ld+json")
                for script_tag in script_tags:
                    try:
                        raw_data = json.loads(script_tag.string or "")
                        nodes = raw_data.get("@graph", [raw_data]) if isinstance(raw_data, dict) else raw_data
                        if isinstance(nodes, dict):
                            nodes = [nodes]

                        for item in nodes:
                            if not isinstance(item, dict):
                                continue
                            item_type = item.get("@type", "")

                            if item_type in ["SingleFamilyResidence", "Apartment", "House", "Product", "Place", "RealEstateListing"]:
                                title = item.get("name") or item.get("description") or title
                                description = item.get("description") or description

                                if "geo" in item and isinstance(item["geo"], dict):
                                    latitude = item["geo"].get("latitude")
                                    longitude = item["geo"].get("longitude")

                                if "address" in item:
                                    if isinstance(item["address"], dict):
                                        address = item["address"].get("streetAddress") or item["address"].get("addressLocality") or ""
                                    elif isinstance(item["address"], str):
                                        address = item["address"]

                                if "offers" in item and isinstance(item["offers"], dict):
                                    offers = item["offers"]
                                    if "price" in offers:
                                        try:
                                            price_egp = float(offers["price"])
                                        except (ValueError, TypeError):
                                            pass
                                    price_currency = offers.get("priceCurrency", "EGP")

                                if "numberOfBedrooms" in item:
                                    try:
                                        bedrooms = int(item["numberOfBedrooms"])
                                    except (ValueError, TypeError):
                                        pass

                                if "numberOfBathroomsTotal" in item:
                                    try:
                                        bathrooms = int(item["numberOfBathroomsTotal"])
                                    except (ValueError, TypeError):
                                        pass

                                if "floorSize" in item and isinstance(item["floorSize"], dict):
                                    try:
                                        val = item["floorSize"].get("value")
                                        if val is not None:
                                            area_sqm = float(val)
                                    except (ValueError, TypeError):
                                        pass

                                if "image" in item:
                                    img_field = item["image"]
                                    if isinstance(img_field, list):
                                        images.extend([img for img in img_field if isinstance(img, str)])
                                    elif isinstance(img_field, str):
                                        images.append(img_field)

                            if "mainEntity" in item and isinstance(item["mainEntity"], dict):
                                main_entity = item["mainEntity"]
                                if "seller" in main_entity and isinstance(main_entity["seller"], dict):
                                    agent_info = main_entity.get("agent", {})
                                    if agent_info and isinstance(agent_info, dict):
                                        agent_name = agent_info.get("name")
                                        agent_phone = agent_info.get("telephone")
                                        agent_type = "agent"
                                    else:
                                        seller = main_entity.get("seller", {})
                                        agent_name = seller.get("name")
                                        agent_phone = seller.get("telephone")
                                        agency = seller.get("memberOf", {})
                                        agent_type = "agency" if agency.get("name") else "individual"

                                    if not agent_phone and description:
                                        wa_match = re.search(r"https://wa\.me/(\d+)", description)
                                        if wa_match:
                                            agent_whatsapp = f"+{wa_match.group(1)}"

                                    floor_number = self._extract_floor_number(description)
                                    break
                    except Exception:
                        continue

                # 2. Fallbacks
                if not title:
                    h1 = soup.select_one("h1")
                    if h1:
                        title = h1.get_text(strip=True)

                if not price_egp and description:
                    price_patterns = [
                        r"بسعر\s*:\s*([\d,]+)\s*ج",
                        r"بسعر\s*:\s*([\d,]+)",
                        r"سعر\s*:\s*([\d,]+)\s*ج",
                        r"مطلوب\s*([\d,]+)\s*ج",
                        r"(\d{4,})[,\s]*جنيهاً?",
                    ]
                    for pattern in price_patterns:
                        match = re.search(pattern, description)
                        if match:
                            price_str = match.group(1).replace(",", "")
                            try:
                                price_egp = float(price_str)
                                break
                            except Exception:
                                pass

                if not address and city:
                    address = f"{city}, مصر"

                all_text = soup.get_text(separator=" ", strip=True)
                clean_prop_type = self._determine_property_type(property_type or all_text)
                is_rent = any(w in listing_type.lower() for w in ["ايجار", "للايجار", "إيجار", "rent"])
                clean_listing_type = "ايجار" if is_rent else "تمليك"

                norm_city = "alexandria" if "إسكندرية" in city or "alex" in city.lower() else ("cairo" if "قاهرة" in city else city.lower())

                record = {
                    "id": f"bayut_{property_id}",
                    "source": "bayut",
                    "title": title or "عقار بدون عنوان",
                    "location": norm_city,
                    "listing_type": clean_listing_type,
                    "property_type": clean_prop_type,
                    "price_egp": price_egp or 0.0,
                    "price_text": f"{price_egp:,.0f} {price_currency}" if price_egp else None,
                    "currency": price_currency,
                    "bedrooms": int(bedrooms) if bedrooms else None,
                    "bathrooms": int(bathrooms) if bathrooms else None,
                    "area_sqm": area_sqm,
                    "floor_number": floor_number,
                    "address": address,
                    "latitude": float(latitude) if latitude else None,
                    "longitude": float(longitude) if longitude else None,
                    "agent_name": agent_name,
                    "agent_phone": agent_phone,
                    "agent_whatsapp": agent_whatsapp or agent_phone,
                    "agent_type": agent_type or "individual",
                    "images": images[:20],
                    "description": description or "",
                    "url": url,
                    "last_updated": last_updated,
                }

                await self._mark_url_scraped(url)
                return record

            except Exception as e:
                logger.error("bayut_detail_scrape_error", url=url, error=str(e))
                return None

    async def scrape(
        self,
        city: str = "alexandria",
        listing_type: str = "for-sale",
        max_pages: int = 2,
        dry_run: bool = False
    ) -> List[Dict[str, Any]]:
        """Executes asynchronous batch scraping for Bayut with automatic redirect handling and schema extraction."""
        mapped_city, mapped_listing = self._map_query_params(city, listing_type)
        self._log("info", "starting_async_bayut_scraping", city=mapped_city, listing_type=mapped_listing, max_pages=max_pages)
        scraped_properties: List[Dict[str, Any]] = []
        total_candidate_urls_found = 0

        limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
        async with httpx.AsyncClient(limits=limits, timeout=30.0, follow_redirects=True) as client:
            # Warm up cookie jar by hitting homepage first
            try:
                await client.get(self.base_url, headers=self._get_headers())
                await asyncio.sleep(0.5)
            except Exception:
                pass

            for page in range(1, max_pages + 1):
                candidate_urls_to_try = [
                    (
                        f"{self.base_url}/{mapped_listing}/{mapped_city}/"
                        if page == 1
                        else f"{self.base_url}/{mapped_listing}/{mapped_city}/صفحة-{page}/"
                    ),
                    (
                        f"{self.base_url}/{mapped_listing}/{mapped_city}/"
                        if page == 1
                        else f"{self.base_url}/صفحة-{page}/{mapped_listing}/{mapped_city}/"
                    ),
                    (
                        f"{self.base_url}/en/{'to-rent' if 'rent' in listing_type else 'for-sale'}/property/{city.lower()}/"
                        if page == 1
                        else f"{self.base_url}/en/{'to-rent' if 'rent' in listing_type else 'for-sale'}/property/{city.lower()}/page-{page}/"
                    )
                ]

                page_success = False
                for page_url in candidate_urls_to_try:
                    await asyncio.sleep(random.uniform(0.5, 1.2))
                    self._log("info", "fetching_bayut_directory_page", page=page, url=page_url)

                    try:
                        resp = await client.get(page_url, headers=self._get_headers())
                        if "captchaChallenge" in str(resp.url) or resp.status_code in [403, 429]:
                            self._log("warning", "bayut_waf_captcha_challenge_detected", page=page, url=str(resp.url))
                            continue

                        if resp.status_code != 200:
                            self._log("warning", "bayut_directory_page_failed", page=page, status=resp.status_code, url=page_url)
                            continue

                        soup = BeautifulSoup(resp.content, "html.parser")
                        json_ld_properties = []

                        # 1. Direct structured JSON-LD real estate listings from directory page
                        for script in soup.find_all("script", type="application/ld+json"):
                            try:
                                ld_data = json.loads(script.string or "")
                                graph = ld_data.get("@graph", [ld_data]) if isinstance(ld_data, dict) else ld_data
                                if not isinstance(graph, list):
                                    graph = [graph]
                                for node in graph:
                                    if isinstance(node, dict) and "mainEntity" in node:
                                        main_entity = node["mainEntity"]
                                        if isinstance(main_entity, dict) and "itemListElement" in main_entity:
                                            for item_wrapper in main_entity["itemListElement"]:
                                                listing_item = item_wrapper.get("item", {})
                                                entity = listing_item.get("mainEntity", {})
                                                prop_url = listing_item.get("url", "")
                                                if not prop_url:
                                                    continue

                                                total_candidate_urls_found += 1
                                                if await self._is_url_scraped(prop_url):
                                                    continue

                                                prop_id = hashlib.sha256(prop_url.encode()).hexdigest()[:16]
                                                title = str(entity.get("description", "")) or f"عقار في {city}"
                                                price = float(entity.get("price", 0.0) or 0.0)
                                                raw_cat = entity.get("accommodationCategory", "")
                                                cat = self._determine_property_type(title, raw_cat)

                                                bedrooms = entity.get("numberOfBedrooms")
                                                bathrooms = entity.get("numberOfBathroomsTotal")
                                                floor_size = entity.get("floorSize", {}).get("value") if isinstance(entity.get("floorSize"), dict) else None
                                                geo = entity.get("geo", {}) if isinstance(entity.get("geo"), dict) else {}
                                                addr = entity.get("address", {}) if isinstance(entity.get("address"), dict) else {}
                                                seller = entity.get("seller", {}) if isinstance(entity.get("seller"), dict) else {}
                                                images = entity.get("image", [])
                                                if isinstance(images, str):
                                                    images = [images]

                                                is_rent = "rent" in listing_type.lower() or "ايجار" in listing_type.lower()
                                                json_ld_properties.append({
                                                    "id": prop_id,
                                                    "source": "bayut",
                                                    "ingested_from": "web_scraping_live",
                                                    "title": title[:512],
                                                    "location": f"{city.title()} - {addr.get('addressLocality', '')}",
                                                    "listing_type": "Rent" if is_rent else "Sale",
                                                    "property_type": cat,
                                                    "price_egp": price,
                                                    "price_text": f"{price:,.0f} EGP",
                                                    "currency": "EGP",
                                                    "bedrooms": int(bedrooms) if bedrooms is not None else None,
                                                    "bathrooms": int(bathrooms) if bathrooms is not None else None,
                                                    "area_sqm": float(floor_size) if floor_size is not None else None,
                                                    "floor_number": None,
                                                    "address": f"{addr.get('addressLocality', '')}, {addr.get('addressRegion', city)}",
                                                    "latitude": float(geo.get("latitude")) if geo.get("latitude") else None,
                                                    "longitude": float(geo.get("longitude")) if geo.get("longitude") else None,
                                                    "agent_name": str(seller.get("name", "")),
                                                    "agent_phone": str(seller.get("telephone", "")),
                                                    "agent_whatsapp": str(seller.get("telephone", "")),
                                                    "agent_type": "Agency",
                                                    "images": list(images)[:10],
                                                    "description": title,
                                                    "url": prop_url,
                                                    "last_updated": datetime.utcnow().isoformat(),
                                                    "scraped_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                                                })
                                                await self._mark_url_scraped(prop_url)
                            except Exception as e:
                                self._log("debug", "json_ld_parse_skipped", error=str(e))

                        if json_ld_properties:
                            self._log("info", "extracted_live_bayut_json_ld_properties", count=len(json_ld_properties), page=page)
                            scraped_properties.extend(json_ld_properties)
                            if self.warehouse and not dry_run:
                                await self.warehouse.insert_properties(json_ld_properties)
                            page_success = True
                            break

                        # 2. Extract HTML detail links if JSON-LD not present
                        candidate_urls = []
                        for link in soup.find_all("a", href=True):
                            href = link["href"]
                            if "/تفاصيل-" in href or "/property/" in href or "details-" in href or "/عقار/" in href or "/listings/" in href:
                                if not href.startswith("http"):
                                    href = f"{self.base_url}{href}"
                                candidate_urls.append(href)

                        if not candidate_urls:
                            for card in soup.select("div[class*='listing'], div[class*='card'], article, div[data-testid*='listing'], li[role*='article']"):
                                link = card.find("a", href=True)
                                if link and link.get("href"):
                                    href = link["href"]
                                    if not href.startswith("http"):
                                        href = f"{self.base_url}{href}"
                                    candidate_urls.append(href)

                        if candidate_urls:
                            unique_urls = list(dict.fromkeys(candidate_urls))
                            total_candidate_urls_found += len(unique_urls)

                            tasks = []
                            for prop_url in unique_urls:
                                if not await self._is_url_scraped(prop_url):
                                    tasks.append(self.scrape_detail_page(client, prop_url, city, listing_type))

                            if tasks:
                                self._log("info", "dispatching_bayut_detail_page_tasks", count=len(tasks), total_in_page=len(unique_urls), page=page)
                                results = await asyncio.gather(*tasks)
                                valid_items = [item for item in results if item is not None]
                                if valid_items:
                                    scraped_properties.extend(valid_items)
                                    if self.warehouse and not dry_run:
                                        await self.warehouse.insert_properties(valid_items)
                            page_success = True
                            break

                    except Exception as e:
                        self._log("error", "error_scraping_bayut_page", page=page, error=str(e), url=page_url)

                if not page_success:
                    self._log("warning", "bayut_all_route_variations_failed_for_page", page=page)

        if not scraped_properties and total_candidate_urls_found == 0:
            error_msg = f"Live scraping returned 0 properties for Bayut {city} ({listing_type}). Check target site connectivity or blocking."
            self._log("error", error_msg)
            raise RuntimeError(error_msg)

        if not scraped_properties and total_candidate_urls_found > 0:
            self._log("info", "all_discovered_listings_already_scraped_and_up_to_date", count=total_candidate_urls_found)

        self._log("info", "bayut_scraping_completed", total_scraped=len(scraped_properties), total_discovered=total_candidate_urls_found)
        return scraped_properties