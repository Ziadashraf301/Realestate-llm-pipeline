"""
High-Performance Asynchronous AQARMAP Scraper (Enterprise M2).
Extracts complete, deep property data (specifications, images, agent details,
exact geo-coordinates, addresses) from https://aqarmap.com.eg
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


class AsyncAQARMAPScraper:
    """Enterprise Async Scraper for Egyptian Real Estate on AQARMAP (https://aqarmap.com.eg)."""

    def __init__(
        self,
        warehouse: Optional[ClickHouseWarehouseRepository] = None,
        cache_repo: Optional[RedisCacheRepository] = None,
        max_concurrency: int = 5,
        logger: Optional[Any] = None,
    ):
        self.base_url = "https://aqarmap.com.eg"
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
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Referer": "https://aqarmap.com.eg/",
        }

    async def _is_url_scraped(self, url: str) -> bool:
        """Checks Redis SET or local set to guarantee zero redundant scrapes."""
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
        """Marks URL as scraped in Redis SET and in-memory cache."""
        self.in_memory_seen_urls.add(url)
        if self.cache_repo:
            try:
                await self.cache_repo.mark_url_scraped(url)
            except Exception:
                pass

    def _determine_property_type(self, title: str, text: str = "") -> str:
        """Accurately determines property type with priority given to title and listing slug."""
        combined = f"{title} {text}".lower()

        # Priority 1: Check title explicitly for office, commercial, medical, or villa types
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
        if any(w in title_lower for w in ["شاليه", "chalet", "شالية", "كابينة"]):
            return "شاليه"
        if any(w in title_lower for w in ["استوديو", "studio", "ستوديو"]):
            return "استوديو"
        if any(w in title_lower for w in ["مفروش", "مفروشة", "furnished"]):
            return "شقة مفروشة"
        if any(w in title_lower for w in ["شقة", "apartment", "flat"]):
            return "شقة"
        if any(w in title_lower for w in ["عمارة", "building", "بناية", "مبنى"]):
            return "عمارة"
        if any(w in title_lower for w in ["أرض", "ارض", "land", "plot", "قطعة أرض"]):
            return "أرض"

        # Priority 2: Check full page text if title lacked explicit keywords
        for ptype, keywords in [
            ("مكتب", ["مكتب", "مقر إداري", "إداري", "مكاتب"]),
            ("عيادة", ["عيادة", "مركز طبي", "طبي"]),
            ("محل", ["محل", "متجر", "تجاري"]),
            ("فيلا", ["فيلا", "villa", "فيلات"]),
            ("دوبلكس", ["دوبلكس", "duplex"]),
            ("بنتهاوس", ["بنتهاوس", "penthouse", "روف"]),
            ("شاليه", ["شاليه", "chalet"]),
            ("شقة مفروشة", ["شقة مفروشة", "مفروش"]),
            ("شقة", ["شقة", "apartment"]),
        ]:
            if any(f" {k} " in f" {combined} " for k in keywords):
                return ptype

        return "شقة"

    def _extract_specifications(self, soup: BeautifulSoup, title: str, description: str) -> Dict[str, Any]:
        """Extracts bedrooms, bathrooms, area, and floor number with Arabic linguistic support."""
        specs: Dict[str, Any] = {}
        all_text = f"{title} {description} {soup.get_text(separator=' ', strip=True)}"

        specs["property_type"] = self._determine_property_type(title, all_text)

        # 1. Bedrooms extraction
        if "استوديو" in title.lower() or "استوديو" in description.lower():
            specs["bedrooms"] = 1
        elif any(w in title or w in description for w in ["غرفتين", "غرفتان", "2 غرف", "2 غرفة", "غرفتين نوم", "٢ غرفة", "٢ غرف"]):
            specs["bedrooms"] = 2
        elif any(w in title or w in description for w in ["3 غرف", "3 غرفة", "٣ غرف", "٣ غرفة", "ثلاث غرف"]):
            specs["bedrooms"] = 3
        elif any(w in title or w in description for w in ["4 غرف", "4 غرفة", "٤ غرف", "٤ غرفة", "أربع غرف", "اربع غرف"]):
            specs["bedrooms"] = 4
        elif any(w in title or w in description for w in ["5 غرف", "5 غرفة", "٥ غرف", "٥ غرفة", "خمس غرف"]):
            specs["bedrooms"] = 5
        elif any(w in title or w in description for w in ["غرفة واحدة", "غرفة نوم واحدة", "1 غرفة", "١ غرفة"]):
            specs["bedrooms"] = 1
        else:
            bed_patterns = [
                r"(?:عدد\s*الغرف|غرف\s*النوم|غرف)\s*[:\-]?\s*(\d+)",
                r"(\d+)\s*(?:غرف\b|غرفة\b|نوم\b|bedrooms?\b|beds?\b)",
            ]
            for pattern in bed_patterns:
                match = re.search(pattern, all_text, re.I)
                if match:
                    val = int(match.group(1))
                    if 1 <= val <= 20:
                        specs["bedrooms"] = val
                        break

        # 2. Bathrooms extraction
        if any(w in title or w in description for w in ["حمامين", "حمامان", "2 حمام", "2 حمامات", "٢ حمام", "٢ حمامات"]):
            specs["bathrooms"] = 2
        elif any(w in title or w in description for w in ["3 حمام", "3 حمامات", "٣ حمام", "٣ حمامات", "ثلاثة حمامات", "ثلاث حمامات"]):
            specs["bathrooms"] = 3
        elif any(w in title or w in description for w in ["حمام واحد", "1 حمام", "١ حمام"]):
            specs["bathrooms"] = 1
        else:
            bath_patterns = [
                r"(?:عدد\s*الحمامات|حمامات|حمام)\s*[:\-]?\s*(\d+)",
                r"(\d+)\s*(?:حمامات\b|حمام\b|bathrooms?\b|baths?\b)",
            ]
            for pattern in bath_patterns:
                match = re.search(pattern, all_text, re.I)
                if match:
                    val = int(match.group(1))
                    if 1 <= val <= 15:
                        specs["bathrooms"] = val
                        break

        # 3. Area (sqm) extraction
        area_patterns = [
            r"(?:المساحة|مساحة)\s*[:\-]?\s*(\d+(?:\.\d+)?)",
            r"(\d+(?:\.\d+)?)\s*(?:متر\s*مربع|م²|sqm|متر|m²)",
            r"(\d+)\s*م\b(?!\s*[\u0600-\u06FF])",
        ]
        for pattern in area_patterns:
            match = re.search(pattern, all_text, re.I)
            if match:
                try:
                    val = float(match.group(1))
                    if 15.0 <= val <= 10000.0:
                        specs["area_sqm"] = val
                        break
                except (ValueError, TypeError):
                    pass

        # 4. Floor number extraction
        floor_names = {
            "الأرضي": 0, "الارضي": 0, "أرضي": 0, "ارضي": 0,
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
        }
        for fname, fnum in floor_names.items():
            if f"الدور {fname}" in all_text or f"الطابق {fname}" in all_text or f"بالدور {fname}" in all_text:
                specs["floor_number"] = fnum
                break

        if "floor_number" not in specs:
            floor_patterns = [
                r"(?:الدور|الطابق|floor)\s*(?:ال)?(\d+)",
                r"(\d+)(?:st|nd|rd|th)?\s*floor",
            ]
            for pattern in floor_patterns:
                match = re.search(pattern, all_text, re.I)
                if match:
                    try:
                        f_val = int(match.group(1))
                        if 0 <= f_val <= 60:
                            specs["floor_number"] = f_val
                            break
                    except (ValueError, TypeError):
                        pass

        return specs

    def _extract_images(self, soup: BeautifulSoup) -> List[str]:
        """Extracts image URLs associated with listing."""
        images = []
        img_tags = soup.find_all("img", src=True)
        for img in img_tags:
            src = img.get("src", "")
            if any(x in src for x in ["aqarmap.com", "property", "listing", "slider", "thumb", "photo"]):
                if src not in images and not src.endswith(".svg") and "logo" not in src.lower():
                    images.append(src)
        return images[:20]

    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extracts address, latitude, longitude, and last updated timestamp from script payloads."""
        metadata = {
            "address": None,
            "latitude": None,
            "longitude": None,
            "last_updated": None,
        }
        html = str(soup)

        try:
            # Address
            address_patterns = [
                r'"address":\s*"([^"]+)"',
                r'\\"address\\":\s*\\"([^"]+)\\"',
                r'"location_name":\s*"([^"]+)"',
            ]
            for pattern in address_patterns:
                match = re.search(pattern, html)
                if match:
                    addr = match.group(1)
                    addr = addr.replace("\\r\\n", " ").replace("\\n", " ").replace("\\r", " ")
                    addr = addr.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
                    metadata["address"] = addr.strip()
                    break

            # Latitude
            lat_patterns = [r'"center_lat":\s*([0-9.]+)', r'\\"center_lat\\":\s*([0-9.]+)', r'"latitude":\s*([0-9.]+)']
            for pattern in lat_patterns:
                lat_match = re.search(pattern, html)
                if lat_match:
                    metadata["latitude"] = float(lat_match.group(1))
                    break

            # Longitude
            lng_patterns = [r'"center_lng":\s*([0-9.]+)', r'\\"center_lng\\":\s*([0-9.]+)', r'"longitude":\s*([0-9.]+)']
            for pattern in lng_patterns:
                lng_match = re.search(pattern, html)
                if lng_match:
                    metadata["longitude"] = float(lng_match.group(1))
                    break

            # Updated at
            updated_patterns = [r'"updated_at":\s*"([^"]+)"', r'\\"updated_at\\":\s*\\"([^"]+)\\"', r'"datePosted":\s*"([^"]+)"']
            for pattern in updated_patterns:
                match = re.search(pattern, html)
                if match:
                    metadata["last_updated"] = match.group(1)
                    break
        except Exception:
            pass

        return metadata

    def _extract_agent_info(self, soup: BeautifulSoup, description: str = "") -> Dict[str, Any]:
        """Extracts agent/broker details including phone, WhatsApp, and agency name."""
        agent_info = {"name": None, "phone": None, "whatsAppNumber": None, "type": None}
        html = str(soup)

        try:
            # 1. Phone from tel: links
            for tel_link in soup.find_all("a", href=True):
                href = tel_link["href"]
                if href.startswith("tel:"):
                    clean_phone = href.replace("tel:", "").strip()
                    if clean_phone:
                        agent_info["phone"] = clean_phone
                        break

            # 2. WhatsApp from wa.me links
            for wa_link in soup.find_all("a", href=True):
                href = wa_link["href"]
                if "wa.me/" in href:
                    match = re.search(r"wa\.me/(\d+)", href)
                    if match:
                        agent_info["whatsAppNumber"] = f"+{match.group(1)}"
                        break

            # 3. Egyptian mobile phone regex in description / HTML
            if not agent_info["phone"]:
                phone_patterns = [
                    r"(?:01[0125]\d{8})",
                    r"(?:\+201[0125]\d{8})",
                    r'"phone_number":\s*"([^"]+)"',
                    r'"phone":\s*"([^"]+)"',
                    r'"telephone":\s*"([^"]+)"',
                ]
                for pattern in phone_patterns:
                    p_match = re.search(pattern, f"{description} {html}")
                    if p_match:
                        raw_num = p_match.group(1) if "(" in pattern else p_match.group(0)
                        if raw_num and len(raw_num) >= 10:
                            agent_info["phone"] = raw_num
                            break

            if not agent_info["whatsAppNumber"] and agent_info["phone"]:
                agent_info["whatsAppNumber"] = agent_info["phone"]

            # 4. Agent / Broker Name
            name_patterns = [
                r'"full_name":\s*"([^"]+)"',
                r'"broker_name":\s*"([^"]+)"',
                r'"agent_name":\s*"([^"]+)"',
                r'"user_name":\s*"([^"]+)"',
            ]
            for pattern in name_patterns:
                name_match = re.search(pattern, html)
                if name_match:
                    agent_info["name"] = name_match.group(1)
                    break

            if not agent_info["name"]:
                agent_el = soup.select_one(".agent-name, .broker-name, [class*='agent-title'], [class*='seller-name']")
                if agent_el:
                    agent_info["name"] = agent_el.get_text(strip=True)

            # 5. User Type
            type_match = re.search(r'"user_type":\s*(\d+)', html)
            if type_match:
                u_type = int(type_match.group(1))
                agent_info["type"] = "owner" if u_type == 0 else ("agency" if u_type == 1 else "agent")
            else:
                agent_info["type"] = "agency" if agent_info["name"] else "individual"
        except Exception:
            pass

        return agent_info

    def _extract_price(self, soup: BeautifulSoup, html: str, title: str, description: str) -> tuple[float, str, str]:
        """Deep price extraction from JSON-LD, script payloads, HTML price classes, and text regex."""
        price_egp = 0.0
        price_text = ""
        price_currency = "EGP"

        # 1. JSON-LD scripts
        script_tags = soup.find_all("script", type="application/ld+json")
        for script_tag in script_tags:
            try:
                raw_ld = json.loads(script_tag.string or "")
                nodes = raw_ld.get("@graph", [raw_ld]) if isinstance(raw_ld, dict) else raw_ld
                if isinstance(nodes, dict):
                    nodes = [nodes]

                for item in nodes:
                    if not isinstance(item, dict):
                        continue
                    offers = item.get("offers")
                    if isinstance(offers, dict) and "price" in offers:
                        try:
                            p_val = float(offers["price"])
                            if p_val > 0:
                                price_egp = p_val
                                price_currency = str(offers.get("priceCurrency", "EGP"))
                                price_text = f"{price_egp:,.0f} {price_currency}"
                                return price_egp, price_text, price_currency
                        except (ValueError, TypeError):
                            pass
                    if "price" in item:
                        try:
                            p_val = float(item["price"])
                            if p_val > 0:
                                price_egp = p_val
                                price_text = f"{price_egp:,.0f} EGP"
                                return price_egp, price_text, price_currency
                        except (ValueError, TypeError):
                            pass
            except Exception:
                continue

        # 2. Embedded JSON state payloads
        json_price_patterns = [
            r'"price":\s*(\d+(?:\.\d+)?)',
            r'"listing_price":\s*(\d+(?:\.\d+)?)',
            r'"formattedPrice":\s*"([^"]+)"',
        ]
        for pattern in json_price_patterns:
            match = re.search(pattern, html)
            if match:
                raw_val = match.group(1).replace(",", "")
                try:
                    p_val = float(raw_val)
                    if p_val > 0:
                        price_egp = p_val
                        price_text = f"{price_egp:,.0f} EGP"
                        return price_egp, price_text, price_currency
                except (ValueError, TypeError):
                    pass

        # 3. HTML price selectors
        price_selectors = [
            "h2[class*='price']", "div[class*='price']", "span[class*='price']",
            ".listing-price", ".property-price", "[data-testid*='price']"
        ]
        for selector in price_selectors:
            for elem in soup.select(selector):
                txt = elem.get_text(strip=True)
                clean_num = re.sub(r"[^\d.]", "", txt.replace(",", ""))
                if clean_num:
                    try:
                        p_val = float(clean_num)
                        if p_val > 0:
                            price_egp = p_val
                            price_text = f"{price_egp:,.0f} EGP"
                            return price_egp, price_text, price_currency
                    except (ValueError, TypeError):
                        pass

        # 4. Arabic text price patterns in title & description (e.g., "75 الف", "25,000 ج.م")
        combined_text = f"{title} {description}"
        # Arabic thousands words: "75 الف" / "٧٥ الف"
        arabic_k_match = re.search(r"(\d+|[\u0660-\u0669]+)\s*(?:ألف|الف|k)\s*(?:جنيه|ج\.م|ج)?", combined_text, re.I)
        if arabic_k_match:
            raw_k = arabic_k_match.group(1)
            # convert arabic digits if present
            raw_k = raw_k.translate(str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789"))
            try:
                p_val = float(raw_k) * 1000
                if p_val > 0:
                    price_egp = p_val
                    price_text = f"{price_egp:,.0f} EGP"
                    return price_egp, price_text, price_currency
            except (ValueError, TypeError):
                pass

        # Standard currency regex: "25000 جنيه" or "25,000 ج.م"
        text_price_match = re.search(r"(\d[\d,]{3,})\s*(?:جنيه|ج\.م|ج|EGP|LE)", combined_text, re.I)
        if text_price_match:
            clean_str = text_price_match.group(1).replace(",", "")
            try:
                p_val = float(clean_str)
                if p_val > 0:
                    price_egp = p_val
                    price_text = f"{price_egp:,.0f} EGP"
                    return price_egp, price_text, price_currency
            except (ValueError, TypeError):
                pass

        return price_egp, price_text, price_currency

    async def scrape_detail_page(
        self,
        client: httpx.AsyncClient,
        url: str,
        city: str,
        listing_type: str
    ) -> Optional[Dict[str, Any]]:
        """Scrapes deep property detail page with full specs, agent info, images, and coordinates."""
        async with self.semaphore:
            await asyncio.sleep(random.uniform(0.3, 0.8))

            try:
                resp = await client.get(url, headers=self._get_headers(), timeout=25.0)
                if resp.status_code != 200:
                    logger.warning("aqarmap_detail_page_non_200", url=url, status=resp.status_code)
                    return None

                soup = BeautifulSoup(resp.content, "html.parser")
                html_str = str(soup)
                property_id = hashlib.md5(url.encode()).hexdigest()[:16]

                # 1. Title
                title = ""
                for selector in ["h1", "h2[class*='title']", ".property-title"]:
                    title_elem = soup.select_one(selector)
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        break

                # 2. Description
                description = ""
                meta_desc = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
                if meta_desc and meta_desc.get("content"):
                    description = meta_desc.get("content").strip()

                # 3. Price extraction
                price_egp, price_text, price_currency = self._extract_price(soup, html_str, title, description)

                # 4. Specifications, Images, Agent Info, and Metadata
                specs_data = self._extract_specifications(soup, title, description)
                images = self._extract_images(soup)
                agent_info = self._extract_agent_info(soup, description)
                metadata = self._extract_metadata(soup)

                clean_listing_type = "تمليك" if "sale" in listing_type.lower() else "ايجار"

                record = {
                    "id": f"aqarmap_{property_id}",
                    "source": "aqarmap",
                    "title": title or "عقار بدون عنوان",
                    "location": city.lower(),
                    "listing_type": clean_listing_type,
                    "property_type": specs_data.get("property_type", "شقة"),
                    "price_egp": price_egp,
                    "price_text": price_text or (f"{price_egp:,.0f} EGP" if price_egp else "السعر عند الطلب"),
                    "currency": price_currency,
                    "bedrooms": specs_data.get("bedrooms"),
                    "bathrooms": specs_data.get("bathrooms"),
                    "area_sqm": specs_data.get("area_sqm"),
                    "floor_number": specs_data.get("floor_number"),
                    "address": metadata.get("address") or f"{city.title()}, مصر",
                    "latitude": metadata.get("latitude"),
                    "longitude": metadata.get("longitude"),
                    "agent_name": agent_info.get("name"),
                    "agent_phone": agent_info.get("phone"),
                    "agent_whatsapp": agent_info.get("whatsAppNumber"),
                    "agent_type": agent_info.get("type"),
                    "images": images,
                    "description": description,
                    "url": url,
                    "last_updated": metadata.get("last_updated"),
                }

                await self._mark_url_scraped(url)
                return record

            except Exception as e:
                logger.error("aqarmap_detail_scrape_error", url=url, error=str(e))
                return None

    async def scrape(
        self,
        city: str = "alexandria",
        listing_type: str = "for-sale",
        max_pages: int = 2,
        dry_run: bool = False
    ) -> List[Dict[str, Any]]:
        """Executes asynchronous batch scraping with URL deduplication and ClickHouse storage."""
        self._log("info", "starting_async_aqarmap_scraping", city=city, listing_type=listing_type, max_pages=max_pages)
        scraped_properties: List[Dict[str, Any]] = []
        total_candidate_urls_found = 0

        # City route variations (e.g. direct city path, property-type, and major district slugs)
        city_slugs = [city.lower()]
        if city.lower() == "giza":
            city_slugs = ["giza", "al-giza", "new-giza", "6th-of-october", "sheikh-zayed", "dokki", "el-haram", "faisal"]
        elif city.lower() == "cairo":
            city_slugs = ["cairo", "new-cairo", "nasr-city", "maadi"]
        elif city.lower() == "alexandria":
            city_slugs = ["alexandria", "smouha", "miami"]

        limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
        async with httpx.AsyncClient(limits=limits, timeout=30.0, follow_redirects=True) as client:
            for page in range(1, max_pages + 1):
                page_found = False
                for slug in city_slugs:
                    candidate_urls_to_try = [
                        # 1. Standard Aqarmap direct route (e.g. /ar/for-sale/giza/ or /ar/for-rent/giza/)
                        f"{self.base_url}/ar/{listing_type}/{slug}/" if page == 1 else f"{self.base_url}/ar/{listing_type}/{slug}/?page={page}",
                        # 2. Property-type route (e.g. /ar/for-sale/property-type/cairo/)
                        f"{self.base_url}/ar/{listing_type}/property-type/{slug}/" if page == 1 else f"{self.base_url}/ar/{listing_type}/property-type/{slug}/?page={page}",
                        # 3. Apartment route fallback
                        f"{self.base_url}/ar/{listing_type}/apartment/{slug}/" if page == 1 else f"{self.base_url}/ar/{listing_type}/apartment/{slug}/?page={page}",
                    ]

                    for page_url in candidate_urls_to_try:
                        self._log("info", "fetching_listing_directory_page", page=page, url=page_url)

                        try:
                            resp = await client.get(page_url, headers=self._get_headers())
                            if resp.status_code != 200:
                                continue

                            soup = BeautifulSoup(resp.content, "html.parser")
                            candidate_urls = []

                            # 1. Search for all <a> tags matching strict listing URLs with numeric ID
                            for link in soup.find_all("a", href=True):
                                href = link["href"].strip()
                                if "undefined" in href or href.endswith("/property-type") or href.endswith("/property-type/"):
                                    continue

                                # Must match a listing pattern with a numerical property ID (at least 4 digits)
                                if (
                                    re.search(r"/(?:ar|en)?/?listing/\d{4,}", href)
                                    or re.search(r"/property/\d{4,}", href)
                                    or re.search(r"/(?:ar|en)?/(?:for-sale|for-rent)/[a-zA-Z0-9\-_]+/\d{4,}", href)
                                    or re.search(r"/(?:ar|en)?/(?:for-sale|for-rent)/[a-zA-Z0-9\-_]+/[a-zA-Z0-9\-_]+/\d{4,}", href)
                                ):
                                    if not href.startswith("http"):
                                        href = f"{self.base_url}{href}"
                                    candidate_urls.append(href)

                            # 2. Also search for listing-card containers as backup
                            if not candidate_urls:
                                for card in soup.select("div[class*='listing'], div[class*='card'], article, div[data-testid*='listing']"):
                                    link = card.find("a", href=True)
                                    if link and link.get("href"):
                                        href = link["href"].strip()
                                        if "undefined" not in href and re.search(r"\d{4,}", href):
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

                                self._log("info", "dispatching_detail_page_tasks", count=len(tasks), total_in_page=len(unique_urls), page=page)
                                if tasks:
                                    results = await asyncio.gather(*tasks)
                                    valid_items = [item for item in results if item is not None]
                                    scraped_properties.extend(valid_items)

                                    if self.warehouse and valid_items and not dry_run:
                                        await self.warehouse.insert_properties(valid_items)

                                page_found = True
                                break

                        except Exception as e:
                            self._log("error", "error_scraping_page_batch", page=page, error=str(e), url=page_url)

                    if page_found:
                        break

                if not page_found:
                    self._log("info", "no_more_listings_found_for_page", page=page)

        if not scraped_properties and total_candidate_urls_found == 0:
            error_msg = f"Live scraping returned 0 properties for Aqarmap {city} ({listing_type}). Check target site connectivity or blocking."
            self._log("error", error_msg)
            raise RuntimeError(error_msg)

        if not scraped_properties and total_candidate_urls_found > 0:
            self._log("info", "all_discovered_listings_already_scraped_and_up_to_date", count=total_candidate_urls_found)

        self._log("info", "aqarmap_scraping_completed", total_scraped=len(scraped_properties), total_discovered=total_candidate_urls_found)
        return scraped_properties
