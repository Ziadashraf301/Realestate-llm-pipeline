import re
from typing import Optional
import json
class TextPreprocessor:
    """Handles all text cleaning and preprocessing logic"""

    @staticmethod
    def clean_arabic_text(text: Optional[str]) -> str:
        """
        Clean Arabic real estate text:
        - Normalize Arabic letters
        - Remove emojis and special symbols
        - Keep Arabic, English, numbers
        """
        if not text:
            return ""

        text = str(text)

        # Replace bullets with spaces
        text = re.sub(r'[▪•●◼◾▫◽]', ' ', text)

        # Replace newlines/tabs
        text = re.sub(r'[\n\r\t]+', ' ', text)

        # Convert Eastern Arabic numerals to Western
        arabic_numbers = '٠١٢٣٤٥٦٧٨٩'
        english_numbers = '0123456789'
        translation_table = str.maketrans(arabic_numbers, english_numbers)
        text = text.translate(translation_table)

        # Normalization
        text = re.sub(r'[إأآا]', 'ا', text)
        text = re.sub(r'[يى]', 'ي', text)
        text = re.sub(r'[هة]\b', 'ه', text)

        # Remove unwanted symbols
        text = re.sub(r'[،/!؟💰:()+-]', '', text)

        # Keep only allowed characters
        text = re.sub(r'[^\w\s\u0600-\u06FF%.,]', '', text)

        # Collapse multiple spaces
        text = re.sub(r'\s+', ' ', text)

        return text.strip()

    def create_searchable_text(self, property_data: dict):
        """
        Create searchable text from property data.
        Combines title, address, description, location, and property type.

        Args:
            property_data (dict): Dictionary containing property fields.
            return_json (bool): If True, returns a JSON-like dict with each field cleaned.

        Returns:
            dict: JSON dict.
        """
        # Mapping for locations (can expand as needed)
        location_map = {
            "alexandria": "الاسكندرية",
            "cairo": "القاهرة"
        }

        text_parts = []
        json_output = {}

        # Title
        title = property_data.get('title', '')
        title_clean = self.clean_arabic_text(title)
        if title_clean:
            text_parts.append(title_clean)
            json_output['title'] = title_clean

        # Address
        address = property_data.get('address', '')
        address_clean = self.clean_arabic_text(address)
        if address_clean:
            text_parts.append(address_clean)
            json_output['address'] = address_clean

        # Description
        description = property_data.get('description', '')
        description_clean = self.clean_arabic_text(description)
        if description_clean:
            text_parts.append(description_clean)
            json_output['description'] = description_clean

        # Location
        location = property_data.get('location', '').lower()
        location_clean = location_map.get(location, location)
        location_clean = self.clean_arabic_text(location_clean)
        if location_clean:
            text_parts.append(location_clean)
            json_output['location'] = location_clean

        # Property type
        prop_type = property_data.get('property_type', '')
        prop_type_clean = self.clean_arabic_text(prop_type)
        if prop_type_clean:
            text_parts.append(prop_type_clean)
            json_output['property_type'] = prop_type_clean

        # combined_text = " ".join(text_parts).strip()

        return json.dumps(json_output, ensure_ascii=False)