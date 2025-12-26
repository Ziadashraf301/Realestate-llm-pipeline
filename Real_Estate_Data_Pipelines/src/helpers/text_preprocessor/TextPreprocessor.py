import re
from typing import Optional

class TextPreprocessor:
    """Handles all text cleaning and preprocessing logic"""
    
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
            
            # Normalize Arabic letters
            text = re.sub(r'[إأآا]', 'ا', text)
            text = re.sub(r'[يى]', 'ي', text)
            
            # Remove unwanted symbols
            text = re.sub(r'[،/!؟💰:()+-.,]', '', text)
            
            # Keep only allowed characters
            text = re.sub(r'[^\w\s\u0600-\u06FF%]', '', text)
            
            # Collapse multiple spaces
            text = re.sub(r'\s+', ' ', text)
            
            return text.strip()
    
    def create_searchable_text(self, property_data: dict) -> str:
        """
        Create searchable text from property data.
        Combines title, address, and description.
        """
        text_parts = []
        
        # Title
        if property_data.get('title'):
            text_parts.append(self.clean_arabic_text(property_data['title']))
        
        # Address
        if property_data.get('address'):
            text_parts.append(self.clean_arabic_text(property_data['address']))
        
        # Description
        if property_data.get('description'):
            text_parts.append(self.clean_arabic_text(property_data['description']))
        
        return " ".join(text_parts).strip()