import re
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class RuleBasedBrandExtractor:
    def __init__(self):
        # Common Hebrew brands
        self.hebrew_brands = {
            'תנובה': ['תנובה', 'tnuva'],
            'אסם': ['אסם', 'osem'],
            'שטראוס': ['שטראוס', 'strauss'],
            'עלית': ['עלית', 'elit'],
            'טרה': ['טרה', 'tera'],
            'יטבתה': ['יטבתה'],
            'גד': ['גד', 'gad'],
            'תרה': ['תרה'],
            'שופרסל': ['שופרסל', 'shufersal'],
            'קוקה קולה': ['קוקה קולה', 'coca cola', 'coke'],
            'פפסי': ['פפסי', 'pepsi'],
            'נסטלה': ['נסטלה', 'nestle', 'nestlé'],
            'שוופס': ['שוופס', 'schweppes'],
            'ספרייט': ['ספרייט', 'sprite'],
            'פנטה': ['פנטה', 'fanta'],
            'מילקי': ['מילקי', 'milky'],
            'ביסלי': ['ביסלי', 'bissli'],
            'במבה': ['במבה', 'bamba'],
            'אפרסק': ['אפרסק'],
            'יפו': ['יפו', 'jaffa'],
            'פריגת': ['פריגת', 'prigat'],
            'רמי לוי': ['רמי לוי', 'rami levy'],
            'מגה': ['מגא', 'mega'],
            'יוניליוור': ['יוניליוור', 'unilever'],
            'פרוקטר': ['פרוקטר', 'procter'],
        }
        
        # English brands
        self.english_brands = {
            'Coca-Cola': ['coca cola', 'coke', 'קוקה קולה'],
            'Pepsi': ['pepsi', 'פפסי'],
            'Nestle': ['nestle', 'nestlé', 'נסטלה'],
            'Nutella': ['nutella', 'נוטלה'],
            'Kellogg': ['kellogg', 'קלוג'],
            'Heinz': ['heinz', 'היינץ'],
            'Pringles': ['pringles', 'פרינגלס'],
            'Oreo': ['oreo', 'אוראו'],
            'KitKat': ['kitkat', 'kit kat', 'קיט קט'],
            'Snickers': ['snickers', 'סניקרס'],
            'Mars': ['mars', 'מארס'],
            'Twix': ['twix', 'טוויקס'],
            'Sprite': ['sprite', 'ספרייט'],
            'Fanta': ['fanta', 'פנטה'],
            'Red Bull': ['red bull', 'רד בול'],
            'Monster': ['monster', 'מונסטר'],
        }
        
        logger.info("Rule-based brand extractor initialized")
    
    def extract_brand_with_rules(self, item_data: Dict[str, Any]) -> Dict[str, Any]:
        item_name = (item_data.get('ItemName') or item_data.get('item_name', '')).lower()
        manufacturer = (item_data.get('ManufacturerName') or item_data.get('item_manufacturer', '')).lower()
        description = (item_data.get('ManufacturerItemDescription') or item_data.get('item_description', '')).lower()
        
        # Combine all text for searching
        combined_text = f"{item_name} {manufacturer} {description}".strip()
        
        if not combined_text or combined_text in ['לא ידוע', 'unknown', '']:
            return {
                'brand': None,
                'confidence': 0.0,
                'source_field': None,
                'extraction_method': 'rule_based_no_data'
            }
        
        # Check Hebrew brands first
        for brand, patterns in self.hebrew_brands.items():
            for pattern in patterns:
                if pattern.lower() in combined_text:
                    confidence = 0.9 if pattern.lower() in item_name else 0.7
                    source = 'item_name' if pattern.lower() in item_name else 'manufacturer' if pattern.lower() in manufacturer else 'description'
                    return {
                        'brand': brand,
                        'confidence': confidence,
                        'source_field': source,
                        'extraction_method': 'rule_based_hebrew'
                    }
        
        # Check English brands
        for brand, patterns in self.english_brands.items():
            for pattern in patterns:
                if pattern.lower() in combined_text:
                    confidence = 0.9 if pattern.lower() in item_name else 0.7
                    source = 'item_name' if pattern.lower() in item_name else 'manufacturer' if pattern.lower() in manufacturer else 'description'
                    return {
                        'brand': brand,
                        'confidence': confidence,
                        'source_field': source,
                        'extraction_method': 'rule_based_english'
                    }
        
        # Try to extract from manufacturer if it's not "לא ידוע"
        if manufacturer and manufacturer not in ['לא ידוע', 'unknown', '']:
            # Clean manufacturer name
            clean_manufacturer = re.sub(r'[^\w\s]', '', manufacturer).strip()
            if len(clean_manufacturer) > 2:
                return {
                    'brand': clean_manufacturer.title(),
                    'confidence': 0.6,
                    'source_field': 'manufacturer',
                    'extraction_method': 'rule_based_manufacturer'
                }
        
        # Try to extract first meaningful word from item name
        if item_name:
            words = re.findall(r'[\w]+', item_name)
            for word in words:
                if len(word) > 2 and word not in ['ליטר', 'גרם', 'יחידה', 'חבילה', 'קופסה']:
                    return {
                        'brand': word.title(),
                        'confidence': 0.4,
                        'source_field': 'item_name',
                        'extraction_method': 'rule_based_first_word'
                    }
        
        return {
            'brand': None,
            'confidence': 0.0,
            'source_field': None,
            'extraction_method': 'rule_based_failed'
        }
    
    def enrich_item_with_brand(self, item_data: Dict[str, Any]) -> Dict[str, Any]:
        brand_info = self.extract_brand_with_rules(item_data)
        
        item_data['item_brand'] = brand_info['brand']
        item_data['brand_confidence'] = brand_info['confidence']
        item_data['brand_source'] = brand_info['source_field']
        item_data['brand_extraction_method'] = brand_info['extraction_method']
        
        return item_data
    
    def batch_extract_brands(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        enriched_items = []
        
        for i, item in enumerate(items):
            try:
                enriched_item = self.enrich_item_with_brand(item)
                enriched_items.append(enriched_item)
                
                if (i + 1) % 100 == 0:
                    logger.info(f"Processed {i + 1}/{len(items)} items")
                    
            except Exception as e:
                logger.error(f"Failed to process item {i}: {e}")
                item['item_brand'] = None
                item['brand_confidence'] = 0.0
                item['brand_source'] = None
                item['brand_extraction_method'] = 'failed'
                enriched_items.append(item)
        
        return enriched_items
    
    def get_extraction_stats(self) -> Dict[str, Any]:
        return {
            'extractor_type': 'rule_based',
            'hebrew_brands_count': len(self.hebrew_brands),
            'english_brands_count': len(self.english_brands),
            'total_patterns': sum(len(patterns) for patterns in self.hebrew_brands.values()) + 
                            sum(len(patterns) for patterns in self.english_brands.values())
        }
