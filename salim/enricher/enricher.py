import logging
from datetime import datetime
from typing import Dict, Any
from utils.ai_brand_extractor import AIBrandExtractor

logger = logging.getLogger(__name__)

class DataEnricher:
    """Handles enrichment of normalized data with additional fields"""
    
    def __init__(self):
        self.brand_extractor = AIBrandExtractor()
    
    def enrich_message(self, normalized_message: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich message with additional data"""
        try:
            if normalized_message['type'] == 'price_data':
                return self.enrich_price_data(normalized_message)
            elif normalized_message['type'] == 'store_data':
                return self.enrich_store_data(normalized_message)
            else:
                return normalized_message
        except Exception as e:
            logger.error(f"Failed to enrich message: {e}")
            return normalized_message
    
    def enrich_price_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich price data with additional fields"""
        # Add timestamp if missing
        if not message.get('processed_at'):
            message['processed_at'] = datetime.now().isoformat()
        
        # Enrich items with default values and brand extraction
        for item in message.get('items', []):
            if not item.get('item_promotion'):
                item['item_promotion'] = False
            if not item.get('item_promotion_price'):
                item['item_promotion_price'] = 0.0
            
            # Extract brand if not present or empty
            if not item.get('item_brand'):
                item = self.brand_extractor.enrich_item_with_brand(item)
                logger.info(f"Extracted brand '{item.get('item_brand')}' for item '{item.get('item_name', 'unknown')}' "
                           f"(confidence: {item.get('brand_confidence', 0):.2f})")
        
        return message
    
    def enrich_store_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich store data with additional fields"""
        # Add timestamp if missing
        if not message.get('processed_at'):
            message['processed_at'] = datetime.now().isoformat()
        
        # Enrich stores with default values
        for store in message.get('stores', []):
            if not store.get('store_type'):
                store['store_type'] = 1
            if not store.get('city'):
                store['city'] = 'Unknown'
        
        return message
