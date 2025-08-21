import logging
from datetime import datetime
from typing import Dict, Any
from ml_brand_extractor import MLBrandExtractor

logger = logging.getLogger(__name__)

class DataEnricher:
    """Handles enrichment of normalized data with additional fields"""
    
    def __init__(self):
        self.brand_extractor = MLBrandExtractor()
        # Check if we're in test mode
        import os
        self.test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
    
    def enrich_message(self, normalized_message: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich message with additional data"""
        try:
            if normalized_message['type'] == 'price_data':
                return self.enrich_price_data(normalized_message)
            elif normalized_message['type'] == 'promo_data':
                return self.enrich_promo_data(normalized_message)
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
        items = message.get('items', [])
        logger.info(f"Processing {len(items)} items for enrichment")
        
        # Limit processing for test mode to avoid getting stuck
        if self.test_mode:
            max_items = 20  # Process only first 20 items in test mode
            items_to_process = items[:max_items] if len(items) > max_items else items
            logger.info(f"TEST MODE: Processing only first {len(items_to_process)} items out of {len(items)} total")
            # Replace the items list with only the processed items for database save
            message['items'] = items_to_process
        else:
            items_to_process = items
        
        for i, item in enumerate(items_to_process):
            if (i + 1) % 10 == 0:  # Log progress every 10 items
                logger.info(f"Processed {i + 1}/{len(items_to_process)} items")
            
            if not item.get('item_promotion'):
                item['item_promotion'] = False
            if not item.get('item_promotion_price'):
                item['item_promotion_price'] = 0.0
            
            # Extract brand if not present or empty
            if not item.get('item_brand'):
                item = self.brand_extractor.enrich_item_with_brand(item)
                # Only log high confidence extractions to reduce spam
                if item.get('brand_confidence', 0) >= 0.7:
                    logger.info(f"Extracted brand '{item.get('item_brand')}' for item '{item.get('item_name', 'unknown')}' "
                               f"(confidence: {item.get('brand_confidence', 0):.2f})")
                elif item.get('brand_confidence', 0) >= 0.4:
                    logger.info(f"Extracted brand '{item.get('item_brand')}' for item '{item.get('item_name', 'unknown')}' "
                               f"(confidence: {item.get('brand_confidence', 0):.2f}) - {item.get('brand_extraction_method', 'unknown')}")
        
        return message
    
    def enrich_promo_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich promotion data with additional fields"""
        # Add timestamp if missing
        if not message.get('processed_at'):
            message['processed_at'] = datetime.now().isoformat()
        
        # Enrich promotions with default values
        for promotion in message.get('promotions', []):
            if not promotion.get('allow_multiple_discounts'):
                promotion['allow_multiple_discounts'] = False
            if not promotion.get('reward_type'):
                promotion['reward_type'] = '1'
        
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
