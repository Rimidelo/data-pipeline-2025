import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DataNormalizer:
    """Handles normalization of different data types into unified schema"""
    
    def normalize_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize message into unified schema"""
        try:
            data = message.get('data', {})
            metadata = message.get('metadata', {})
            
            # Determine message type
            if 'PriceFull' in str(data):
                return self.normalize_price_data(data, metadata)
            elif 'Stores' in str(data):
                return self.normalize_store_data(data, metadata)
            else:
                return self.normalize_generic_data(data, metadata)
                
        except Exception as e:
            logger.error(f"Failed to normalize message: {e}")
            raise
    
    def normalize_price_data(self, data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize price/product data"""
        normalized = {
            'type': 'price_data',
            'chain_id': data.get('ChainID'),
            'store_id': metadata.get('store_id', 'unknown'),
            'items': []
        }
        
        # Extract items from the data structure
        items_data = data.get('Items', {}).get('Item', [])
        if not isinstance(items_data, list):
            items_data = [items_data]
        
        for item in items_data:
            normalized_item = {
                'item_code': item.get('ItemCode'),
                'item_name': item.get('ItemName'),
                'item_price': float(item.get('ItemPrice', 0)),
                'item_unit': item.get('UnitQty'),
                'item_unit_measure': item.get('UnitOfMeasure'),
                'item_quantity': float(item.get('Quantity', 0)),
                'item_manufacturer': item.get('ManufacturerName'),
                'item_description': item.get('ManufacturerItemDescription'),
                'item_category': item.get('ItemCategory'),
                'item_subcategory': item.get('ItemSubCategory'),
                'item_brand': item.get('ItemBrand'),  # Will be enriched later
                'item_promotion': bool(item.get('ItemPromotion', False)),
                'item_promotion_price': float(item.get('ItemPromotionPrice', 0)),
                'item_promotion_description': item.get('ItemPromotionDescription'),
                'manufacture_country': item.get('ManufactureCountry'),
                'last_update_date': data.get('LastUpdateDate'),
                'last_update_time': data.get('LastUpdateTime')
            }
            normalized['items'].append(normalized_item)
        
        return normalized
    
    def normalize_store_data(self, data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize store data"""
        normalized = {
            'type': 'store_data',
            'chain_id': data.get('ChainID'),
            'chain_name': data.get('ChainName'),
            'stores': []
        }
        
        # Extract stores from the data structure
        sub_chains = data.get('SubChains', {}).get('SubChain', [])
        if not isinstance(sub_chains, list):
            sub_chains = [sub_chains]
        
        for sub_chain in sub_chains:
            stores_data = sub_chain.get('Stores', {}).get('Store', [])
            if not isinstance(stores_data, list):
                stores_data = [stores_data]
            
            for store in stores_data:
                normalized_store = {
                    'sub_chain_id': sub_chain.get('SubChainID'),
                    'sub_chain_name': sub_chain.get('SubChainName'),
                    'store_id': store.get('StoreID'),
                    'bikoret_no': store.get('BikoretNo'),
                    'store_type': int(store.get('StoreType', 0)),
                    'store_name': store.get('StoreName'),
                    'address': store.get('Address'),
                    'city': store.get('City'),
                    'zip_code': store.get('ZipCode'),
                    'last_update_date': data.get('LastUpdateDate'),
                    'last_update_time': data.get('LastUpdateTime')
                }
                normalized['stores'].append(normalized_store)
        
        return normalized
    
    def normalize_generic_data(self, data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize generic data"""
        return {
            'type': 'generic_data',
            'data': data,
            'metadata': metadata
        }
