import logging
import psycopg2
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles database operations for storing data"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.connection = None
        self.connect()
    
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(self.database_url)
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def save_to_database(self, enriched_message: Dict[str, Any]) -> bool:
        """Save enriched message to PostgreSQL"""
        try:
            cursor = self.connection.cursor()
            
            if enriched_message['type'] == 'price_data':
                return self.save_price_data(cursor, enriched_message)
            elif enriched_message['type'] == 'store_data':
                return self.save_store_data(cursor, enriched_message)
            else:
                logger.info("Skipping generic data")
                return True
                
        except Exception as e:
            logger.error(f"Failed to save to database: {e}")
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()
    
    def save_price_data(self, cursor, message: Dict[str, Any]) -> bool:
        """Save price data to items table"""
        try:
            for item in message.get('items', []):
                # Upsert query for items table
                upsert_query = """
                INSERT INTO items (
                    chain_id, store_id, item_code, item_name, item_price,
                    item_unit, item_unit_measure, item_quantity, item_manufacturer,
                    item_category, item_subcategory, item_brand, item_promotion,
                    item_promotion_price, item_promotion_description,
                    last_update_date, last_update_time
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (chain_id, store_id, item_code, last_update_date)
                DO UPDATE SET
                    item_price = EXCLUDED.item_price,
                    item_brand = EXCLUDED.item_brand,
                    item_promotion = EXCLUDED.item_promotion,
                    item_promotion_price = EXCLUDED.item_promotion_price,
                    updated_at = CURRENT_TIMESTAMP
                """
                
                cursor.execute(upsert_query, (
                    message['chain_id'],
                    message['store_id'],
                    item['item_code'],
                    item['item_name'],
                    item['item_price'],
                    item['item_unit'],
                    item['item_unit_measure'],
                    item['item_quantity'],
                    item['item_manufacturer'],
                    item['item_category'],
                    item['item_subcategory'],
                    item['item_brand'],
                    item['item_promotion'],
                    item['item_promotion_price'],
                    item['item_promotion_description'],
                    item['last_update_date'],
                    item['last_update_time']
                ))
            
            self.connection.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to save price data: {e}")
            self.connection.rollback()
            return False
    
    def save_store_data(self, cursor, message: Dict[str, Any]) -> bool:
        """Save store data to stores table"""
        try:
            for store in message.get('stores', []):
                # Upsert query for stores table
                upsert_query = """
                INSERT INTO stores (
                    chain_id, chain_name, sub_chain_id, sub_chain_name,
                    store_id, bikoret_no, store_type, store_name,
                    address, city, zip_code, last_update_date, last_update_time
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (chain_id, store_id)
                DO UPDATE SET
                    store_name = EXCLUDED.store_name,
                    address = EXCLUDED.address,
                    last_update_date = EXCLUDED.last_update_date,
                    updated_at = CURRENT_TIMESTAMP
                """
                
                cursor.execute(upsert_query, (
                    message['chain_id'],
                    message['chain_name'],
                    store['sub_chain_id'],
                    store['sub_chain_name'],
                    store['store_id'],
                    store['bikoret_no'],
                    store['store_type'],
                    store['store_name'],
                    store['address'],
                    store['city'],
                    store['zip_code'],
                    store['last_update_date'],
                    store['last_update_time']
                ))
            
            self.connection.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to save store data: {e}")
            self.connection.rollback()
            return False
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
