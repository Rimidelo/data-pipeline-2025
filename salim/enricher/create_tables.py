import os
import psycopg2
from dotenv import load_dotenv

def create_tables():
    """Create both stores and items tables"""
    
    load_dotenv()
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("DATABASE_URL not found in .env file")
        return False
    
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Create stores table
        print("Creating stores table...")
        stores_sql = """
        CREATE TABLE IF NOT EXISTS stores (
            id SERIAL PRIMARY KEY,
            chain_id VARCHAR(50) NOT NULL,
            chain_name VARCHAR(255) NOT NULL,
            sub_chain_id VARCHAR(50),
            sub_chain_name VARCHAR(255),
            store_id VARCHAR(50) NOT NULL,
            bikoret_no VARCHAR(10),
            store_type INTEGER,
            store_name VARCHAR(255) NOT NULL,
            address TEXT,
            city VARCHAR(100),
            zip_code VARCHAR(20),
            last_update_date DATE,
            last_update_time TIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chain_id, store_id)
        );
        """
        cursor.execute(stores_sql)
        
        # Create stores indexes
        stores_indexes = """
        CREATE INDEX IF NOT EXISTS idx_stores_chain_id ON stores(chain_id);
        CREATE INDEX IF NOT EXISTS idx_stores_store_id ON stores(store_id);
        CREATE INDEX IF NOT EXISTS idx_stores_chain_store ON stores(chain_id, store_id);
        """
        cursor.execute(stores_indexes)
        
        # Create items table
        print("Creating items table...")
        items_sql = """
        CREATE TABLE IF NOT EXISTS items (
            id SERIAL PRIMARY KEY,
            chain_id VARCHAR(50) NOT NULL,
            store_id VARCHAR(50) NOT NULL,
            item_code VARCHAR(50) NOT NULL,
            item_name VARCHAR(500) NOT NULL,
            item_price DECIMAL(10,2),
            item_unit VARCHAR(50),
            item_unit_measure VARCHAR(50),
            item_quantity DECIMAL(10,3),
            item_manufacturer VARCHAR(255),
            item_category VARCHAR(255),
            item_subcategory VARCHAR(255),
            item_brand VARCHAR(255),
            item_promotion BOOLEAN DEFAULT FALSE,
            item_promotion_price DECIMAL(10,2),
            item_promotion_description TEXT,
            last_update_date DATE,
            last_update_time TIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chain_id, store_id, item_code, last_update_date)
        );
        """
        cursor.execute(items_sql)
        
        # Create items indexes
        items_indexes = """
        CREATE INDEX IF NOT EXISTS idx_items_chain_id ON items(chain_id);
        CREATE INDEX IF NOT EXISTS idx_items_store_id ON items(store_id);
        CREATE INDEX IF NOT EXISTS idx_items_item_code ON items(item_code);
        CREATE INDEX IF NOT EXISTS idx_items_chain_store ON items(chain_id, store_id);
        CREATE INDEX IF NOT EXISTS idx_items_price ON items(item_price);
        CREATE INDEX IF NOT EXISTS idx_items_category ON items(item_category);
        CREATE INDEX IF NOT EXISTS idx_items_brand ON items(item_brand);
        """
        cursor.execute(items_indexes)
        
        conn.commit()
        print("Tables created successfully!")
        
        # Verify tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name IN ('stores', 'items')
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        if 'stores' in tables and 'items' in tables:
            print("Both tables verified!")
        else:
            print("Some tables were not created")
            return False
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Failed to create tables: {e}")
        return False

if __name__ == "__main__":
    success = create_tables()
    if success:
        print("Database setup complete.")
    else:
        print("Database setup failed.")
