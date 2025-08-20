import json
import os
import logging
from pathlib import Path
from collections import Counter
from rule_based_brand_extractor import RuleBasedBrandExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_products_from_json(json_file_path):
    """Load products from a JSON file"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract items from the nested structure
        items = []
        if 'Root' in data and 'Items' in data['Root'] and 'Item' in data['Root']['Items']:
            items_data = data['Root']['Items']['Item']
            # Handle both single item and list of items
            if isinstance(items_data, list):
                items = items_data
            else:
                items = [items_data]
        
        return items
    except Exception as e:
        logger.error(f"Error loading {json_file_path}: {e}")
        return []

def test_rule_based_extractor():
    """Test the rule-based brand extractor on extracted JSON files"""
    
    # Initialize extractor
    extractor = RuleBasedBrandExtractor()
    
    # Find JSON files
    extracted_files_dir = Path("../extractor/extracted_files")
    if not extracted_files_dir.exists():
        extracted_files_dir = Path("salim/extractor/extracted_files")
    
    json_files = list(extracted_files_dir.glob("*.json"))[:5]  # Test with first 5 files
    
    if not json_files:
        print("No JSON files found in extracted_files directory")
        return
    
    print(f"Testing rule-based brand extraction on {len(json_files)} files")
    print("=" * 60)
    
    all_items = []
    total_items = 0
    
    # Load items from files
    for json_file in json_files:
        print(f"Loading {json_file.name}...")
        items = load_products_from_json(json_file)
        all_items.extend(items)
        total_items += len(items)
        print(f"  Loaded {len(items)} items")
    
    print(f"\nTotal items loaded: {total_items}")
    
    if not all_items:
        print("No items found in JSON files")
        return
    
    # Test on sample items
    sample_size = min(50, len(all_items))
    sample_items = all_items[:sample_size]
    
    print(f"\nTesting brand extraction on {sample_size} sample items")
    print("-" * 50)
    
    extraction_results = []
    method_counts = Counter()
    confidence_levels = []
    brands_found = Counter()
    
    for i, item in enumerate(sample_items):
        brand_info = extractor.extract_brand_with_rules(item)
        extraction_results.append(brand_info)
        
        method_counts[brand_info['extraction_method']] += 1
        confidence_levels.append(brand_info['confidence'])
        
        if brand_info['brand']:
            brands_found[brand_info['brand']] += 1
        
        # Show first 10 examples
        if i < 10:
            print(f"\nItem {i+1}:")
            print(f"  Name: {item.get('ItemName', 'N/A')}")
            print(f"  Manufacturer: {item.get('ManufacturerName', 'N/A')}")
            print(f"  Extracted Brand: {brand_info['brand'] or 'None'}")
            print(f"  Confidence: {brand_info['confidence']:.2f}")
            print(f"  Method: {brand_info['extraction_method']}")
            print(f"  Source: {brand_info['source_field'] or 'N/A'}")
    
    # Show statistics
    print(f"\n\nExtraction Statistics")
    print("=" * 40)
    print(f"Total items tested: {sample_size}")
    print(f"Successful extractions: {sum(1 for r in extraction_results if r['brand'])}")
    print(f"Success rate: {sum(1 for r in extraction_results if r['brand'])/sample_size*100:.1f}%")
    print(f"Average confidence: {sum(confidence_levels)/len(confidence_levels):.2f}")
    
    print(f"\nExtraction Methods:")
    for method, count in method_counts.most_common():
        print(f"  {method}: {count} ({count/sample_size*100:.1f}%)")
    
    print(f"\nTop Brands Found:")
    for brand, count in brands_found.most_common(10):
        print(f"  {brand}: {count}")
    
    # Show extractor configuration
    print(f"\nExtractor Configuration:")
    stats = extractor.get_extraction_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Test batch processing
    print(f"\nTesting Batch Processing")
    print("-" * 30)
    batch_items = sample_items[:10]
    enriched_items = extractor.batch_extract_brands(batch_items)
    print(f"Batch processed {len(enriched_items)} items")
    
    for item in enriched_items[:5]:  # Show first 5
        print(f"  {item['ItemName']} -> {item.get('item_brand', 'None')}")

if __name__ == "__main__":
    test_rule_based_extractor()
