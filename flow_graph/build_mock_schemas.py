"""
Script to build mock_data_schemas.json from all .ndjson files in mock_data directory.

Run this script whenever mock data files are added or updated:
    python flow_graph/build_mock_schemas.py
"""

import json
import os
import pandas as pd
from typing import Dict, Any


def infer_type(value: Any) -> str:
    """Infer the type of a value."""
    if value is None:
        return "str"
    elif isinstance(value, bool):
        return "bool"
    elif isinstance(value, int):
        return "int"
    elif isinstance(value, float):
        return "float"
    elif isinstance(value, str):
        try:
            pd.to_datetime(value)
            return "timestamp"
        except:
            return "str"
    elif isinstance(value, (list, dict)):
        return "object"
    else:
        return "str"


def flatten_keys_with_types(data: dict, parent_key: str = "", sep: str = ".") -> Dict[str, str]:
    """Flatten nested dictionary keys and infer their types."""
    columns = {}
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            columns.update(flatten_keys_with_types(v, new_key, sep=sep))
        else:
            columns[new_key] = infer_type(v)
    return columns


def build_schemas():
    """Build schemas from all mock data files."""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    mock_data_dir = os.path.join(script_dir, "mock_data")
    output_file = os.path.join(script_dir, "mock_data_schemas.json")
    
    if not os.path.exists(mock_data_dir):
        print(f"Error: Mock data directory not found: {mock_data_dir}")
        return
    
    print(f"Reading mock data from: {mock_data_dir}")
    print(f"Output file: {output_file}")
    print("-" * 60)
    
    schemas = {}
    
    # Get all .ndjson files sorted alphabetically
    ndjson_files = sorted([f for f in os.listdir(mock_data_dir) if f.endswith('.ndjson')])
    
    if not ndjson_files:
        print("Warning: No .ndjson files found in mock_data directory")
        return
    
    for filename in ndjson_files:
        file_path = os.path.join(mock_data_dir, filename)
        data_name = filename.replace('.ndjson', '')
        
        print(f"Processing '{data_name}'...", end=" ")
        
        try:
            # Load all data to capture columns from all rows
            df = pd.read_json(file_path, lines=True)
            
            # Collect all columns from all rows
            all_columns = {}
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                row_columns = flatten_keys_with_types(row_dict)
                all_columns.update(row_columns)
            
            schemas[data_name] = all_columns
            print(f"✓ {len(all_columns)} columns from {len(df)} rows")
            
        except Exception as e:
            print(f"✗ Error: {e}")
            continue
    
    # Save to JSON file with proper formatting
    print("-" * 60)
    with open(output_file, 'w') as f:
        json.dump(schemas, f, indent=2, sort_keys=True)
    
    total_columns = sum(len(cols) for cols in schemas.values())
    print(f"\n✅ Successfully generated '{os.path.basename(output_file)}'")
    print(f"   - {len(schemas)} datasets")
    print(f"   - {total_columns} total columns")
    print(f"\nDatasets: {', '.join(sorted(schemas.keys()))}")


if __name__ == "__main__":
    build_schemas()

