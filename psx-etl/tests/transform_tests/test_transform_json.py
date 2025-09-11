#!/usr/bin/env python3
"""
Test script to verify the transform service handles JSON serialization correctly
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'services', 'transform'))

import json
import numpy as np
import pandas as pd
from app import transform_data, ensure_json_serializable, safe_float

# Sample test data with potential problematic values
test_data = {
    "raw_data": [
        {
            "Ticker": "AAPL",
            "Date": "2024-01-01",
            "Open": 150.0,
            "High": 155.0,
            "Low": 149.0,
            "Close": 154.0,
            "Volume": 1000000,
            "Dividend": 0.0,
            "industry": "Technology",
            "sector": "Technology",
            "marketCap": 3000000000000,
            "trailingPE": 25.5,
            "forwardPE": 22.0,
            "dividendYield": 0.5,
            "averageVolume": 50000000
        },
        {
            "Ticker": "AAPL", 
            "Date": "2024-01-02",
            "Open": 154.0,
            "High": 158.0,
            "Low": 153.0,
            "Close": 157.0,
            "Volume": 1200000,
            "Dividend": 0.0,
            "industry": "Technology",
            "sector": "Technology", 
            "marketCap": 3000000000000,
            "trailingPE": 25.5,
            "forwardPE": 22.0,
            "dividendYield": 0.5,
            "averageVolume": 50000000
        }
    ]
}

def test_safe_float():
    """Test the safe_float function with edge cases"""
    print("Testing safe_float function...")
    
    # Test cases
    test_cases = [
        (5.0, 5.0),
        (np.nan, None),
        (np.inf, None),
        (-np.inf, None),
        (None, None),
        ("not_a_number", None),
        (np.float64(3.14159), 3.1416),
        (float('inf'), None),
        (float('nan'), None)
    ]
    
    for input_val, expected in test_cases:
        result = safe_float(input_val)
        print(f"  Input: {input_val} -> Output: {result} (Expected: {expected})")
        if expected is None:
            assert result is None or result == 0.0, f"Failed for {input_val}"
        else:
            assert abs(result - expected) < 0.0001, f"Failed for {input_val}"
    
    print("✓ safe_float tests passed")

def test_json_serializable():
    """Test the ensure_json_serializable function"""
    print("Testing ensure_json_serializable function...")
    
    # Test data with problematic values
    test_obj = {
        "normal_float": 3.14,
        "nan_value": np.nan,
        "inf_value": np.inf,
        "numpy_int": np.int64(42),
        "numpy_float": np.float64(2.718),
        "nested_dict": {
            "another_nan": float('nan'),
            "list_with_nan": [1, 2, np.nan, 4]
        }
    }
    
    result = ensure_json_serializable(test_obj)
    
    # Try to serialize to JSON
    json_str = json.dumps(result)
    print(f"  Serialized JSON: {json_str}")
    
    # Parse back to verify
    parsed = json.loads(json_str)
    print("✓ JSON serialization tests passed")

def test_transform_service():
    """Test the full transform service"""
    print("Testing transform service...")
    
    try:
        result = transform_data(test_data)
        
        # Try to serialize the result to JSON
        json_str = json.dumps(result)
        print(f"  Transform successful, JSON size: {len(json_str)} characters")
        
        # Verify structure
        assert result["success"] == True
        assert "data" in result
        assert len(result["data"]) > 0
        
        print("✓ Transform service tests passed")
        
    except Exception as e:
        print(f"✗ Transform service test failed: {e}")
        raise

if __name__ == "__main__":
    print("Running JSON compliance tests for transform service...\n")
    
    test_safe_float()
    print()
    
    test_json_serializable()
    print()
    
    test_transform_service()
    print()
    
    print("All tests completed successfully! 🎉")