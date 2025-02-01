#!/usr/bin/env python3
"""Script to generate test data for manual testing of the Keboola Storage Daemon."""

import csv
import gzip
import os
import random
import string
from pathlib import Path
from typing import List, Tuple

def create_directory_structure(base_dir: str) -> None:
    """Create the test directory structure."""
    directories = [
        'basic_csv',
        'different_delimiters',
        'encodings',
        'large_files',
        'malformed',
        'concurrent'
    ]
    
    for dir_name in directories:
        path = Path(base_dir) / dir_name
        path.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {path}")

def generate_row_data(num_columns: int) -> List[str]:
    """Generate random data for a CSV row."""
    return [
        ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        for _ in range(num_columns)
    ]

def create_basic_csv(base_dir: str) -> None:
    """Create basic CSV files with different structures."""
    output_dir = Path(base_dir) / 'basic_csv'
    
    # Simple CSV with header
    with open(output_dir / 'simple.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'value'])
        for i in range(100):
            writer.writerow([i, f'name_{i}', random.randint(1, 1000)])
    
    print("Created basic CSV files")

def create_delimiter_variations(base_dir: str) -> None:
    """Create CSV files with different delimiters."""
    output_dir = Path(base_dir) / 'different_delimiters'
    data = [
        ['id', 'name', 'value'],
        *[generate_row_data(3) for _ in range(50)]
    ]
    
    delimiters = {
        'comma.csv': ',',
        'semicolon.csv': ';',
        'tab.csv': '\t'
    }
    
    for filename, delimiter in delimiters.items():
        with open(output_dir / filename, 'w', newline='') as f:
            writer = csv.writer(f, delimiter=delimiter)
            writer.writerows(data)
    
    print("Created delimiter variation files")

def create_encoding_variations(base_dir: str) -> None:
    """Create CSV files with different encodings."""
    output_dir = Path(base_dir) / 'encodings'
    data = [
        ['id', 'name', 'description'],
        ['1', 'José', 'áéíóú'],
        ['2', '中文', '测试'],
        ['3', 'Русский', 'тест']
    ]
    
    # UTF-8
    with open(output_dir / 'utf8.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(data)
    
    # UTF-8 with BOM
    with open(output_dir / 'utf8_bom.csv', 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerows(data)
    
    print("Created encoding variation files")

def create_large_file(base_dir: str) -> None:
    """Create a large CSV file (>50MB)."""
    output_dir = Path(base_dir) / 'large_files'
    
    # Create a ~60MB file
    with open(output_dir / 'large.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id'] + [f'col_{i}' for i in range(20)])
        
        for i in range(200000):  # This should generate >50MB
            row = [i] + generate_row_data(20)
            writer.writerow(row)
    
    print("Created large CSV file")

def create_malformed_files(base_dir: str) -> None:
    """Create malformed CSV files for error testing."""
    output_dir = Path(base_dir) / 'malformed'
    
    # Missing columns
    with open(output_dir / 'missing_columns.csv', 'w', newline='') as f:
        f.write('header1,header2,header3\n')
        f.write('value1,value2\n')  # Missing one value
        f.write('value1,value2,value3,value4\n')  # Extra value
    
    # Invalid quotes
    with open(output_dir / 'invalid_quotes.csv', 'w', newline='') as f:
        f.write('header1,header2,header3\n')
        f.write('value1,"unclosed quote,value3\n')
    
    # Empty file
    Path(output_dir / 'empty.csv').touch()
    
    print("Created malformed CSV files")

def create_concurrent_test_files(base_dir: str) -> None:
    """Create files for testing concurrent operations."""
    output_dir = Path(base_dir) / 'concurrent'
    
    for i in range(5):
        with open(output_dir / f'concurrent_{i}.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name', 'value'])
            for j in range(1000):
                writer.writerow([j, f'name_{j}', random.randint(1, 1000)])
    
    print("Created concurrent test files")

def main():
    """Main function to generate all test data."""
    base_dir = Path('test_data')
    
    # Create base directory
    base_dir.mkdir(exist_ok=True)
    
    # Create directory structure
    create_directory_structure(base_dir)
    
    # Generate test files
    create_basic_csv(base_dir)
    create_delimiter_variations(base_dir)
    create_encoding_variations(base_dir)
    create_large_file(base_dir)
    create_malformed_files(base_dir)
    create_concurrent_test_files(base_dir)
    
    print("\nTest data generation complete!")
    print(f"Test files are located in: {base_dir.absolute()}")

if __name__ == '__main__':
    main() 