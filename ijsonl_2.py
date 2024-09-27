import json
import os
import struct
from json import JSONDecoder
from typing import List, Tuple, Dict

class IJSONL:
    HEADER_FORMAT = '<QQ'  # Two uint64: n and num_fields
    FIELD_LENGTH = 256  # Fixed length for field names

    def __init__(self, filename: str):
        self.filename = filename
        self.header_file = f"{filename}_header.bin"
        self.data_file = f"{filename}_data.jsonl"
        self.index_dir = f"{filename}_indices"
        
        if not os.path.exists(self.header_file):
            self.init_header()
        if not os.path.exists(self.index_dir):
            os.makedirs(self.index_dir)

    def init_header(self):
        """Initialize the binary header file with default values."""
        with open(self.header_file, 'wb') as f:
            f.write(struct.pack(self.HEADER_FORMAT, 0, 0))  # n = 0, num_fields = 0

    def update_header(self, n: int, new_fields: List[str]):
        """Update the binary header file with new values."""
        current_fields = self.get_fields()
        all_fields = sorted(set(current_fields + new_fields))
        
        with open(self.header_file, 'r+b') as f:
            # Update n and num_fields
            f.write(struct.pack(self.HEADER_FORMAT, n, len(all_fields)))
            
            # Write all fields
            for field in all_fields:
                f.write(struct.pack(f'{self.FIELD_LENGTH}s', field.encode('utf-8')))

    def increment_n(self):
        """Increment the 'n' value in the header."""
        with open(self.header_file, 'r+b') as f:
            n, _ = struct.unpack(self.HEADER_FORMAT, f.read(struct.calcsize(self.HEADER_FORMAT)))
            f.seek(0)
            f.write(struct.pack('<Q', n + 1))
        return n + 1

    def get_header_info(self) -> Tuple[int, int]:
        """Read n and num_fields from the header."""
        with open(self.header_file, 'rb') as f:
            return struct.unpack(self.HEADER_FORMAT, f.read(struct.calcsize(self.HEADER_FORMAT)))

    def get_fields(self) -> List[str]:
        """Read and return the current fields from the header."""
        n, num_fields = self.get_header_info()
        with open(self.header_file, 'rb') as f:
            f.seek(struct.calcsize(self.HEADER_FORMAT))
            fields = []
            for _ in range(num_fields):
                field = f.read(self.FIELD_LENGTH).decode('utf-8').rstrip('\0')
                if field:
                    fields.append(field)
        return fields

    def init_index(self, field: str):
        """Initialize an index file and a gaps file for a new field."""
        index_file = os.path.join(self.index_dir, f"{field}.index")
        gaps_file = os.path.join(self.index_dir, f"{field}.gaps")
        
        with open(index_file, 'wb') as f:
            # Write initial values for last_idx and num_entries
            f.write(struct.pack('QQ', 0, 0))
        
        # Create an empty gaps file
        open(gaps_file, 'wb').close()

    def append_index(self, field: str, idx: int, start_offset: int, end_offset: int):
        """Append an index entry for a field, updating gaps if necessary."""
        index_file = os.path.join(self.index_dir, f"{field}.index")
        gaps_file = os.path.join(self.index_dir, f"{field}.gaps")
        
        with open(index_file, 'r+b') as f:
            # Read and update lead int64s
            last_idx, num_entries = struct.unpack('QQ', f.read(16))
            
            if idx != last_idx + 1:
                # Update gaps file
                gap_start = last_idx + 1
                gap_length = idx - last_idx - 1
                with open(gaps_file, 'ab') as gf:
                    gf.write(struct.pack('QQ', gap_start, gap_length))
            
            # Append new entry
            f.seek(0, 2)  # Go to end of file
            f.write(struct.pack('QQ', start_offset, end_offset))
            
            # Update lead int64s
            f.seek(0)
            f.write(struct.pack('QQ', idx, num_entries + 1))


    def get_index_entry(self, field: str, row_number: int) -> Tuple[int, int]:
        """Read an index entry for a field, given the row number."""
        index_file = os.path.join(self.index_dir, f"{field}.index")
        
        with open(index_file, 'rb') as f:
            last_idx, num_entries = struct.unpack('QQ', f.read(16))
            
            if row_number > last_idx:
                raise IndexError(f"Row number {row_number} out of range")
            
            index_position = self.map_index_to_row(field, row_number)
            
            f.seek(16 + (index_position - 1) * 16)  # 16 bytes for lead int64s, 16 bytes per entry
            return struct.unpack('QQ', f.read(16))


    def map_index_to_row(self, field: str, index_position: int) -> int:
        """Map an index position to its actual row number, considering gaps."""
        index_file = os.path.join(self.index_dir, f"{field}.index")
        gaps_file = os.path.join(self.index_dir, f"{field}.gaps")
        
        with open(index_file, 'rb') as f:
            last_idx, num_entries = struct.unpack('QQ', f.read(16))
            
            if index_position >= num_entries:
                raise IndexError(f"Index position {index_position} out of range")
        
        row_number = index_position + 1  # Start with the assumption of no gaps
        
        with open(gaps_file, 'rb') as f:
            while True:
                gap_data = f.read(16)
                if not gap_data:
                    break
                gap_start, gap_length = struct.unpack('QQ', gap_data)
                if gap_start <= row_number:
                    row_number += gap_length
                else:
                    break
        
        return row_number


    def traverse_json(self, json_str: str) -> Dict[str, Tuple[int, int]]:
        """Traverse JSON string and return field positions."""
        decoder = JSONDecoder()
        field_positions = {}
        
        def find_key_value_positions(s: str, key: str, start: int) -> Tuple[int, int, int]:
            key_pattern = f'"{key}"'
            key_start = s.find(key_pattern, start)
            if key_start == -1:
                return -1, -1, -1
            key_end = key_start + len(key_pattern)
            colon_pos = s.find(':', key_end)
            if colon_pos == -1:
                return -1, -1, -1
            value_start = colon_pos + 1
            while value_start < len(s) and s[value_start].isspace():
                value_start += 1
            return key_start, key_end, value_start

        def traverse(obj, path="", start_pos=0):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    new_path = f"{path}.{k}" if path else k
                    key_start, key_end, value_start = find_key_value_positions(json_str, k, start_pos)
                    if key_start != -1:
                        try:
                            parsed, end = decoder.raw_decode(json_str[value_start:])
                            field_positions[new_path] = (value_start, value_start + end)
                            traverse(v, new_path, value_start)
                        except json.JSONDecodeError:
                            print(f"Error parsing value for key '{k}' at position {value_start}")
                    else:
                        print(f"Key '{k}' not found in JSON string starting from position {start_pos}")
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    traverse(item, f"{path}[{i}]", start_pos)

        try:
            parsed_json, _ = decoder.raw_decode(json_str)
            traverse(parsed_json)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            print(f"JSON string: {json_str}")

        return field_positions

    def add_record(self, record: Dict):
        """Add a new record to the data file and update indices."""
        json_str = json.dumps(record)
        print(f"JSON string: {json_str}")  # Debugging
        
        # Append to data file
        with open(self.data_file, 'a') as f:
            start_pos = f.tell()
            f.write(json_str + '\n')
            end_pos = f.tell()

        # Increment N
        n = self.increment_n()

        # Traverse JSON and update indices
        field_positions = self.traverse_json(json_str)
        print(f"Field positions: {field_positions}")  # Debugging
        
        new_fields = []
        for field, (start, end) in field_positions.items():
            index_file = os.path.join(self.index_dir, f"{field}.index")
            if not os.path.exists(index_file):
                new_fields.append(field)
                self.init_index(field)
            self.append_index(field, n, start_pos + start, start_pos + end)

        # Update header if there are new fields
        if new_fields:
            self.update_header(n, new_fields)

        print(f"New fields: {new_fields}")  # Debugging
        print(f"All fields: {self.get_fields()}")  # Debugging


# Example usage and testing
if __name__ == "__main__":
    ijsonl = IJSONL("test")
    ijsonl.add_record({"name": "Alice", "age": 30})
    ijsonl.add_record({"name": "Bob", "age": 25, "address": {"city": "New York", "zip": "10001"}})
    ijsonl.add_record({"name": "Charlie", "age": 35, "hobbies": ["reading", "swimming"]})

    # Print contents of header file for verification
    print("Header:")
    n, num_fields = ijsonl.get_header_info()
    print(f"N: {n}")
    print
