import json
import os
import struct
from json import JSONDecoder
from typing import List, Tuple, Dict

class IJSONL:
    HEADER_FORMAT = '<QQ'  # Two uint64: n and num_fields

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
        
        with open(self.header_file, 'wb') as f:
            # Write n and num_fields
            f.write(struct.pack(self.HEADER_FORMAT, n, len(all_fields)))
            
            # Write all fields with their lengths
            for field in all_fields:
                field_bytes = field.encode('utf-8')
                f.write(struct.pack('<H', len(field_bytes)))  # Write length as unsigned short
                f.write(field_bytes)


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
        fields = []
        with open(self.header_file, 'rb') as f:
            f.seek(struct.calcsize(self.HEADER_FORMAT))
            for _ in range(num_fields):
                length = struct.unpack('<H', f.read(2))[0]
                field = f.read(length).decode('utf-8')
                if field != 'RECORD':
                    fields.append(field)
        return fields



    def init_index(self, field: str):
        """Initialize an index file and a gaps file for a new field."""
        index_file = os.path.join(self.index_dir, f"{field}.index")
        gaps_file = os.path.join(self.index_dir, f"{field}.gaps")
        
        with open(index_file, 'wb') as f:
            # Truncate the file if it already exists
            f.truncate(0)
            # Write initial values for last_idx and num_entries
            f.write(struct.pack('qQ', -1, 0))
        
        # Verify the written values
        with open(index_file, 'rb') as f:
            verify_last_idx, verify_num_entries = struct.unpack('qQ', f.read(16))
        
        # Create an empty gaps file
        open(gaps_file, 'wb').close()


    def append_index(self, field: str, idx: int, start_offset: int, end_offset: int):
        """Append an index entry for a field, updating gaps if necessary."""
        index_file = os.path.join(self.index_dir, f"{field}.index")
        gaps_file = os.path.join(self.index_dir, f"{field}.gaps")
        
        with open(index_file, 'r+b') as f:
            # Check file size
            f.seek(0, 2)  # Go to the end of the file
            file_size = f.tell()
            
            # Read lead int64s
            f.seek(0)
            if file_size >= 16:
                last_idx, num_entries = struct.unpack('qQ', f.read(16))
            else:
                last_idx, num_entries = -1, 0
                f.seek(0)
                f.write(struct.pack('qQ', last_idx, num_entries))
                        
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
            new_num_entries = num_entries + 1
            f.seek(0)
            f.write(struct.pack('qQ', idx, new_num_entries))
            
            # Verify the update
            f.seek(0)
            verify_last_idx, verify_num_entries = struct.unpack('qQ', f.read(16))

    def get_index_entry(self, field: str, row_number: int) -> Tuple[int, int]:
        """Read an index entry for a field, given the row number."""
        index_file = os.path.join(self.index_dir, f"{field}.index")
        
        with open(index_file, 'rb') as f:
            last_idx, num_entries = struct.unpack('qQ', f.read(16))
            
            if row_number > last_idx:
                raise IndexError(f"Row number {row_number} out of range")
            
            index_position = self.map_index_to_row(field, row_number)
            
            f.seek(16 + index_position * 16)  # 16 bytes for lead int64s, 16 bytes per entry
            return struct.unpack('QQ', f.read(16))

    def map_index_to_row(self, field: str, index_position: int) -> int:
        """Map an index position to its actual row number, considering gaps."""
        index_file = os.path.join(self.index_dir, f"{field}.index")
        gaps_file = os.path.join(self.index_dir, f"{field}.gaps")
        
        with open(index_file, 'rb') as f:
            last_idx, num_entries = struct.unpack('qQ', f.read(16))
            
            if index_position > last_idx:
                raise IndexError(f"Index position {index_position} out of range")
        
        row_number = index_position  # Start with the assumption of no gaps
        
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
        field_positions = {'RECORD': (0, len(json_str))}
        
        def traverse(obj, path=""):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    new_path = f"{path}.{k}" if path else k
                    key_start = json_str.index(f'"{k}":', field_positions['RECORD'][0]) + len(f'"{k}":')
                    while json_str[key_start].isspace():
                        key_start += 1
                    value_start = key_start
                    try:
                        parsed, end = decoder.raw_decode(json_str[value_start:])
                        field_positions[f"RECORD.{new_path}"] = (value_start, value_start + end)
                        if isinstance(v, dict):
                            traverse(v, new_path)
                    except json.JSONDecodeError:
                        print(f"Error parsing value for key '{k}' at position {value_start}")
            # We no longer traverse into lists

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
        print(f"Adding record: {json_str}")
        
        # Append to data file
        with open(self.data_file, 'a') as f:
            start_pos = f.tell()
            f.write(json_str + '\n')
            end_pos = f.tell()

        # Increment N
        n = self.increment_n()

        # Traverse JSON and update indices
        field_positions = self.traverse_json(json_str)
        print(f"Field positions: {field_positions}")
        
        new_fields = []
        for field, (start, end) in field_positions.items():
            index_file = os.path.join(self.index_dir, f"{field}.index")
            if not os.path.exists(index_file):
                new_fields.append(field)
                self.init_index(field)
            
            if field == 'RECORD':
                self.append_index(field, n - 1, start_pos, end_pos)
            else:
                self.append_index(field, n - 1, start_pos + start, start_pos + end)

        # Update header if there are new fields
        if new_fields:
            self.update_header(n, new_fields)

        print(f"New fields: {new_fields}")
        print(f"All fields: {self.get_fields()}")


    def get_record(self, index: int, fields=None):
        """
        Get record data by index using field indices.
        
        :param index: The index of the record to retrieve.
        :param fields: None for full record, a string for a single field, or a list of fields.
        :return: The requested data (str, bytes, or dict depending on fields parameter).
        """
        if fields is None:
            start, end = self.get_index_entry('RECORD', index)
            with open(self.data_file, 'r') as f:
                f.seek(start)
                return f.read(end - start).strip()
        
        result = {}
        fields_to_fetch = [fields] if isinstance(fields, str) else fields
        
        for field in fields_to_fetch:
            full_field = f"RECORD.{field}" if field != "RECORD" else "RECORD"
            try:
                start, end = self.get_index_entry(full_field, index)
                with open(self.data_file, 'rb') as f:
                    f.seek(start)
                    field_value = f.read(end - start)
                    # Parse the field_value if it's a list or dict
                    try:
                        parsed_value = json.loads(field_value)
                        if isinstance(parsed_value, (dict, list)):
                            field_value = parsed_value
                    except json.JSONDecodeError:
                        # If it's not a valid JSON, keep it as bytes
                        pass
                    self._set_nested_dict(result, field.split('.'), field_value)
            except FileNotFoundError:
                # Field index doesn't exist, set to None
                self._set_nested_dict(result, field.split('.'), None)
        
        if isinstance(fields, str):
            return result.get(fields.split('.')[-1])
        
        return result

    def _set_nested_dict(self, d, keys, value):
        """Helper method to set value in nested dictionary."""
        for key in keys[:-1]:
            d = d.setdefault(key, {})
        d[keys[-1]] = value



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
    print(f"num fields: {num_fields}")