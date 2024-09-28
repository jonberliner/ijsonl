import json
import os
import struct
from json import JSONDecoder
from typing import List, Tuple, Dict

from parse_json_str import parse_json_positions_binary
import io

class IJSONL:
    HEADER_FORMAT = '<QQ'  # Two uint64: n and num_fields

    def __init__(self, filename: str):
        self.filename = filename if filename.endswith('.ijsonl') else filename + '.ijsonl'
        os.makedirs(self.filename, exist_ok=True)
        self.header_file = os.path.join(self.filename, f"header.bin")
        self.data_file = os.path.join(self.filename, f"data.jsonl")
        self.index_dir = os.path.join(self.filename, f"indices")
        
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
                if field != '__RECORD__':
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
        index_file = os.path.join(self.index_dir, f"{field}.index")
        
        with open(index_file, 'r+b') as f:
            # Read current header
            f.seek(0)
            header = f.read(16)
            if len(header) == 16:
                last_idx, num_entries = struct.unpack('QQ', header)
            else:
                print(f"Warning: Index file for {field} has incomplete header")
                last_idx, num_entries = -1, 0

            print(f"Appending to index for {field}: idx={idx}, last_idx={last_idx}, num_entries={num_entries}")

            # Update header
            f.seek(0)
            f.write(struct.pack('QQ', idx, num_entries + 1))

            # Append new entry
            f.seek(0, 2)  # Go to end of file
            f.write(struct.pack('QQ', start_offset, end_offset))

            print(f"Updated index for {field}: new last_idx={idx}, new num_entries={num_entries + 1}")


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
        decoder = JSONDecoder()
        field_positions = {'__RECORD__': (0, len(json_str))}
        
        def find_key_value_bounds(s: str, key: str, start: int) -> Tuple[int, int, int]:
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
            value_end = value_start
            stack = []
            while value_end < len(s):
                if s[value_end] in '{[':
                    stack.append(s[value_end])
                elif s[value_end] in '}]':
                    if stack and ((s[value_end] == '}' and stack[-1] == '{') or (s[value_end] == ']' and stack[-1] == '[')):
                        stack.pop()
                    if not stack:
                        value_end += 1
                        break
                elif not stack and s[value_end] in ',}]':
                    break
                value_end += 1
            return key_start, value_start, value_end

        def traverse(obj, path=""):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    new_path = f"{path}.{k}" if path else k
                    key_start, value_start, value_end = find_key_value_bounds(json_str, k, field_positions['__RECORD__'][0])
                    if key_start != -1:
                        field_positions[new_path] = (value_start, value_end)
                        # Only continue traversing if the value is also a dictionary
                        if isinstance(v, dict):
                            traverse(v, new_path)
            # We don't traverse into lists or other types

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
        field_positions = parse_json_positions_binary(
                json_str.encode('utf-8'))
        field_positions["__RECORD__"] = field_positions[""]
        print(f"Field positions: {field_positions}")
        
        new_fields = []
        for field, (start, end) in field_positions.items():
            index_file = os.path.join(self.index_dir, f"{field}.index")
            if not os.path.exists(index_file):
                new_fields.append(field)
                self.init_index(field)
            
            if field == '__RECORD__':
                self.append_index(field, n - 1, start_pos, end_pos)
            else:
                self.append_index(field, n - 1, start_pos + start, start_pos + end)
        # Update header if there are new fields
        if new_fields:
            self.update_header(n, new_fields)

        print(f"New fields: {new_fields}")
        print(f"All fields: {self.get_fields()}")


    def get_record(self, index: int, fields=None):
        if fields is None:
            start, end = self.get_index_entry('__RECORD__', index)  # Changed from 'RECORD' to '__RECORD__'
            with open(self.data_file, 'r') as f:
                f.seek(start)
                return f.read(end - start).strip()
        
        result = {}
        fields_to_fetch = [fields] if isinstance(fields, str) else fields
        
        for field in fields_to_fetch:
            try:
                start, end = self.get_index_entry(field, index)
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

    def get_record(self, index: int, fields=None):
        """
        Get record data by index using field indices.
        
        :param index: The index of the record to retrieve.
        :param fields: None for full record, a string for a single field, or a list of fields.
        :return: The requested data (str, bytes, or dict depending on fields parameter).
        """
        if fields is None:
            # Retrieve full record
            start, end = self.get_index_entry('__RECORD__', index)
            with open(self.data_file, 'rb') as f:
                f.seek(start)
                return f.read(end - start)
        
        is_str = isinstance(fields, str)
        if is_str:
            fields = [fields]
        if isinstance(fields, list):
            # Retrieve multiple fields
            result = {}
            for field in fields:
                try:
                    start, end = self.get_index_entry(field, index)
                    with open(self.data_file, 'rb') as f:
                        f.seek(start)
                        result[field] = f.read(end - start)
                except FileNotFoundError:
                    # Field index doesn't exist, set to None
                    result[field] = None
            return result
        if is_str:
            result = result[fields[0]]
        
        raise ValueError("Fields must be None, a string, or a list of strings")



# Example usage and testing
if __name__ == "__main__":
    # Initialize IJSONL
    ijsonl = IJSONL("test_data")

    # Test data with nested structures and varying fields
    test_records = [
        {
            "name": "Alice",
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "Wonderland",
                "zip": "12345"
            },
            "hobbies": ["reading", "painting"],
            "family": {
                "spouse": "Bob",
                "children": [
                    {"name": "Charlie", "age": 5},
                    {"name": "Diana", "age": 3}
                ]
            }
        },
        {
            "name": "Eve",
            "age": 28,
            "skills": ["hacking", "cryptography"],
            "job": {
                "title": "Security Analyst",
                "company": {
                    "name": "Tech Corp",
                    "location": "Cyberspace"
                }
            }
        },
        {
            "name": "Mallory",
            "pets": [
                {"type": "cat", "name": "Whiskers"},
                {"type": "dog", "name": "Fido"}
            ],
            "education": {
                "degree": "Ph.D",
                "field": "Computer Science",
                "university": {
                    "name": "Tech University",
                    "location": "Silicon Valley"
                }
            }
        }
    ]

    # Add records
    for record in test_records:
        ijsonl.add_record(record)

    print("Testing get_record method:")

    # Test getting full records
    print("\nFull Records:")
    for i in range(3):
        print(f"Record {i}:", ijsonl.get_record(i))

    # Test getting single fields
    print("\nSingle Fields:")
    print("Name (Record 0):", ijsonl.get_record(0, "name"))
    print("Age (Record 1):", ijsonl.get_record(1, "age"))
    print("Pets (Record 2):", ijsonl.get_record(2, "pets"))

    # Test getting nested fields
    print("\nNested Fields:")
    print("Address.City (Record 0):", ijsonl.get_record(0, "address.city"))
    print("Job.Company.Name (Record 1):", ijsonl.get_record(1, "job.company.name"))
    print("Education.University.Location (Record 2):", ijsonl.get_record(2, "education.university.location"))

    # Test getting multiple fields
    print("\nMultiple Fields:")
    print("Name and Age (Record 0):", ijsonl.get_record(0, ["name", "age"]))
    print("Skills and Job.Title (Record 1):", ijsonl.get_record(1, ["skills", "job.title"]))
    print("Name and Pets[0].Name (Record 2):", ijsonl.get_record(2, ["name", "pets.0.name"]))

    # Test getting non-existent fields
    print("\nNon-existent Fields:")
    print("Non-existent field (Record 0):", ijsonl.get_record(0, "non_existent"))
    print("Multiple fields including non-existent (Record 1):", ijsonl.get_record(1, ["name", "non_existent", "age"]))

    print("\nTesting complete.")

    import shutil
    shutil.rmtree("test_data.ijsonl")