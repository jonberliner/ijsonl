import io

def parse_json_positions_binary(json_data):
    positions = {}
    f = io.BytesIO(json_data)

    def read_char():
        return f.read(1)

    def peek_char():
        char = f.read(1)
        f.seek(-1, 1)  # Move back one byte
        return char

    def consume_whitespace():
        while peek_char().isspace():
            f.seek(1, 1)

    def parse_string():
        start = f.tell()
        f.seek(1, 1)  # Skip opening quote
        while True:
            char = read_char()
            if not char:
                raise ValueError("Unterminated string")
            if char == b'\\':
                # This is an escape character, skip the next character
                next_char = read_char()
                if not next_char:
                    raise ValueError("Unterminated escape sequence")
            elif char == b'"':
                # We've found an unescaped quote, end of string
                return start, f.tell()

    def parse_number():
        start = f.tell()
        while peek_char() in b'0123456789+-.eE':
            f.seek(1, 1)
        return start, f.tell()

    def parse_keyword(keyword):
        start = f.tell()
        for expected_byte in keyword.encode():
            if read_char() != bytes([expected_byte]):
                raise ValueError(f"Expected {keyword}")
        return start, f.tell()

    def parse_value(prefix=None):
        char = peek_char()
        if char == b'"':
            return parse_string()
        elif char in b'0123456789-':
            return parse_number()
        elif char == b't':
            return parse_keyword('true')
        elif char == b'f':
            return parse_keyword('false')
        elif char == b'n':
            return parse_keyword('null')
        elif char == b'{':
            return parse_struct(prefix)
        elif char == b'[':
            return parse_list(prefix)
        else:
            raise ValueError(f"Unexpected character: {char}")

    def parse_list(prefix):
        start = f.tell()
        f.seek(1, 1)  # Skip opening bracket
        consume_whitespace()
        index = 0
        while peek_char() != b']':
            new_prefix = f"{prefix}.{index}" if prefix is not None else str(index)
            value_start, value_end = parse_value(new_prefix)
            positions[new_prefix] = (value_start, value_end)
            index += 1
            consume_whitespace()
            if peek_char() == b',':
                f.seek(1, 1)  # Skip comma
                consume_whitespace()
            elif peek_char() != b']':
                raise ValueError("Expected ',' or ']'")
        f.seek(1, 1)  # Skip closing bracket
        end = f.tell()
        if prefix is not None:
            positions[prefix] = (start, end)
        return start, end

    def parse_struct(prefix):
        start = f.tell()
        f.seek(1, 1)  # Skip opening brace
        consume_whitespace()
        while peek_char() != b'}':
            key_start, key_end = parse_string()
            key = json_data[key_start+1:key_end-1].decode()
            consume_whitespace()
            if read_char() != b':':
                raise ValueError("Expected ':'")
            consume_whitespace()
            new_prefix = f"{prefix}.{key}" if prefix else key
            value_start, value_end = parse_value(new_prefix)
            positions[new_prefix] = (value_start, value_end)
            consume_whitespace()
            if peek_char() == b',':
                f.seek(1, 1)  # Skip comma
                consume_whitespace()
            elif peek_char() != b'}':
                raise ValueError("Expected ',' or '}'")
        f.seek(1, 1)  # Skip closing brace
        end = f.tell()
        if prefix is not None:
            positions[prefix] = (start, end)
        return start, end

    consume_whitespace()
    if peek_char() == b'{':
        parse_struct("")
    elif peek_char() == b'[':
        parse_list("")
    else:
        raise ValueError("JSON must start with '{' or '['")

    return positions

if __name__ == "__main__":
    import json
    # Create a test JSON with the difficult nested structure
    test_json = {
        "a": "hello\"",
        "b": {
            "c": [1, 2, {"c3": "\"{}[]\""}]
        },
        "e": False
    }

    # Write the test JSON to a file
    with open("testdata.json", 'w') as f:
        json.dump(test_json, f)

    # Now let's read and process the file
    # with open("testdata.json", 'rb') as fp:
    with open("crazy_struct.json", "rb") as fp:
        json_bytes = fp.read()
        positions = parse_json_positions_binary(json_bytes)

    # Verify the results
    print("Positions and their contents:")
    for key, (start, end) in sorted(positions.items()):
        print(f"{key}: {json_bytes[start:end].decode()}")

    # Additional verification: let's check specific nested structures
    print("\nVerifying nested structures:")
    if "b.c.2.c3" in positions:
        start, end = positions["b.c.2.c3"]
        print(f"b.c.2.c3: {json_bytes[start:end].decode()}")
    else:
        print("Nested structure b.c.2.c3 not found!")

    # Clean up: remove the test file
    import os
    os.remove("testdata.json")
