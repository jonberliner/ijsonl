import io

def parse_json_positions_binary(json_bytes):
    positions = {}
    f = io.BytesIO(json_bytes)

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
            if char == b'"' and peek_char() != b'\\':
                return start, f.tell()
            if not char:
                raise ValueError("Unterminated string")

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

    def parse_value():
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
        else:
            raise ValueError(f"Unexpected character: {char}")

    def parse_list(prefix):
        start = f.tell()
        f.seek(1, 1)  # Skip opening bracket
        consume_whitespace()
        index = 0
        while peek_char() != b']':
            if peek_char() == b'{':
                parse_struct(f"{prefix}.{index}")
            elif peek_char() == b'[':
                parse_list(f"{prefix}.{index}")
            else:
                value_start, value_end = parse_value()
                positions[f"{prefix}.{index}"] = (value_start, value_end)
            index += 1
            consume_whitespace()
            if peek_char() == b',':
                f.seek(1, 1)  # Skip comma
                consume_whitespace()
            elif peek_char() != b']':
                raise ValueError("Expected ',' or ']'")
        f.seek(1, 1)  # Skip closing bracket
        positions[prefix] = (start, f.tell())

    def parse_struct(prefix):
        start = f.tell()
        f.seek(1, 1)  # Skip opening brace
        consume_whitespace()
        while peek_char() != b'}':
            key_start, key_end = parse_string()
            key = json_bytes[key_start+1:key_end-1].decode()
            consume_whitespace()
            if read_char() != b':':
                raise ValueError("Expected ':'")
            consume_whitespace()
            if peek_char() == b'{':
                parse_struct(f"{prefix}.{key}" if prefix else key)
            elif peek_char() == b'[':
                parse_list(f"{prefix}.{key}" if prefix else key)
            else:
                value_start, value_end = parse_value()
                positions[f"{prefix}.{key}" if prefix else key] = (value_start, value_end)
            consume_whitespace()
            if peek_char() == b',':
                f.seek(1, 1)  # Skip comma
                consume_whitespace()
            elif peek_char() != b'}':
                raise ValueError("Expected ',' or '}'")
        f.seek(1, 1)  # Skip closing brace
        positions[prefix] = (start, f.tell())

    consume_whitespace()
    if peek_char() == b'{':
        parse_struct("")
    elif peek_char() == b'[':
        parse_list("")
    else:
        raise ValueError("JSON must start with '{' or '['")

    return positions


# Test the function
json_str = '{"a": "hello", "b": {"c": [1, 2, {"d": null}]}, "e": false}'
json_bytes = json_str.encode('utf-8')
positions = parse_json_positions_binary(json_bytes)

# Verify the results
for key, (start, end) in sorted(positions.items()):
    print(f"key: {key}, start: {start}, end: {end}")
    print(f"{key}: {json_bytes[start:end].decode()}")
