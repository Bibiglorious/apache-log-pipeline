
# Step 2 - Transformation
# Parse fields using a regex-based parser (parser.py).
# Deduplicate records based on a hash of IP + timestamp + request

# Import necessary libraries
import re
import hashlib


def deduplicate_hash(log_entry, seen_hashes):
    payload = f"{log_entry['ip']}|{log_entry['timestamp']}|{log_entry['method']}|{log_entry['url']}"
    h = hashlib.md5(payload.encode("utf-8")).hexdigest()
    is_new = h not in seen_hashes
    if is_new:
        seen_hashes.add(h)
    return is_new, h

# Parsing logs using regex and deduplicating
def parse_logs(logs):
    log_pattern = re.compile(
        r'(?P<ip>\S+) - - \[(?P<timestamp>[^\]]+)\] "(?P<method>\S+) (?P<url>\S+) (?P<protocol>[^"]+)" (?P<status>\d{3}) (?P<size>\d+|-)'
    )
    parsed_logs = []
    parsed_errors = []
    seen_hashes = set()  
    duplicates_count = 0

    for line in logs:
        match = log_pattern.match(line)
        if match:
            log_entry = match.groupdict()
        

        # Checks for size field in '-', before converting to integer
            if log_entry['size'] == '-':
                log_entry['size'] = 0
            else:
                log_entry['size'] = int(log_entry['size']) 
                
            # Convert status to integers as regex captures everything as strings
            log_entry['status'] = int(log_entry['status']) 

            is_new, h = deduplicate_hash(log_entry, seen_hashes)
            if is_new:
                log_entry['log_hash'] = h
                parsed_logs.append(log_entry)
            else:
                duplicates_count += 1

        else:
            print(f"failed to parse log line: {line}")
            parsed_errors.append(line)

    print(f"Parsed {len(parsed_logs)} logs")
    print(f"Duplicates removed: {duplicates_count}")

    return parsed_logs, parsed_errors