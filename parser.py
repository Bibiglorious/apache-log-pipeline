# Step 2 - Transformation
# Parse fields using a regex-based parser (parser.py).
# Deduplicate records based on a hash of IP + timestamp + request

# Import necessary libraries
import re
import hashlib

# Parsing logs using regex and deduplicating
def parse_logs(logs):
    log_pattern = re.compile(
        r'(?P<ip>\S+) - - \[(?P<timestamp>[^\]]+)\] "(?P<method>\S+) (?P<url>\S+) (?P<protocol>[^"]+)" (?P<status>\d{3}) (?P<size>\d+|-)'
    )
    parsed_logs = []
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

            # Deduplication: Create hash based on IP + timestamp + request
            hash_input = f"{log_entry['ip']}{log_entry['timestamp']}{log_entry['method']}{log_entry['url']}"
            log_hash = hashlib.md5(hash_input.encode()).hexdigest()
                
            # Check for duplicates
            if log_hash in seen_hashes:
                duplicates_count += 1
                continue  

            # Add hash to seen set and store in log entry
            seen_hashes.add(log_hash)
            log_entry['log_hash'] = log_hash
            
            parsed_logs.append(log_entry)

    print(f"Parsed {len(parsed_logs)} logs")
    print(f"Duplicates removed: {duplicates_count}")

    return parsed_logs