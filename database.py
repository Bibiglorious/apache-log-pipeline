import sqlite3

def create_database():
    """
    Create SQLite database and logs table
    """
    conn = sqlite3.connect('apache_logs.db')
    cursor = conn.cursor()
    
    # Create logs table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY,
            ip TEXT,
            timestamp TEXT,
            method TEXT,
            url TEXT,
            status INTEGER,
            size INTEGER,
            classification TEXT,
            log_hash TEXT UNIQUE
        )
    ''')
    
    # Create indexes for faster queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON logs(status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_classification ON logs(classification)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ip ON logs(ip)')
    
    conn.commit()
    conn.close()
    print("Database created")

def save_classified_logs_to_database(classified_logs):
    """
    Save classified logs to the database
    """
    if not classified_logs:
        print("No logs to save")
        return 0
    
    conn = sqlite3.connect('apache_logs.db')
    cursor = conn.cursor()
    
    saved_count = 0
    duplicate_count = 0
    
    for log in classified_logs:
        try:
            cursor.execute('''
                INSERT INTO logs (ip, timestamp, method, url, status, size, classification, log_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                log['ip'],
                log['timestamp'],
                log['method'],
                log['url'],
                log['status'],
                log['size'],
                log['classification'],
                log['log_hash']
            ))
            saved_count += 1
            
        except sqlite3.IntegrityError:
            duplicate_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"Saved {saved_count} logs to database")
    print(f"Skipped {duplicate_count} duplicates")
    
    return saved_count

def get_database_summary():
    """
    Get summary statistics from database
    """
    conn = sqlite3.connect('apache_logs.db')
    cursor = conn.cursor()
    
    # Count total logs
    cursor.execute("SELECT COUNT(*) FROM logs")
    total = cursor.fetchone()[0]
    
    # Count success logs
    cursor.execute("SELECT COUNT(*) FROM logs WHERE classification = 'Success'")
    success = cursor.fetchone()[0]
    
    # Count error logs
    cursor.execute("SELECT COUNT(*) FROM logs WHERE classification = 'Error'")
    error = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"Database Summary:")
    print(f"  Total logs: {total}")
    print(f"  Success: {success}")
    print(f"  Error: {error}")
    
    return {'total': total, 'success': success, 'error': error}

# For testing this file directly
if __name__ == "__main__":
    print("Testing database operations...")