import sqlite3
import json
from datetime import datetime

# Generate summary reports from database
def generate_all_reports():
    
    # connecting to the database where the logs are stored
    conn = sqlite3.connect('apache_logs.db')
    cursor = conn.cursor()
    
    # Count total number of logs in the database
    cursor.execute("SELECT COUNT(*) FROM logs")
    total = cursor.fetchone()[0]
    
    # Count how many logs were successful
    cursor.execute("SELECT COUNT(*) FROM logs WHERE classification = 'Success'")
    success = cursor.fetchone()[0]
    
    #  Count how many logs were errors
    cursor.execute("SELECT COUNT(*) FROM logs WHERE classification = 'Error'")
    error = cursor.fetchone()[0]
    
    # top 5 IP addresses that made the most requests
    cursor.execute("""
        SELECT ip, COUNT(*) as count 
        FROM logs 
        GROUP BY ip 
        ORDER BY count DESC 
        LIMIT 5
    """)
    top_ips = cursor.fetchall()
    
    conn.close()
    
    # Create summary report data structure
    summary = {
        'report_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_logs': total,
        'success_count': success,
        'error_count': error,
        'top_ips': [{'ip': ip, 'count': count} for ip, count in top_ips]
    }
    
    # Save report as JSON file for external use
    filename = f"daily_report_{datetime.now().strftime('%Y%m%d')}.json"
    with open(filename, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"Report generated: {filename}")
    print(f"Total: {total}, Success: {success}, Error: {error}")
    
    return summary

if __name__ == "__main__":
    generate_all_reports()