from extractor import fetch_logs_with_retry
from parser import parse_logs
from classifier import classify_logs
from database import create_database, save_classified_logs_to_database
from summarizer import generate_all_reports

def run_complete_pipeline():
    logs = fetch_logs_with_retry()
    parsed_logs, parsed_errors = parse_logs(logs)
    print(f"Parsed {parsed_logs}")
          
    classified_logs = classify_logs(parsed_logs)
    create_database()
    save_classified_logs_to_database(classified_logs)
    generate_all_reports()

if __name__ == "__main__":
    run_complete_pipeline()