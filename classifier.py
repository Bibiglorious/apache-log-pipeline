# Step 3 - Classify
# Success vs. error (status codes)
# classify logs with 200 status codes as Success 400, 500, etc as error.



 # Classifying the parsed logs as either sucess or error
def classify_logs(parsed_logs):
    classified_logs = []

    for log in parsed_logs:
        status_code = log['status']

        if 200 <= status_code < 400:
            log['classification'] = 'Success'
        elif 400 <= status_code < 600:
            log['classification'] = 'Error'
        else:
            log['classification'] = 'unknown'
        
        classified_logs.append(log)


    # Count success vs errors
    success_count = sum(1 for log in classified_logs if log['classification'] == 'Success')
    error_count = sum(1 for log in classified_logs if log['classification'] == 'Error')
    unknown_count = sum(1 for log in classified_logs if log['classification'] == 'unknown')
    print(f"Success: {success_count}, Error: {error_count}, Unknown: {unknown_count}")

    return classified_logs

classify_logs(parsed_logs)