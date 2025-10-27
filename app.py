import threading
import time

from flask import Flask, render_template, request, jsonify, make_response
from ScraperLogic import WebScraperCore  # Import your core logic
import uuid
import pandas as pd
import json
import os
from urllib.parse import urlparse
import re

app = Flask(__name__)
# Global dictionary to manage all scraping jobs and their data
# {job_id: {status: str, progress: int, data: dict, instance: WebScraperCore, log: list}}
scrape_jobs = {}
SCRAPE_DIR = 'scraped_results'
os.makedirs(SCRAPE_DIR, exist_ok=True)


def update_job_status(job_id, status, pages_scraped, scraped_data, log_message=None):
    """Thread-safe function to update the global job status."""
    job = scrape_jobs.get(job_id)
    if not job:
        return

    job['status'] = status
    job['progress'] = pages_scraped
    job['data'] = scraped_data  # Store the scraped data
    if log_message:
        job['log'].append(f"[{time.strftime('%H:%M:%S')}] {log_message}")


@app.route('/')
def index():
    """Serves the main HTML page."""
    return render_template('index.html')


@app.route('/start_scrape', methods=['POST'])
def start_scrape():
    """Endpoint to start the scraping process in a new thread."""
    try:
        data = request.get_json()
        start_url = data['url']

        # Input Validation and Parsing
        max_pages = int(data.get('max_pages', 10))
        delay = float(data.get('delay', 1))
        tags = data.get('tags', ['h1', 'p'])
        structured = data.get('structured', False)

        job_id = str(uuid.uuid4())

        # Initialize the job tracking dictionary
        scrape_jobs[job_id] = {
            'status': 'STARTING',
            'progress': 0,
            'max_pages': max_pages,
            'data': {},
            'instance': None,
            'log': []
        }

        # Create and run the scraper instance in a thread
        scraper = WebScraperCore(start_url, max_pages, delay, tags, structured, job_id, update_job_status)
        scrape_jobs[job_id]['instance'] = scraper

        thread = threading.Thread(target=scraper.crawl_and_scrape)
        thread.start()

        return jsonify({"message": "Scraping started successfully", "job_id": job_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/stop_scrape/<job_id>', methods=['POST'])
def stop_scrape(job_id):
    """Endpoint to stop a running scraping job."""
    job = scrape_jobs.get(job_id)
    if job and job['instance']:
        job['instance'].stop_scraping()
        return jsonify({"message": f"Stop signal sent to job {job_id}"})
    return jsonify({"error": "Job not found or already finished"}), 404


@app.route('/status/<job_id>', methods=['GET'])
def get_status(job_id):
    """Endpoint for the frontend to poll for job status, progress, and logs."""
    job = scrape_jobs.get(job_id)
    if not job:
        return jsonify({"status": "NOT_FOUND"}), 404

    # Send a copy of the log and clear the log list for the next poll cycle
    current_log = job['log'][:]
    job['log'] = []  # Clear for next poll

    return jsonify({
        "status": job['status'],
        "pages_scraped": job['progress'],
        "max_pages": job['max_pages'],
        "log": current_log,
        "total_urls_scraped": len(job['data'])
    })


@app.route('/download/<job_id>/<file_format>', methods=['GET'])
def download_data(job_id, file_format):
    """Endpoint to save and download the scraped data."""
    job = scrape_jobs.get(job_id)
    if not job or not job['data']:
        return jsonify({"error": "No data available for this job"}), 404

    data = job['data']
    start_url = job['instance'].start_url
    domain = urlparse(start_url).netloc
    domain_prefix = re.sub(r'[^\w\-]', '_', domain)

    if file_format == 'csv':
        df = pd.DataFrame([
            {'URL': url, 'Text': d['text'], 'Tables': json.dumps(d['tables']), 'Metadata': json.dumps(d['metadata'])}
            for url, d in data.items()
        ])
        filepath = os.path.join(SCRAPE_DIR, f"{domain_prefix}_{job_id}_output.csv")
        df.to_csv(filepath, index=False, encoding='utf-8')
        mimetype = 'text/csv'
    elif file_format == 'json':
        filepath = os.path.join(SCRAPE_DIR, f"{domain_prefix}_{job_id}_output.json")
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        mimetype = 'application/json'
    else:
        return jsonify({"error": "Unsupported format"}), 400

    # Serve the file for download
    with open(filepath, 'rb') as f:
        file_content = f.read()

    response = make_response(file_content)
    response.headers["Content-Disposition"] = f"attachment; filename={os.path.basename(filepath)}"
    response.mimetype = mimetype
    return response


if __name__ == '__main__':
    app.run(debug=True, threaded=True)