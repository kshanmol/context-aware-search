import json
import os
import time
import pdfplumber
import requests
from pathlib import Path
from collections import deque
from datetime import datetime

class RateLimiter:
    def __init__(self, burst_size=4, sleep_time=1):
        self.burst_size = burst_size
        self.sleep_time = sleep_time
        self.request_times = deque(maxlen=burst_size)
    
    def wait_if_needed(self):
        now = datetime.now()
        if len(self.request_times) == self.burst_size:
            time_diff = (now - self.request_times[0]).total_seconds()
            if time_diff < self.sleep_time:
                time.sleep(self.sleep_time - time_diff)
            self.request_times.popleft()
        self.request_times.append(now)

def download_pdf(arxiv_id, output_path, rate_limiter):
    """Download PDF from arXiv's export site with rate limiting"""
    url = f"https://export.arxiv.org/pdf/{arxiv_id}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Python script for academic research; Contact: kshasingh@gmail.com)'
    }
    
    try:
        rate_limiter.wait_if_needed()
        
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return True
    except Exception as e:
        print(f"Error downloading {arxiv_id}: {str(e)}")
        return False

def extract_text_from_pdf(pdf_path):
    """Extract text from PDF using pdfplumber"""
    try:
        text_content = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_content.append(text)
        return '\n\n'.join(text_content)
    except Exception as e:
        print(f"Error extracting text from PDF: {str(e)}")
        return None

def process_papers(input_file, output_dir, resume=True):
    """Process CS papers: download PDF, convert to text, save with metadata"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Initialize rate limiter
    rate_limiter = RateLimiter(burst_size=4, sleep_time=1)
    
    # Keep track of progress
    processed = 0
    failed = 0
    
    # Create a log file for tracking progress
    log_file = os.path.join(output_dir, "processed_papers.log")
    processed_ids = set()
    
    # Load previously processed papers if resuming
    if resume and os.path.exists(log_file):
        with open(log_file, 'r') as f:
            processed_ids = set(line.strip() for line in f)
    
    with open(input_file, 'r') as f:
        papers = json.load(f)
        total_papers = len(papers)
        
        # Open log file in append mode
        with open(log_file, 'a') as log:
            for paper in papers:
                try:
                    arxiv_id = paper['id']
                    
                    # Skip if already processed and resuming
                    if arxiv_id in processed_ids:
                        processed += 1
                        continue
                    
                    print(f"\nProcessing paper {processed + 1}/{total_papers}: {arxiv_id}")
                    
                    pdf_path = os.path.join(output_dir, f"{arxiv_id}.pdf")
                    
                    if not download_pdf(arxiv_id, pdf_path, rate_limiter):
                        failed += 1
                        continue
                    
                    paper_text = extract_text_from_pdf(pdf_path)
                    if not paper_text:
                        failed += 1
                        if os.path.exists(pdf_path):
                            os.remove(pdf_path)
                        continue
                    
                    output_path = os.path.join(output_dir, f"{arxiv_id}.txt")
                    
                    with open(output_path, 'w', encoding='utf-8') as out_f:
                        out_f.write("---METADATA---\n")
                        json.dump(paper, out_f, ensure_ascii=False, indent=2)
                        out_f.write("\n---FULLTEXT---\n")
                        out_f.write(paper_text)
                    
                    # Delete PDF file
                    os.remove(pdf_path)
                    
                    # Log successfully processed paper
                    log.write(f"{arxiv_id}\n")
                    log.flush()
                    
                    processed += 1
                    
                    if processed % 10 == 0:
                        print(f"Processed {processed} papers. Failed: {failed}")
                    
                except Exception as e:
                    print(f"Error processing paper {arxiv_id}: {str(e)}")
                    failed += 1
                    if os.path.exists(pdf_path):
                        os.remove(pdf_path)
    
    print(f"\nProcessing complete!")
    print(f"Successfully processed: {processed}")
    print(f"Failed: {failed}")
    print(f"Total completion rate: {(processed/(processed+failed))*100:.2f}%")

if __name__ == "__main__":
    
    process_papers(
        input_file="data/cs_papers_filtered.json",
        output_dir="data/papers",
        resume=True  # Enable resuming from previous run
    )
