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
    """Download PDF from arXiv with rate limiting"""
    url = f"https://export.arxiv.org/pdf/{arxiv_id}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Python script for academic research; Contact: your@email.com)'
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
    """Extract text from PDF with layout preservation"""
    try:
        text_content = []
        with pdfplumber.open(pdf_path) as pdf:
            # First pass: detect page layout and fonts
            all_fonts = set()
            all_sizes = []
            title_candidates = []
            
            for i, page in enumerate(pdf.pages):
                if i == 0:  # Check first page for title
                    words = page.extract_words(
                        keep_blank_chars=True,
                        extra_attrs=['size', 'fontname']
                    )
                    for word in words[:20]:  # Look at first 20 words for title
                        all_fonts.add(word['fontname'])
                        all_sizes.append(float(word['size']))
                        title_candidates.append(word)

            # Identify title font (usually largest on first page)
            title_size = max(all_sizes) if all_sizes else 0
            
            # Second pass: extract text with layout preservation
            for i, page in enumerate(pdf.pages):
                # Get page dimensions
                width = page.width
                height = page.height
                
                # Extract text with careful layout settings
                page_text = page.extract_text(
                    x_tolerance=3,      # Closer x_tolerance for better word grouping
                    y_tolerance=3,      # Closer y_tolerance for better line detection
                    keep_blank_chars=True,
                    use_text_flow=False,  # Preserve original layout
                )
                
                if page_text:
                    # Add page number and clean text
                    text_content.append(f"\n\n=== Page {i+1} ===\n")
                    text_content.append(page_text)
                else:
                    # Fallback: extract words and rebuild layout
                    words = page.extract_words(
                        keep_blank_chars=True,
                        extra_attrs=['size', 'fontname', 'top', 'bottom', 'doctop']
                    )
                    
                    current_line = []
                    current_y = None
                    
                    for word in sorted(words, key=lambda w: (w['doctop'], w['x0'])):
                        if current_y is None:
                            current_y = word['doctop']
                        
                        # New line if significant y-position change
                        if abs(word['doctop'] - current_y) > 5:
                            if current_line:
                                text_content.append(' '.join(current_line))
                                text_content.append('\n')
                            current_line = []
                            current_y = word['doctop']
                        
                        current_line.append(word['text'])
                    
                    # Add last line
                    if current_line:
                        text_content.append(' '.join(current_line))
                        text_content.append('\n')
        
        return '\n'.join(text_content)
    except Exception as e:
        print(f"Error extracting text from PDF: {str(e)}")
        return None

def process_papers(input_file, output_dir, resume=True):
    """Process papers: download PDFs and extract text"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    rate_limiter = RateLimiter(burst_size=4, sleep_time=1)
    processed = 0
    failed = 0
    
    log_file = os.path.join(output_dir, "processed_papers.log")
    processed_ids = set()
    
    if resume and os.path.exists(log_file):
        with open(log_file, 'r') as f:
            processed_ids = set(line.strip() for line in f)
    
    with open(input_file, 'r') as f:
        papers = json.load(f)
        total_papers = len(papers)
        
        with open(log_file, 'a') as log:
            for paper in papers:
                try:
                    arxiv_id = paper['id']
                    
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
                    
                    os.remove(pdf_path)
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
        resume=True
    )
