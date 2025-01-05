import json
import os
import time
import pdfplumber
import requests
from pathlib import Path

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('processing.log', mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)


def download_pdf(arxiv_id, output_path):
   """Download PDF from arXiv"""
   url = f"https://export.arxiv.org/pdf/{arxiv_id}"
   headers = {
       'User-Agent': 'Mozilla/5.0 (Python script for academic research; Contact: your@email.com)'
   }
   
   try:
       response = requests.get(url, headers=headers)
       response.raise_for_status()
       
       with open(output_path, 'wb') as f:
           f.write(response.content)
       return True
   except Exception as e:
       logging.error(f"Error downloading {arxiv_id}: {str(e)}")
       return False


def extract_text_from_pdf(pdf_path):
    """Extract text from PDF with focus on proper word separation"""
    try:
        text_content = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # Extract words with their positions
                words = page.extract_words(
                    keep_blank_chars=False,
                    x_tolerance=1,
                    y_tolerance=3,
                    split_at_punctuation=True,
                )
                
                if words:
                    # Sort words by vertical position then horizontal
                    words.sort(key=lambda w: (round(w['top'], 1), w['x0']))
                    
                    # Group words into lines
                    current_line = []
                    current_y = None
                    lines = []
                    
                    for word in words:
                        if current_y is None:
                            current_y = round(word['top'], 1)
                        
                        # If significant y-position change, start new line
                        if abs(round(word['top'], 1) - current_y) > 3:
                            if current_line:
                                # Join words in the line with proper spacing
                                lines.append(' '.join(current_line))
                                current_line = []
                            current_y = round(word['top'], 1)
                        
                        # Clean the word text
                        word_text = word['text'].strip()
                        if word_text:
                            current_line.append(word_text)
                    
                    # Add last line
                    if current_line:
                        lines.append(' '.join(current_line))
                    
                    # Add page content
                    if lines:
                        text_content.append("\n".join(lines))
                        text_content.append("\n")  # Add separation between pages
        
        return '\n'.join(text_content)
    except Exception as e:
        logging.error(f"Error extracting text from PDF: {str(e)}")
        return None


def process_papers(input_file, output_dir, resume=True):
   """Process papers in bursts of 4 with 1 second sleep between bursts"""
   Path(output_dir).mkdir(parents=True, exist_ok=True)
   
   processed = 0
   failed = 0
   
   # Track processed papers if resuming
   log_file = os.path.join(output_dir, "processed_papers.log")
   processed_ids = set()
   
   if resume and os.path.exists(log_file):
       with open(log_file, 'r') as f:
           processed_ids = set(line.strip() for line in f)
   
   # Load papers
   with open(input_file, 'r') as f:
       papers = json.load(f)
   
   # Open log file in append mode
   with open(log_file, 'a') as log:
       # Process in batches of 4
       for i in range(0, len(papers), 4):
           batch = papers[i:i+4]
           logging.info(f"\nProcessing batch {i//4 + 1}/{(len(papers) + 3)//4}")
           
           # Process each paper in the batch
           for paper in batch:
               arxiv_id = paper['id']
               
               # Skip if already processed
               if arxiv_id in processed_ids:
                   processed += 1
                   continue
               
               logging.info(f"Processing paper: {arxiv_id}")
               
               pdf_path = os.path.join(output_dir, f"{arxiv_id}.pdf")
               output_path = os.path.join(output_dir, f"{arxiv_id}.txt")
               
               try:
                   # Download and process
                   if not download_pdf(arxiv_id, pdf_path):
                       failed += 1
                       continue
                   
                   paper_text = extract_text_from_pdf(pdf_path)
                   if not paper_text:
                       failed += 1
                       if os.path.exists(pdf_path):
                           os.remove(pdf_path)
                       continue
                   
                   # Save output
                   with open(output_path, 'w', encoding='utf-8') as out_f:
                       out_f.write("---METADATA---\n")
                       json.dump(paper, out_f, ensure_ascii=False, indent=2)
                       out_f.write("\n---FULLTEXT---\n")
                       out_f.write(paper_text)
                   
                   # Cleanup
                   os.remove(pdf_path)
                   
                   # Log successful processing
                   log.write(f"{arxiv_id}\n")
                   log.flush()
                   
                   processed += 1
                   
               except Exception as e:
                   logging.info(f"Error processing paper {arxiv_id}: {str(e)}")
                   failed += 1
                   if os.path.exists(pdf_path):
                       os.remove(pdf_path)
           
           # Sleep between batches
           time.sleep(1)
           logging.info(f"Progress: Processed {processed}, Failed: {failed}")
   
   logging.info(f"\nProcessing complete!")
   logging.info(f"Successfully processed: {processed}")
   logging.info(f"Failed: {failed}")
   logging.info(f"Total completion rate: {(processed/(processed+failed))*100:.2f}%")

if __name__ == "__main__":
   process_papers(
       input_file="data/cs_papers_filtered.json",
       output_dir="data/papers",
       resume=True  # Enable resuming from previous run
   )

