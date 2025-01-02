import json
from datetime import datetime
import re

def parse_date(date_str):
    """Parse date string from arxiv format to datetime object"""
    try:
        return datetime.strptime(date_str.strip(), '%a, %d %b %Y %H:%M:%S %Z')
    except ValueError:
        return None

def filter_papers(input_file, start_date=None, end_date=None, output_file="cs_papers_filtered.json"):
    """
    Filter papers by CS category and date range
    
    Args:
        input_file (str): Path to input JSON file
        start_date (str): Start date in format 'YYYY-MM-DD' (inclusive)
        end_date (str): End date in format 'YYYY-MM-DD' (inclusive)
        output_file (str): Path to output JSON file
    """
    # Convert date strings to datetime objects if provided
    start_dt = datetime.strptime(start_date, '%Y-%m-%d') if start_date else None
    end_dt = datetime.strptime(end_date, '%Y-%m-%d') if end_date else None
    
    count = 0
    cs_count = 0
    
    print("Starting to process papers...")
    
    with open(input_file, 'r') as f, open(output_file, 'w') as out_f:
        # Write opening bracket for JSON array
        out_f.write('[\n')
        first_entry = True
        
        for line in f:
            try:
                paper = json.loads(line.strip())
                count += 1
                
                if count % 100000 == 0:
                    print(f"Processed {count:,} papers, found {cs_count:,} CS papers...")
                
                # Check categories
                categories = paper.get('categories', '')
                if not any(re.match(r'^cs\.', cat) for cat in categories.split()):
                    continue
                
                # Get creation date from v1 version
                versions = paper.get('versions', [])
                v1_version = next((v for v in versions if v['version'] == 'v1'), None)
                if not v1_version:
                    continue
                
                created_date = parse_date(v1_version['created'])
                if not created_date:
                    continue
                
                # Check date range if specified
                if start_dt and created_date < start_dt:
                    continue
                if end_dt and created_date > end_dt:
                    continue
                
                # If we get here, the paper matches all criteria
                cs_count += 1
                
                # Write comma before entry if it's not the first one
                if not first_entry:
                    out_f.write(',\n')
                first_entry = False
                
                # Write the paper data
                json.dump(paper, out_f)
                
            except json.JSONDecodeError:
                print(f"Warning: Skipping invalid JSON at line {count}")
            except Exception as e:
                print(f"Warning: Error processing line {count}: {str(e)}")
    
        # Write closing bracket for JSON array
        out_f.write('\n]')
    
    print(f"\nProcessing complete!")
    print(f"Total papers processed: {count:,}")
    print(f"CS papers found: {cs_count:,}")
    print(f"Results saved to: {output_file}")

if __name__ == "__main__":
    # Example usage:
    filter_papers(
        input_file="data/arxiv-metadata-oai-snapshot.json",
        start_date="2024-12-01",  # Optional: filter papers from this date
        end_date="2024-12-31",    # Optional: filter papers until this date
        output_file="data/cs_papers_filtered.json"
    )

