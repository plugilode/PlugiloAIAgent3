import csv
import subprocess
import concurrent.futures
import json
import pandas as pd
import time
from urllib.parse import urlparse
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'api_calls_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
)

# Constants
API_URL = 'https://api-lr.agent.ai/api/company/lite'
HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/json',
    'origin': 'https://agent.ai',
    'priority': 'u=1, i',
    'referer': 'https://agent.ai/',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
}

SAVE_FREQUENCY = 10

class Report:
    def __init__(self):
        self.stats = {
            "success_count": 0,
            "error_count": 0,
            "start_time": datetime.now(),
            "last_save_time": None,
            "processed_domains": [],
            "failed_domains": []
        }
    
    def update(self, success=True, domain=None):
        if success:
            self.stats["success_count"] += 1
            if domain:
                self.stats["processed_domains"].append(domain)
        else:
            self.stats["error_count"] += 1
            if domain:
                self.stats["failed_domains"].append(domain)
    
    def save(self, filename="report.json"):
        self.stats["last_save_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(filename, 'w') as f:
            json.dump(self.stats, f, indent=4)

def clean_domain(url):
    """Extract and clean domain from URL."""
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        domain = parsed.netloc
        
        if domain.startswith('www.'):
            domain = domain[4:]
            
        return domain.lower().strip()
    except Exception as e:
        logging.error(f"Error cleaning domain {url}: {str(e)}")
        return url

def call_api(website, report):
    """Make API call for a given website."""
    try:
        clean_website = clean_domain(website)
        
        data = {
            "domain": clean_website,
            "report_component": "harmonic_funding_and_web_traffic",
            "user_id": None
        }

        command = [
            'curl', API_URL,
            '-X', 'POST',
            '-H', f'accept: {HEADERS["accept"]}',
            '-H', f'accept-language: {HEADERS["accept-language"]}',
            '-H', f'content-type: {HEADERS["content-type"]}',
            '-H', f'origin: {HEADERS["origin"]}',
            '-H', f'priority: {HEADERS["priority"]}',
            '-H', f'referer: {HEADERS["referer"]}',
            '-H', f'sec-ch-ua: {HEADERS["sec-ch-ua"]}',
            '-H', f'sec-ch-ua-mobile: {HEADERS["sec-ch-ua-mobile"]}',
            '-H', f'sec-ch-ua-platform: {HEADERS["sec-ch-ua-platform"]}',
            '-H', f'sec-fetch-dest: {HEADERS["sec-fetch-dest"]}',
            '-H', f'sec-fetch-mode: {HEADERS["sec-fetch-mode"]}',
            '-H', f'sec-fetch-site: {HEADERS["sec-fetch-site"]}',
            '-H', f'user-agent: {HEADERS["user-agent"]}',
            '--data-raw', json.dumps(data)
        ]
        
        result = subprocess.run(command, capture_output=True, text=True)
        
        if result.returncode != 0:
            logging.error(f"Error calling API for {website}: {result.stderr}")
            report.update(success=False, domain=clean_website)
            return None

        try:
            response_data = json.loads(result.stdout)
            logging.info(f"Successfully processed {website}")
            report.update(success=True, domain=clean_website)
            return response_data
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON response for {website}: {str(e)}")
            report.update(success=False, domain=clean_website)
            return None

    except Exception as e:
        logging.error(f"Unexpected error processing {website}: {str(e)}")
        report.update(success=False, domain=clean_website)
        return None

def process_websites(websites):
    """Process a list of websites using concurrent workers."""
    results = []
    report = Report()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        future_to_website = {executor.submit(call_api, website, report): website 
                           for website in websites}
        
        for future in concurrent.futures.as_completed(future_to_website):
            website = future_to_website[future]
            try:
                result = future.result()
                if result is not None:
                    company_data = result.get('company_data', {})
                    company = company_data.get('company', {})
                    
                    output = {
                        'basic_info': {
                            'name': company.get('name'),
                            'domain': company.get('domain'),
                            'description': company.get('description'),
                            'founded_year': company.get('foundedYear'),
                            'legal_name': company.get('legalName'),
                            'company_type': company.get('type'),
                            'email_provider': company.get('emailProvider'),
                            'id': company.get('id'),
                            'indexed_at': company.get('indexedAt'),
                            'logo': company.get('logo'),
                        },
                        'category': {
                            'gics_code': company.get('category', {}).get('gicsCode'),
                            'industry': company.get('category', {}).get('industry'),
                            'industry_group': company.get('category', {}).get('industryGroup'),
                            'naics_codes': company.get('category', {}).get('naics6Codes'),
                            'naics_codes_2022': company.get('category', {}).get('naics6Codes2022'),
                            'naics_code_main': company.get('category', {}).get('naicsCode'),
                            'sector': company.get('category', {}).get('sector'),
                            'sic_codes': company.get('category', {}).get('sic4Codes'),
                            'sic_code_main': company.get('category', {}).get('sicCode'),
                            'sub_industry': company.get('category', {}).get('subIndustry')
                        },
                        'location': {
                            'location': company.get('location'),
                            'geo': company.get('geo', {}),
                            'time_zone': company.get('timeZone'),
                            'utc_offset': company.get('utcOffset')
                        },
                        'identifiers': {
                            'us_cik': company.get('identifiers', {}).get('usCIK'),
                            'us_ein': company.get('identifiers', {}).get('usEIN')
                        },
                        'metrics': {
                            'alexa_global_rank': company.get('metrics', {}).get('alexaGlobalRank'),
                            'alexa_us_rank': company.get('metrics', {}).get('alexaUsRank'),
                            'annual_revenue': company.get('metrics', {}).get('annualRevenue'),
                            'employees': company.get('metrics', {}).get('employees'),
                            'employees_range': company.get('metrics', {}).get('employeesRange'),
                            'estimated_annual_revenue': company.get('metrics', {}).get('estimatedAnnualRevenue'),
                            'fiscal_year_end': company.get('metrics', {}).get('fiscalYearEnd'),
                            'market_cap': company.get('metrics', {}).get('marketCap'),
                            'raised': company.get('metrics', {}).get('raised'),
                            'traffic_rank': company.get('metrics', {}).get('trafficRank')
                        },
                        'social_media': {
                            'linkedin': {
                                'handle': company.get('linkedin', {}).get('handle'),
                                'follower_count': company.get('linkedin_follower_count'),
                                'follower_count_raw': company.get('linkedin_follower_count_raw')
                            },
                            'twitter': {
                                'handle': company.get('twitter', {}).get('handle'),
                                'followers': company.get('twitter', {}).get('followers'),
                                'following': company.get('twitter', {}).get('following'),
                                'bio': company.get('twitter', {}).get('bio'),
                                'follower_count': company.get('twitter_follower_count'),
                                'follower_count_raw': company.get('twitter_follower_count_raw')
                            },
                            'facebook': {
                                'handle': company.get('facebook', {}).get('handle'),
                                'likes': company.get('facebook', {}).get('likes')
                            }
                        },
                        'site_info': {
                            'email_addresses': company.get('site', {}).get('emailAddresses', []),
                            'phone_numbers': company.get('site', {}).get('phoneNumbers', [])
                        },
                        'tech_stack': {
                            'technologies': company.get('tech', []),
                            'categories': company.get('techCategories', [])
                        },
                        'company_metrics': {
                            'headcount': company.get('headcount'),
                            'headcount_raw': company.get('headcount_raw'),
                            'web_traffic': company.get('web_traffic'),
                            'web_traffic_raw': company.get('web_traffic_raw')
                        },
                        'founders': company.get('founders', []),
                        'funding_data': company.get('funding_data', {}),
                        'tags': company.get('tags', []),
                        'domain_aliases': company.get('domainAliases', []),
                        'parent_info': {
                            'parent': company.get('parent', {}),
                            'ultimate_parent': company.get('ultimateParent', {})
                        },
                        'question_answers': result.get('question_answers')
                    }
                    results.append(output)
                    
            except Exception as e:
                logging.error(f"Error processing result for {website}: {str(e)}")
                
    return results

def main():
    try:
        # Read URLs in chunks to manage memory
        chunk_size = 1000
        all_results = []
        
        for chunk_index, chunk in enumerate(pd.read_csv('urls.csv', chunksize=chunk_size, encoding='utf-8')):
            logging.info(f"Processing chunk {chunk_index + 1}")
            websites = chunk.iloc[:, 0].tolist()
            
            # Process websites in batches of 100 for optimal concurrency
            batch_size = 100
            for i in range(0, len(websites), batch_size):
                batch = websites[i:i + batch_size]
                logging.info(f"Processing batch {i//batch_size + 1} of chunk {chunk_index + 1}")
                
                results = process_websites(batch)
                all_results.extend(results)
                
                # Save intermediate results after each batch
                if len(all_results) % SAVE_FREQUENCY == 0:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    interim_df = pd.DataFrame(all_results)
                    interim_df.to_csv(f'output_interim_{timestamp}.csv', index=False)
                    logging.info(f"Saved interim results, processed {len(all_results)} websites")
        
        # Save final results
        final_df = pd.DataFrame(all_results)
        final_df.to_csv('output_final.csv', index=False)
        logging.info("Processing completed successfully")
        
    except Exception as e:
        logging.error(f"Main process error: {str(e)}")

if __name__ == '__main__':
    main()