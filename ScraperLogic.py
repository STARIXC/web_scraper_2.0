import requests
from bs4 import BeautifulSoup
import re
import time
from urllib.parse import urlparse,urljoin
from collections import deque
import json

class WebScraperCore:
    def __init__(self,start_url, max_pages,delay,selected_tags,structured_var,job_id, update_callback):
        self.start_url = start_url
        self.max_pages = max_pages
        self.delay = delay
        self.selected_tags = selected_tags
        self.structured_var = structured_var
        self.job_id = job_id
        self.update_callback = update_callback

        self.scraped_data= {}
        self.is_scraping = True
        self.visited_urls = set()
        self.queue = deque()


    def stop_scraping(self):
        self.is_scraping = False

    def crawl_and_scrape(self):
        # initial setup
        self.queue.append(self.start_url)
        self.visited_urls.add(self.start_url)
        domain = urlparse(self.start_url).netloc
        pages_scraped = 0
        max_retries = 3

        #initial status update
        self.update_callback(self.job_id,'RUNNING',pages_scraped,self.scraped_data)

        while self.queue and pages_scraped < self.max_pages and self.is_scraping:
            current_url = self.queue.popleft()
            self.update_callback(self.job_id,'RUNNING',pages_scraped,self.scraped_data,
                                 log_message=f"Attempting to scrape: {current_url}")

            soup=None
            for attempt in range(max_retries):
                if not self.is_scraping: break
                try:
                    #1. Fetch Page (Single Request)
                    response = requests.get(current_url, timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'lxml')
                    break # Successful fetch
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        self.update_callback(self.job_id,'RUNNING',pages_scraped,self.scraped_data,
                                             log_message=f"Failed to fetch {current_url}. Retrying... {attempt + 1}: {e}")
                        time.sleep(2)
                        continue
                    self.update_callback(self.job_id,'RUNNING',pages_scraped,self.scraped_data,
                                         log_message=f"Failed to fetch {current_url} ")
                except Exception as e:
                    self.update_callback(self.job_id,'RUNNING',pages_scraped,self.scraped_data,
                                         log_message=f"Error Processing {current_url}: {e} ")
                    break
            if soup and self.is_scraping:
                # 2. Scrape Content (Text, Table,Metadata
                content=[]
                for tag in self.selected_tags:
                    elements =soup.find_all(tag)
                    for element in elements:
                        text = element.get_text(strip=True)
                        if text:
                            content.append(text)

                cleaned_content=[re.sub(r'\s+',' ',text) for text in content]
                text_content = '\n'.join(list(dict.fromkeys(cleaned_content)))

                tables=[]
                metadata=[]
                if self.structured_var:
                    for table in soup.find_all('table'):
                        rows=[]
                        for row in table.find_all('tr'):
                            cells =[td.get_text(strip=True) for td in row.find_all('td','th')]
                            if cells:
                                rows.append(cells)
                            if rows:tables.append(rows)

                    for meta in soup.find_all('meta'):
                        meta_data=[]
                        #meta extraction
                        if meta.get('name'): meta_data.append(meta.get('name'))
                        if meta.get('property'): meta_data.append(meta.get('property'))
                        if meta.get('content'): meta_data.append(meta.get('content'))
                        if meta_data:metadata.append(meta_data)

                self.scraped_data[current_url] = {
                    'text' : text_content,
                    'tables' : tables,
                    'metadata' : metadata,
                }
                pages_scraped += 1
                # 3. Update status and find links
                self.update_callback(self.job_id,'RUNNING',pages_scraped,self.scraped_data,
                                     log_message=f"Successfully scraped: {pages_scraped} pages"
                                     )
                #find links to crawl
                for link in soup.find_all('a', href=True):
                    if not self.is_scraping: break
                    href = link['href']
                    full_url = urljoin(current_url, href)
                    parsed_url = urlparse(full_url)
                    if parsed_url.netloc == domain and full_url not in self.visited_urls:
                        self.queue.append(full_url)
                        self.visited_urls.add(full_url)

            if self.delay >0 and self.is_scraping:
                time.sleep(self.delay)
        # Final status update
        final_status ='STOPPED' if not self.is_scraping else 'FINISHED'
        self.update_callback(self.job_id,final_status,pages_scraped,self.scraped_data,
                             log_message=f"Scraping finished. Status : {final_status}")
