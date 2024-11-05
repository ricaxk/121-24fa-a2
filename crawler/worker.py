import asyncio
import hashlib
import mimetypes
import pdb
import re
import threading
import traceback
from collections import Counter
from datetime import datetime, date
from threading import Thread
from inspect import getsource
from utils.download import download
from utils import get_logger
from urllib.parse import urlparse, parse_qs, urldefrag
import scraper
import time
from simhash import Simhash


MAX_FILE_SIZE = 1024 * 1024 * 10
MIN_DATE = date(1968, 1, 1)
MAX_DATE = date(2023, 11, 3)
DATE_REGEX = re.compile(r'\b\d{4}-\d{1,2}-\d{1,2}\b')
DATE_FORMATES = [
    "%Y-%m-%d",  # ISO 8601 format
    "%d-%m-%Y",  # Day-Month-Year format
    "%m-%d-%Y",  # US format
]


def update_subdomain(url, subdomains):
    """
    Update the subdomains dictionary with unique pages for each subdomain of 'ics.uci.edu'.
    Args:
    url (str): The URL to be processed.
    The function follows these steps:
    1. Parse the URL to extract its components.
    2. Check if the domain is a subdomain of 'ics.uci.edu'.
    3. If it is, remove the fragment part of the URL.
    4. Update the subdomains dictionary and the unique_pages set.
    Note:
    - The fragment part of the URL is removed because it is client-side and does not affect the server's response.
      This ensures that URLs pointing to the same page but with different fragments are not counted separately.
    - Example:
        - let's say there are now two urls:
            1. 'http://subdomain.ics.uci.edu/page2'
            2. 'http://subdomain.ics.uci.edu/page2#i-am-fragment'
        - However, they are actually the same page. This is because the fragment part is used by the client for processing
        and it does not affect the content of the server response. Therefore, when we remove the fragment part from the URL,
        both URLs become http://subdomain.ics.uci.edu/page2.
    """
    parsed_url = urlparse(url)
    domain = parsed_url.hostname

    if domain.endswith("ics.uci.edu"):
        subdomains[domain] = subdomains.get(domain, 0) + 1


class Worker(Thread):
    def __init__(self, worker_id, config, frontier, data_storage, next_permission_request_time, stop_flag):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.data_storage = data_storage
        self.next_permission_request_time = next_permission_request_time
        self._stop_flag = stop_flag
        self.domain_delay_lock = threading.Lock()
        super().__init__(daemon=True)


    def run(self):
        while not self._stop_flag.is_set():
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            self.process_url(tbd_url)
            time.sleep(self.config.time_delay)


    def stop(self):
        self._stop_flag.set()


    def process_url(self, tbd_url):
        try:
            domain = urlparse(tbd_url).netloc
            self.apply_domain_delay(domain)
            resp = download(tbd_url, self.config, self.logger)
            self.update_domain_delay(domain)
            if resp:
                with self.data_storage.visited_url_lock:
                    url_without_fragment, fragment = urldefrag(tbd_url)
                    self.data_storage.visited_url.add(url_without_fragment)

                with self.data_storage.subdomains_lock:
                    update_subdomain(tbd_url, self.data_storage.subdomains)

                self.handle_response(tbd_url, resp)
            else:
                self.logger.error(f"Failed to download {tbd_url}.")
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"An error occurred while processing URL {tbd_url}: {str(e)}")
            pass


    def handle_response(self, tbd_url, resp):
        if resp.raw_response is None:
            self.logger.error(f"No raw response for URL {tbd_url}")
            return
        if tbd_url != resp.url:
            self.logger.info(f"Redirected from {tbd_url} to {resp.url}")
            self.frontier.mark_url_complete(tbd_url)
            tbd_url = resp.url
        if not self.check_duplicate_content(tbd_url, resp.raw_response.content):
            return
        if not self.check_file_size(tbd_url, resp.raw_response.headers, resp.raw_response.content):
            return
        if not self.check_file_type_and_url_pattern(tbd_url, resp.raw_response.headers):
            return
        if not self.check_valid_date_range(tbd_url, resp.raw_response.content):
            return

        self.logger.info(
            f"Downloaded {tbd_url}, status <{resp.status}>, "
            f"using cache {self.config.cache_server}."
        )
        self.process_scraped_urls(tbd_url, resp)


    def process_scraped_urls(self, tbd_url, resp):
        scraped_urls = scraper.scraper(tbd_url, resp, self.data_storage)
        for scraped_url in scraped_urls:
            self.frontier.add_url(scraped_url, parent_url=tbd_url)
        self.frontier.mark_url_complete(tbd_url)


    def apply_domain_delay(self, domain):
        with self.domain_delay_lock:
            now = time.time()
            if domain in self.next_permission_request_time:
                delay = self.next_permission_request_time[domain] - now
                if delay > 0:
                    time.sleep(delay)


    def update_domain_delay(self, domain):
        with self.domain_delay_lock:
            minimum_delay = 0.5
            self.next_permission_request_time[domain] = max(
                time.time() + self.config.time_delay,
                self.next_permission_request_time.get(domain, 0) + minimum_delay
            )


    def check_duplicate_content(self, tbd_url, content):
        try:
            md5_content = self.hash_content_by_md5(content)
            with self.data_storage.md5_set_lock:
                if md5_content in self.data_storage.md5_set:
                    self.logger.info(f"Duplicate content found for URL {tbd_url}")
                    self.frontier.mark_url_complete(tbd_url)
                    return False
                else:
                    self.data_storage.md5_set.add(md5_content)

            simhash_content = self.hash_content_by_simhash(content)
            with self.data_storage.simhash_set_lock:
                if simhash_content in self.data_storage.simhash_set:
                    self.logger.info(f"Similar content found for URL {tbd_url}")
                    self.frontier.mark_url_complete(tbd_url)
                    return False
                else:
                    self.data_storage.simhash_set.add(simhash_content)
            return True
        except Exception as e:
            traceback.print_exc()
            print(f"An error occurred: {str(e)}")
            pass


    def check_file_size(self, tbd_url, headers, content):
        try:
            if self.is_large_file(headers, content):
                self.logger.info(f"Content is too large for URL {tbd_url}")
                self.frontier.mark_url_complete(tbd_url)
                return False
            return True
        except Exception as e:
            traceback.print_exc()
            print(f"An error occurred: {str(e)}")
            pass


    def check_file_type_and_url_pattern(self, tbd_url, headers):
        if self.is_unwanted_file_type(tbd_url, headers) or self.is_unwanted_url_pattern(tbd_url):
            self.logger.info(f"Unwanted content found for URL {tbd_url}")
            self.frontier.mark_url_complete(tbd_url)
            return False
        return True


    @staticmethod
    def hash_content_by_md5(content):
        return hashlib.md5(content).hexdigest()


    @staticmethod
    def hash_content_by_simhash(content):
        try:
            # Step 1: Convert bytes to string
            text = content.decode('utf-8', errors='ignore')

            # Step 2: Tokenize the text and count word occurrences
            words = re.findall(r'\w+', text)
            word_counts = Counter(words)

            # Step 3: Create and return the Simhash object
            return Simhash(word_counts).value
        except Exception as e:
            traceback.print_exc()
            print(f"An error occurred: {str(e)}")
            pass


    @staticmethod
    def is_large_file(headers, content) -> bool:
        content_length = headers.get('Content-Length')
        if content_length is not None:
            content_length = int(content_length)
        elif content is not None:
            content_length = len(content)
        else:
            content_length = 0
        return content_length > MAX_FILE_SIZE


    @staticmethod
    def is_unwanted_file_type(url, headers) -> bool:
        content_type = headers.get('Content-Type')
        if not content_type:
            content_type, _ = mimetypes.guess_type(url)
        return content_type and (
                'video/' in content_type or 'image/' in content_type or 'application/zip' in content_type)


    @staticmethod
    def is_unwanted_url_pattern(url) -> bool:
        parsed_url = urlparse(url)
        unwanted_patterns = ['/download/', '?format=zip', '/calendar/', '/date/']
        date_query_parameters = ['date', 'year', 'month', 'day']

        if any(pattern in parsed_url.path for pattern in unwanted_patterns):
            return True
        query_parameters = parse_qs(parsed_url.query)
        if any(param in query_parameters for param in date_query_parameters):
            return True
        return False


    def check_valid_date_range(self, url, content):
        try:
            content = content.decode('utf-8', errors='ignore')
            dates = DATE_REGEX.findall(url + content)
            if not dates:
                self.logger.info(f"No dates found for URL {url}.")
                return True
            for date_str in dates:
                if not self.is_date_in_range(date_str, url):
                    return False
            return True
        except Exception as e:
            self.logger.error(f"An error occurred while validating date range for URL {url}: {str(e)}")
            traceback.print_exc()
            return False


    def is_date_in_range(self, date_str, url):
        try:
            date = self.parse_date(date_str)
            if not (MIN_DATE <= date <= MAX_DATE):
                self.logger.info(f"URL {url} is out of valid date range.")
                self.frontier.mark_url_complete(url)
                return False
            return True
        except (ValueError, OverflowError) as e:
            self.logger.info(f"Invalid date format for URL {url}: {e}")
            self.frontier.mark_url_complete(url)
            return False


    def parse_date(self, date_str):
        for date_format in DATE_FORMATES:
            try:
                return datetime.strptime(date_str, date_format).date()
            except ValueError:
                continue  # Try the next format

        raise ValueError(f"Date format is incorrect for {date_str}. Tried formats: {DATE_FORMATES}")
