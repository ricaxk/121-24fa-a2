import asyncio
import os
import shelve
from collections import defaultdict
from queue import Queue, Empty, PriorityQueue
from threading import Thread, RLock
from urllib.parse import urlparse

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid


MAX_DEPTH = 500


class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = PriorityQueue()
        self.domain_counts = defaultdict(int)
        self.url_depth = {}
        self.frontier_lock = RLock()

        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed."
            )
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it."
            )
            os.remove(self.config.save_file)
            # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)


    def _parse_save_file(self):
        with self.frontier_lock:
            total_count = len(self.save)
            tbd_count = 0
            for url, completed in self.save.values():
                if not completed and is_valid(url):
                    priority = self.get_url_depth(url)
                    self.to_be_downloaded.put((priority, url))
                    tbd_count += 1
            self.logger.info(
                f"Found {tbd_count} urls to be downloaded from {total_count} "
                f"total urls discovered."
            )


    def get_tbd_url(self):
        try:
            _, url = self.to_be_downloaded.get_nowait()
            return url
        except Empty:
            return None


    def get_url_depth(self, url):
        with self.frontier_lock:
            return self.url_depth.get(url, 0)


    def set_url_depth(self, url, depth):
        with self.frontier_lock:
            self.url_depth[url] = depth


    def increase_url_depth(self, url, increment=1):
        with self.frontier_lock:
            current_depth = self.url_depth.get(url, 0)
            self.url_depth[url] = current_depth + increment


    def add_url(self, url, parent_url=None):
        url = normalize(url)
        urlhash = get_urlhash(url)
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        with self.frontier_lock:
            if urlhash not in self.save:
                if parent_url is not None:
                    parent_depth = self.get_url_depth(parent_url)
                    current_depth = parent_depth + 1
                else:
                    current_depth = 0

                if current_depth > MAX_DEPTH:
                    self.logger.info(f"URL {url} is too deep ({current_depth}), skipping.")
                    return

                self.save[urlhash] = (url, False)
                self.save.sync()
                self.domain_counts[domain] += 1
                priority = self.domain_counts[domain]
                self.to_be_downloaded.put((priority, url))
                self.set_url_depth(url, current_depth)


    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self.frontier_lock:
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before."
                )
            else:
                self.save[urlhash] = (url, True)
                self.save.sync()
