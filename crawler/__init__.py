import asyncio
import signal
import threading

from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
from crawler.data_storage import DataStorage


class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker,
                 data_storage_class=DataStorage):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory
        self.next_permission_request_time = {}
        self.data_storage = data_storage_class()
        self.store_data_timer = None
        self.stop_flag = threading.Event()


    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier, self.data_storage,
                                self.next_permission_request_time, self.stop_flag)
            for worker_id in range(self.config.threads_count)
        ]
        for worker in self.workers:
            worker.start()
        self.store_data_periodically()
        self.stop_flag.clear()


    def start(self):
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        self.start_async()
        self.join()


    def store_data_periodically(self):
        self.logger.info("Storing data...")
        self.data_storage.store_scraped_data()
        self.data_storage.finalize_data()
        self.store_data_timer = threading.Timer(300, self.store_data_periodically)
        self.store_data_timer.start()


    def join(self):
        try:
            for worker in self.workers:
                worker.join()
        except KeyboardInterrupt:
            self.logger.info("Received a stop signal and is stopping all working threads...")
            self.stop()
        finally:
            self.logger.info("Storing final data...")
            self.data_storage.store_scraped_data()
            self.data_storage.finalize_data()
            if self.store_data_timer:
                self.store_data_timer.cancel()


    def stop(self):
        self.stop_flag.set()
        for worker in self.workers:
            worker.stop()


    def sigterm_handler(self, signum, frame):
        self.logger.info("Received SIGTERM, stopping all working threads...")
        self.stop()
