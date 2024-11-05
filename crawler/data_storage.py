import datetime
import fcntl
import json
import os
import threading
from collections import Counter
from pathlib import Path

from sortedcontainers import SortedDict


class DataStorage(object):
    def __init__(self):
        self.DATA_STORAGE_FILES = {
            "unique_pages_count.json": ("visited_url", "visited_url_lock"),
            "longest_content_page.json": ("longest_page", "longest_page_lock"),
            "top50_common_words.json": ("common_words", "common_words_lock"),
            "subdomains_stats.json": ("subdomains", "subdomains_lock")
        }
        self.DATA_STORAGE_DIR = Path("../data_storage")
        self.DATA_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
        self.visited_url = set()
        self.longest_page = {"url": None, "word_count": 0}
        self.common_words = {}
        self.subdomains = {}
        self.md5_set = set()
        self.simhash_set = set()
        self.visited_url_lock = threading.RLock()
        self.longest_page_lock = threading.RLock()
        self.common_words_lock = threading.RLock()
        self.subdomains_lock = threading.RLock()
        self.md5_set_lock = threading.RLock()
        self.simhash_set_lock = threading.RLock()


    def store_scraped_data(self):
        for output_file_name, (data_key, lock_name) in self.DATA_STORAGE_FILES.items():
            with getattr(self, lock_name):
                print(lock_name)
                data = getattr(self, data_key)
                output_file_path = self.DATA_STORAGE_DIR / output_file_name

                if data_key == "visited_url" and lock_name == "visited_url_lock":
                    self._store_unique_pages_count(output_file_path, data)
                elif data_key == "longest_page" and lock_name == "longest_page_lock":
                    self._store_longest_page(output_file_path, data)
                elif data_key == "subdomains" and lock_name == "subdomains_lock":
                    self._store_subdomains_stats(output_file_path, data)
                elif data_key == "common_words" and lock_name == "common_words_lock":
                    self._store_top50_common_words(output_file_path, data)


    def _store_top50_common_words(self, file_path, data):
        top_50_common_words = dict(sorted(data.items(), key=lambda item: item[1], reverse=True)[:50])
        self._write_json(file_path, top_50_common_words)
        print("top_50_common_words(): " + str(top_50_common_words))


    def _store_longest_page(self, file_path, data):
        self._write_json(file_path, data)
        print("store_longest_page(): " + str(data))


    def _store_unique_pages_count(self, file_path, data):
        self._write_json(file_path, len(data))
        print("store_unique_pages_count(): " + str(len(data)))


    def _store_subdomains_stats(self, file_path, data):
        sorted_subdomains = dict(sorted(data.items(), key=lambda item: item[1], reverse=True))
        self._write_json(file_path, sorted_subdomains)
        print("subdomains: " + str(sorted_subdomains))


    def _read_json(self, file_path):
        try:
            if os.path.getsize(file_path) == 0:
                print(f"File {file_path} is empty.")
                return None
            with open(file_path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error loading JSON from {file_path}: {str(e)}")
            return None


    def _write_json(self, file_path, data):
        try:
            with open(file_path, "w") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                json.dump(self._convert_sets_to_lists(data), f)
                f.flush()  # Flush the internal buffer
                os.fsync(f.fileno())
                fcntl.flock(f, fcntl.LOCK_UN)
        except TypeError as e:
            print(f"Type error when writing JSON to {file_path}: {str(e)}")
        except Exception as e:
            print(f"Error writing JSON to {file_path}: {str(e)}")


    def _convert_sets_to_lists(self, obj):
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, list):
            return [self._convert_sets_to_lists(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._convert_sets_to_lists(value) for key, value in obj.items()}
        else:
            return obj


    def finalize_data(self):
        output_file_path = self.DATA_STORAGE_DIR / "final_results_summary.txt"

        with open(output_file_path, "a") as output_file:
            fcntl.flock(output_file, fcntl.LOCK_EX)
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            output_file.write(f"Data finalization started at {current_time}\n")
            output_file.write("-------------------------------\n")

            for _, (data_key, lock_name) in self.DATA_STORAGE_FILES.items():
                with getattr(self, lock_name):
                    data = getattr(self, data_key)
                    if data_key == "visited_url" and lock_name == "visited_url_lock":
                        self._finalize_unique_pages_count(output_file, data)
                    elif data_key == "longest_page" and lock_name == "longest_page_lock":
                        self._finalize_longest_content_page(output_file, data)
                    elif data_key == "subdomains" and lock_name == "subdomains_lock":
                        self._finalize_subdomains_stats(output_file, data)
                    elif data_key == "common_words" and lock_name == "common_words_lock":
                        self._finalize_top50_common_words(output_file, data)

            output_file.write("\n\n\n")
            output_file.flush()
            os.fsync(output_file.fileno())
            fcntl.flock(output_file, fcntl.LOCK_UN)


    def _finalize_top50_common_words(self, output_file, data):
        top50_common_words = dict(sorted(data.items(), key=lambda item: item[1], reverse=True)[:50])
        output_file.write("Top 50 Common Words:\n")
        for word, count in top50_common_words.items():
            output_file.write(f"{word}: {count}\n")
        output_file.write("-------------------------------\n")


    def _finalize_subdomains_stats(self, output_file, data):
        sorted_data = dict(sorted(data.items(), key=lambda item: item[1], reverse=True))
        output_file.write("Subdomains Stats:\n")
        for subdomain, count in sorted_data.items():
            output_file.write(f"{subdomain}: {count}\n")
        output_file.write("-------------------------------\n")


    def _finalize_longest_content_page(self, output_file, data):
        output_file.write("Longest Content Page:\n")
        output_file.write("URL: " + str(data.get("url")) + "\n")
        output_file.write("Word Count: " + str(data.get("word_count")) + "\n")
        output_file.write("-------------------------------\n")


    def _finalize_unique_pages_count(self, output_file, data):
        output_file.write("Unique Pages Count: " + str(len(data)) + "\n")
        output_file.write("-------------------------------\n")


# ds = DataStorage()
# ds.visited_url.update({'url1', 'url2', 'url3'})
# ds.longest_page = {"url": "url2", "word_count": 1500}
# ds.common_words = {'word1': 10, 'word2': 20, 'word3': 30}
# ds.subdomains = {'subdomain1': 5, 'subdomain2': 10}
#
# ds.store_scraped_data()
# ds.finalize_data()
#
# ds.visited_url.update({'url4', 'url5', 'url6'})
# ds.longest_page = {"url": "url5", "word_count": 9500}
# ds.common_words = {'word1': 1000, 'word4': 90, 'word5': 300}
# ds.subdomains = {'subdomain3': 55, 'subdomain2': 990}
#
# ds.store_scraped_data()
# ds.finalize_data()
#
# ds.visited_url.update({'url7', 'url8', 'url9'})
# ds.longest_page = {"url": "url7", "word_count": 100}
# ds.common_words = {'word6': 10, 'word7': 20, 'word8': 30}
# ds.subdomains = {'subdomain1': 5, 'subdomain4': 10}
#
# ds.store_scraped_data()
# ds.finalize_data()
#
# ds.visited_url.update({'url10', 'url11', 'url12'})
# ds.longest_page = {"url": "url10", "word_count": 5000}
# ds.common_words = {'word9': 10, 'word1': 99999999, 'word2': 3304923840}
# ds.subdomains = {'subdomain1': 55465432, 'subdomain5': 9990}
#
# ds.store_scraped_data()
# ds.finalize_data()
#
#
# ds.visited_url.add('url13')
#
# ds.store_scraped_data()
# ds.finalize_data()
