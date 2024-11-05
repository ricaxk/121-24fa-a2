import signal
from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler


def sigterm_handler(signum, frame, crawler):
    print("Received SIGTERM, stopping all working threads...")
    crawler.stop()
    print("All working threads stopped.")


def main(config_file, restart):
    try:
        cparser = ConfigParser()
        cparser.read(config_file)
        config = Config(cparser)
        config.cache_server = get_cache_server(config, restart)
        crawler = Crawler(config, restart)
        signal.signal(signal.SIGTERM, lambda signum, frame: sigterm_handler(signum, frame, crawler))
        crawler.start()
    except KeyboardInterrupt:
        print("The crawler was interrupted and is being cleaned up...")
        crawler.stop()
        print("Clearance complete, program exited.")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
