from urllib.parse import urlparse, urldefrag, parse_qsl, urlunparse, urlencode, urljoin

from bs4 import BeautifulSoup

from utils.text_processor import *

import pdb


MAX_URL_LENGTH = 200


def scraper(url, resp, data_storage) -> list:
    try:
        if resp is None:
            return []
        if resp.raw_response is None or resp.raw_response.content is None:
            return []
        if resp.status < 200 or resp.status >= 400:
            return []

        words_freq = extract_curr_content(resp)
        raw_sub_links = extract_next_links(url, resp)
        valid_sub_links = [link for link in raw_sub_links if (is_valid(link, data_storage))]

        with data_storage.longest_page_lock:
            update_longest_page(url, words_freq, data_storage.longest_page)

        with data_storage.common_words_lock:
            update_common_words(words_freq, data_storage.common_words)

        return valid_sub_links

    except Exception as e:
        print(f"An error occurred for URL: {url}")
        print(f"Error type: {type(e).__name__}, Error reason: {str(e)}")
        return []


def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    soup = BeautifulSoup(resp.raw_response.content, "lxml")
    next_links = [a["href"] for a in soup.find_all("a", href=True)]
    next_links = [urljoin(url, link) for link in next_links]
    next_links = [urldefrag(link)[0] for link in next_links]
    return next_links


def is_valid(url, data_storage=None) -> bool:
    # Decide whether to crawl this url or not.
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        canonicalized_url = canonicalize_url(url)

        if data_storage is not None:
            with data_storage.visited_url_lock:
                if url in data_storage.visited_url:
                    return False

        parsed = urlparse(canonicalized_url)
        if parsed.hostname is None:
            return False
        if parsed.scheme not in {"http", "https"}:
            return False
        if len(url) > MAX_URL_LENGTH:
            return False
        if not re.match(
                r"(.*\.ics\.uci\.edu.*)|"
                r"(.*\.cs\.uci\.edu.*)|"
                r"(.*\.informatics\.uci\.edu.*)|"
                r"(.*\.stat\.uci\.edu.*)|"
                r"(today\.uci\.edu/department/information_computer_sciences.*)",
                parsed.hostname,
        ):
            return False
        if re.match(
                r".*\.(css|js|bmp|gif|jpe?g|ico|swp"
                + r"|png|tiff?|mlid|mp2|mp3|mp4|tmp"
                + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|bak"
                + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|php"
                + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|asm|toml"
                + r"|epub|dll|cnf|tgz|sha1|exe|app|xml|json|jsx|h|hpp|yaml"
                + r"|thmx|mso|arff|rtf|jar|csv|py|java|jar|scss|c|war|ini"
                + r"|flv|mpg|3gp|flac|aac|svg|webp|odt|ods|odp|odg|sqlite"
                + r"|zpix|tar.gz|rar|7z|xz|sh|bat|dll|so|ttf|woff|eot|otf"
                + r"|rm|smil|wmv|swf|wma|zip|rar|gz|txt|img|sql|pdf|cpp)$",
                parsed.path.lower(),
        ):
            return False

        return True

    except TypeError:
        print("TypeError for ", parsed)
        raise


def extract_curr_content(resp) -> dict:
    soup = BeautifulSoup(resp.raw_response.content, "lxml")
    raw_text = soup.text
    processed_text = filter_text(raw_text)
    words = tokenize(processed_text)
    word_frequencies = compute_word_frequencies(words)
    return word_frequencies


def update_longest_page(url, words_freq, longest_page):
    current_word_count = sum(words_freq.values())
    longest_word_count = longest_page.get("word_count", 0)
    if current_word_count > longest_word_count:
        longest_page["url"] = url
        longest_page["word_count"] = current_word_count


def update_common_words(words_freq, common_words):
    for word, count in words_freq.items():
        common_words[word] = common_words.get(word, 0) + count


def canonicalize_url(url):
    parsed = urlparse(url)
    query_params = sorted(parse_qsl(parsed.query))
    canonical_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(query_params), ""))
    return canonical_url
