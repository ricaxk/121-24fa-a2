import re
from collections import defaultdict
from typing import List, Dict

import nltk

nltk.download('stopwords', quiet=True)
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

stop_words = set(stopwords.words('english'))
stop_words.update(
    ["na", "us", "a", "aren't", "can't", "couldn't", "didn't", "doesn't", "don't", "hadn't", "hasn't",
     "haven't", "he'd", "he'll", "he's", "i'd", "i'll", "i'm", "i've", "isn't", "it's", "let's", "mustn't",
     "shan't", "she'd", "she'll", "she's", "shouldn't", "that's", "there's", "they'd", "they'll", "they're",
     "they've", "wasn't", "we'd", "we'll", "we're", "we've", "weren't", "what's", "when's", "where's", "who's",
     "won't", "wouldn't", "you'd", "you'll", "you're", "you've"])


def filter_text(text):
    url_pattern = r"https?://\S+"
    text = re.sub(url_pattern, '', text)
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '', text)
    words = word_tokenize(text)
    filtered_words = [word for word in words if word.lower() not in stop_words and len(word) > 1]
    filtered_words = [word for word in filtered_words if 2 <= len(word) <= 15]
    return " ".join(filtered_words)


# From Assignment1 partA:
def tokenize(text: str) -> List[str]:
    """
    Tokenize the given text.

    :param text: The text to be tokenized.
    :return: A list of tokens extracted from the given text.

    Runtime Complexity:
    - For a text of length n, the regex findall operation is O(n) in the average case but can be O(n^2) in the worst case.
    - Converting the tokens to lowercase is O(n).
    - Overall, this function has a complexity of O(n).
    """
    pattern = r"\b[a-zA-Z]+\b"

    # O(n) for most cases, where n is the number of characters in the text.
    tokens = re.findall(pattern, text)

    # O(n) for list comprehension, where n is the number of characters in the text.
    return [token.lower() for token in tokens]


def compute_word_frequencies(tokens: List[str]) -> Dict[str, int]:
    """
    Compute the frequency of each token in the provided list.

    :param tokens: A list of tokens for which frequencies need to be computed.
    :return: A dictionary mapping each token to its frequency in the provided list.

    Runtime Complexity:
    - Initializing the defaultdict: O(1)
    - Looping through the list of tokens: O(n) where n is the number of tokens in the list.
    - For each token, updating the frequency in the defaultdict is an O(1) operation on average.
    - However, since it's done n times, it accumulates to O(n).
    - Therefore, the overall complexity for this function is the sum of the above operations: O(1) + O(n) = O(n).
    """
    frequencies = defaultdict(int)
    for token in tokens:  # O(n) where n is the number of tokens
        frequencies[token] += 1
    return frequencies


def print_frequencies(frequencies: Dict[str, int]) -> None:
    """
    Print the frequencies of the tokens in descending order.

    :param frequencies: A dictionary mapping each token to its frequency.
    :return: None

    Runtime Complexity:
    - Getting the keys from the dictionary: O(n) where n is the number of unique tokens.
    - Sorting the tokens based on their frequencies requires O(n*log(n)).
    - This is because the sorting operation is based on the Timsort algorithm in Python, which has a worst-case
        and average time complexity of O(n*log(n)).
    - Iterating through the sorted tokens and printing them: O(n).
    - Therefore, the overall complexity is dominated by the sorting operation, making it O(n + n*log(n)) = O(n*log(n)).
    """
    sorted_tokens = sorted(
        frequencies.keys(), key=lambda x: (-frequencies[x], x)
    )  # O(n*log(n))
    for token in sorted_tokens:  # O(n)
        print(f"{token} -> {frequencies[token]}")
