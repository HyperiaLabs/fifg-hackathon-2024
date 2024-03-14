import json
import requests
import time
from libs.exceptions import TooManyRequests, TooFewTransactions, TooMuchTransaction

session = requests.Session()


def get_request_content(url, query):
    headers = {"Content-Type": "application/json"}

    SLEEP_TIME = 1
    MAX_RETRY = 1
    retry_count = 0

    while retry_count < MAX_RETRY:
        response = session.post(url, headers=headers, data=json.dumps(query))

        if response.status_code == 200:
            return response.json()
        if response.status_code == 429:
            time.sleep(SLEEP_TIME)
        else:
            response.raise_for_status()

        retry_count += 1

    raise TooManyRequests()


def get_request_content_scroll(url, query, minimum_data=None, maximum_data=None):
    # Initialize by sending a request to the provided URL
    data = get_request_content(url, query)
    # Extract the total number of hits
    total_data = data["hits"]["total"]["value"]

    if minimum_data is not None and total_data < minimum_data:
        raise TooFewTransactions()

    if maximum_data is not None and total_data > maximum_data:
        raise TooMuchTransaction()

    scroll_id = data["_scroll_id"]
    all_data = data["hits"]["hits"]

    # Keep scrolling and collecting the data until we have collected all hits
    while len(all_data) < total_data:
        query = {"scroll": "1m", "scroll_id": scroll_id}
        data = get_request_content("https://index.multiversx.com/_search/scroll", query)
        scroll_id = data["_scroll_id"]
        all_data += data["hits"]["hits"]
    return all_data


def get_all_traded_tokens(address, minimum_data=None, maximum_data=None):

    url = "https://index.multiversx.com/transactions/_search?scroll=1m&size=10000"
    query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {e: address}} for e in ["sender", "receiver", "receivers"]
                ]
            }
        },
        "sort": [{"timestamp": {"order": "desc"}}],
        "track_total_hits": True,
        "_source": ["tokens"],
    }

    data = get_request_content_scroll(url, query, minimum_data, maximum_data)

    traded_tokens = []

    for item in data:
        if "tokens" in item["_source"]:
            # traded_tokens.extend(item["_source"]["tokens"])
            traded_tokens.append(item["_source"]["tokens"][0])

    return traded_tokens
