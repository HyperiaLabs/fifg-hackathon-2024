## Note:
## This code is similar to https://github.com/MVX-TKYC/tools/tree/main/wallets_data_scrapper
## It could be great to merge these two code into a "common" package used by both repo (this one, and wallets_data_scrapper)

import json
import requests

def get_request_content(url, query):
    headers = {"Content-Type": "application/json"}

    print("Querying " + url)

    # Send a POST request to the provided URL with the given query
    # Return the JSON content of the response
    response = requests.post(url, headers=headers, data=json.dumps(query))

    response.raise_for_status()
    
    print("Got response in " + str(response.elapsed.total_seconds()) + "s from " + url)

    return response.json()


def get_request_content_scroll(url, query):
    # Initialize by sending a request to the provided URL
    data = get_request_content(url, query)
    # Extract the total number of hits
    total_data = data["hits"]["total"]["value"]

    if 100000 < total_data:
        print.write(f"Too much transactions ? ({total_data})")
        return
    else:
        scroll_id = data["_scroll_id"]
        all_data = data["hits"]["hits"]

    # Keep scrolling and collecting the data until we have collected all hits
    while len(all_data) < total_data:
        query = {"scroll": "1m", "scroll_id": scroll_id}
        data = get_request_content(
            "https://index.multiversx.com/_search/scroll", query)
        scroll_id = data["_scroll_id"]
        all_data += data["hits"]["hits"]
    return all_data


def get_all_transactions(wallet):
    
    url = "https://index.multiversx.com/transactions/_search?scroll=1m&size=10000"
    query = {"query": {"bool": {"should": [{"match": {e: wallet}} for e in [
        "sender", "receiver", "receivers"]]}}, "sort": [{"timestamp": {"order": "desc"}}], "track_total_hits": True}
    data = get_request_content_scroll(url, query)

    if not data:
        raise Exception("Too much transactions to get")        

    # Filter out the ids of transactions with results, operations, or logs
    ids = [e["_id"] for e in data if e["_source"].get(
        "hasScResults") or e["_source"].get("hasOperations") or e["_source"].get("hasLogs")]

    # Get all smart contract results related to the filtered transactions
    url = "https://index.multiversx.com/scresults/_search?scroll=1m&size=10000"
    query = {"query": {"bool": {"should": [
        {"terms": {"originalTxHash": ids}}]}}, "track_total_hits": True}
    scresults = get_request_content_scroll(url, query)

    scresults_dict = {}

    for scresult in scresults:
        tx = scresult["_source"]["originalTxHash"]
        if tx not in scresults_dict:
            scresults_dict[tx] = [scresult]
        else:
            scresults_dict[tx] += [scresult]

    # Get all logs related to the filtered transactions
    url = "https://index.multiversx.com/logs/_search?scroll=1m&size=10000"
    query = {"query": {"bool": {"should": [
        {"terms": {"originalTxHash": ids}}]}}, "track_total_hits": True}
    logs = get_request_content_scroll(url, query)

    logs_dict = {}
    for log in logs:
        tx = log["_source"]["originalTxHash"]
        if tx not in logs_dict:
            logs_dict[tx] = [log]
        else:
            logs_dict[tx] += [log]

    # Attach the corresponding smart contract results and logs to each transaction
    for i, transaction in enumerate(data):
        scresult = scresults_dict.get(transaction["_id"])
        if scresult:
            data[i]["_source"].setdefault("events", []).extend(scresult)
        log = logs_dict.get(transaction["_id"])
        if log:
            data[i]["_source"].setdefault("events", []).extend(log)

    return data