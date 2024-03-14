def get_valid_api_keys():
    return [line.strip() for line in open("files/api-keys.txt", "r", encoding="utf-8")]


# can remove nonce of a nft "COLLECTION-TICKER-NONCE"
def remove_after_second_dash(input_string: str):
    parts = input_string.split("-")

    if len(parts) >= 3:
        return "-".join(parts[:2])

    return input_string
