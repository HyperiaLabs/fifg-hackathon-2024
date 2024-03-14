from fastapi import FastAPI, HTTPException
import pandas as pd
from libs.mvx_network import get_all_traded_tokens
from libs.exceptions import TooManyRequests, TooFewTransactions, TooMuchTransaction
from libs.misc import get_valid_api_keys, remove_after_second_dash

app = FastAPI()

api_keys = get_valid_api_keys()


@app.get("/{api_key}/mvx/profile/{address}")
async def root(api_key, address):

    if api_key not in api_keys:
        raise HTTPException(status_code=400, detail="Invalid API key")

    try:

        traded_tokens = get_all_traded_tokens(
            address, minimum_data=100, maximum_data=10000
        )

        # we remove nonce of nfts
        traded_tokens = [remove_after_second_dash(s) for s in traded_tokens]

        df_traded_tokens = pd.DataFrame(traded_tokens, columns=["token"])

        tokens_tags_df = pd.read_csv("./files/mvx-tokens-tags.csv", sep=";")
        tokens_tags_df = tokens_tags_df[["Catégorie - 1", "TICKER"]]

        df_traded_tokens = pd.merge(
            df_traded_tokens,
            tokens_tags_df,
            left_on="token",
            right_on="TICKER",
            how="left",
        )

        df_traded_tokens = df_traded_tokens[df_traded_tokens["Catégorie - 1"].notna()]
        category_percentage = (
            df_traded_tokens["Catégorie - 1"].value_counts() / df_traded_tokens.shape[0]
        ) * 100

        return {"repartition": category_percentage}

    except TooFewTransactions as exc:
        raise HTTPException(
            status_code=400, detail="Too few transactions to analyze profile"
        ) from exc
    except TooMuchTransaction as exc:
        raise HTTPException(
            status_code=400, detail="Too many transactions to analyze profile"
        ) from exc
    except TooManyRequests as exc:
        raise HTTPException(status_code=429) from exc
