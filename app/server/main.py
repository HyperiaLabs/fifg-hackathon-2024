from fastapi import FastAPI, Response, status
from network import get_all_transactions
from converter import NoTokenError, flatten_transactions
from profile import calculate_profile
import os
import json
from dotenv import dotenv_values
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/profile/{address}")
async def address(address):    

    transactions = get_transactions(address)

    try:

        df = flatten_transactions(transactions)
        profile = calculate_profile(df)

        return {
            "address": address,
            "profile": profile
        }

    except NoTokenError:
        return {
            "address": address,
            "error_code": 1
        }

# Use cache or download transaction
def get_transactions(address):

    config = dotenv_values(".env")
    base_path_key = "TRANSACTIONS_CACHE_PATH"

    if base_path_key in config:
        base_path = config[base_path_key]
        path = os.path.join(base_path, address+".json")

        if os.path.exists(path):
            print("cache found")

            file = open(path, "r")
            transactions = json.load(file)
            file.close()  

            return transactions  
    

    return get_all_transactions(address)
