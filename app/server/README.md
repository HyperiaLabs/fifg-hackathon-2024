# Backend

## Overview

Due to the short duration of the hackathon, the server does not use our AI model. 

## Usage

### Start

```
uvicorn main:app --reload
```

### Speeding up development


To enable the reading of transactions from local storage instead of the network, you should create a .env file and specify the path to the folder using the following variable:

```
TRANSACTIONS_CACHE_PATH="path_to_folder"
```

Within this designated folder, you should store transaction data in individual files named after their corresponding addresses. For example, you may have a file named `erd1hdejmz9shgquyk6k85da5r9rvmk49p4lts8z9pvc3fd5kku78qrsrusact.json`, which contains all the relevant transactions.


### Documentation

The Swagger documentation is available at [https://tkyc-frontend-8l0q.onrender.com/docs](https://tkyc-frontend-8l0q.onrender.com/docs)

## Demo
![demo](.readme_assets/docs.png)
