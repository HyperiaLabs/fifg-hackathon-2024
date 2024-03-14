import pandas as pd


def flatten_json(data, parent_key='', sep='_'):
    """
    A function to flatten JSON objects.
    
    :param data: The data to be flattened
    :type data: dict
    :param parent_key: The key to be concatenated with.
    :type parent_key: str
    :param sep: Seperator between concatenated keys.
    :type sep: str
    :returns: The flattened dictionary.
    :rtype: dict
    """
    items = {}
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_json(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items

def flatten_transactions(transactions):
    pd.set_option('display.max_columns', None)


    TAGS_FILE = './files/MultiversX_-_etiquette-2.csv'
    TOKEN_FINAL = './files/token_final.csv'

    flattened_data = [flatten_json(item) for item in transactions]
    df = pd.DataFrame(flattened_data)

    df=df.drop(['_source_events'],axis=1)

    df2=pd.read_csv(TOKEN_FINAL)
    df = pd.merge(df, df2, left_on='_source_receiver', right_on='owner', how='left')

    df[df['ticker'].notna()]

    # Try to convert '_source_value' to numeric, if possible
    try:
        df['_source_value'] = pd.to_numeric(df['_source_value'], errors='coerce')
        conversion_success = True
    except Exception as e:
        conversion_success = False
        conversion_error = str(e)

    df = convert_list_to_columns(df)

    if "_source_tokens_1" not in df.columns:
        raise NoTokenError()
    
    df3=pd.read_csv(TAGS_FILE,sep=';')
    df3=df3.drop(['Catégorie - 2'],axis=1)
    df3=df3.drop(['Unnamed: 5'],axis=1)
    df3=df3.drop(['Unnamed: 6'],axis=1)

    df = pd.merge(df, df3, left_on='_source_tokens_1', right_on='TICKER', how='left')
    
    # Drop columns with more than 99% NaN values
    nan_threshold = 0.99 * len(df)
    columns_to_drop = df.columns[df.isna().sum() > nan_threshold].tolist()

    df_cleaned = df.drop(columns=columns_to_drop)

    return df_cleaned

def convert_list_to_columns(df):
    # Identify columns that contain string representations of lists or arrays
    potential_list_cols = []

    # Check if a string represents a list (by checking the first and last characters)
    def is_list_str(s):
        return isinstance(s, list) or (isinstance(s, str) and s.startswith("[") and s.endswith("]"))

    # Check each column for list strings
    for col in df.columns:    
        if any(df[col].apply(is_list_str).fillna(False)):
            potential_list_cols.append(col)            

    # Function to extract up to first 5 elements of a list
    def extract_up_to_5(lst):
        return lst[:5] + [None] * (5 - len(lst))

    # Apply transformations and create new columns
    for col in potential_list_cols:
        # Extract up to first 5 elements
        df[col] = df[col].apply(lambda x: extract_up_to_5(x) if isinstance(x, list) else [x] + [None] * 4)
        
        # Create new columns
        expanded_col = pd.DataFrame(df[col].tolist(), columns=[f"{col}_{i}" for i in range(1, 6)])
        df = pd.concat([df, expanded_col], axis=1)
        
        # Drop original column
        df = df.drop(columns=[col])

    return df

class NoTokenError(Exception):
    ...
    pass