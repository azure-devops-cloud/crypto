import requests
import pandas as pd
import time

def fetch_crypto_data_dynamic(symbol, interval, days_back):
    url = "https://api.binance.com/api/v3/klines"
    limit = 1000  # Max limit for one request
    end_time = int(time.time() * 1000)  # Current timestamp in ms
    start_time = int((pd.Timestamp.now() - pd.Timedelta(days=days_back)).timestamp() * 1000)

    all_data = []
    while True:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
            "startTime": start_time,
            "endTime": end_time
        }
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception(f"Error {response.status_code}: {response.text}")

        batch_data = response.json()
        print(f"Fetched {len(batch_data)} entries in this batch.")
        if not batch_data:
            break

        all_data.extend(batch_data)
        start_time = batch_data[-1][6]  # Update start_time to the last returned CloseTime

        # If the last batch is less than the limit, we've reached the end
        if len(batch_data) < limit:
            break

    # Create DataFrame and exclude CloseTime and Ignore columns
    df = pd.DataFrame(all_data, columns=[
        'OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume',
        'CloseTime', 'QuoteVolume', 'Trades', 'TakerBase', 'TakerQuote', 'Ignore'
    ])
    
    # Remove 'CloseTime' and 'Ignore' columns
    df = df.drop(columns=['CloseTime', 'Ignore'])
    
    # Convert numerical columns to float for better representation
    df['OpenTime'] = pd.to_datetime(df['OpenTime'], unit='ms')
    numerical_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'QuoteVolume', 'TakerBase', 'TakerQuote']
    df[numerical_columns] = df[numerical_columns].astype(float)
    
    return df

# User Inputs
symbol = input("Enter the cryptocurrency pair (e.g., BTCUSDT): ").strip()
interval = input("Enter the time interval (e.g., 1h, 4h, 1d): ").strip()
days_back = int(input("Enter the number of days back to fetch data: "))

# Fetch Data
try:
    crypto_data = fetch_crypto_data_dynamic(symbol, interval, days_back)
    print(f"Fetched {len(crypto_data)} rows of data for {symbol} ({interval}).")
    print(crypto_data.head())

    # Export to CSV
    output_file = f"{symbol}_{interval}_data_no_closetime_ignore.csv"
    crypto_data.to_csv(output_file, index=False)
    print(f"Data successfully exported to {output_file}.")

except Exception as e:
    print(f"Error: {e}")
