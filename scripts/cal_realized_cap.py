import os
from dotenv import load_dotenv
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import pandas as pd
import time

def calculate_realized_balance_with_timestamp_and_return_df(group):
    virtual_coins = []  # This will hold tuples of (amount, price)
    data_for_df = []

    for _, transaction in group.iterrows():
        address = transaction['address']
        timestamp = transaction['minute']
        amount = transaction['degen_amount']
        price = transaction['price_at_time']

        if amount > 0:
            virtual_coins.append((amount, price))  # Incoming payment, add a new virtual coin
        else:
            amount = -amount  # Make the amount positive for outgoing payments
            virtual_coins.sort(key=lambda x: -x[0])  # Sort by amount descending

            for i, (virtual_coin_amount, _) in enumerate(virtual_coins):
                if amount >= virtual_coin_amount:
                    amount -= virtual_coin_amount
                    virtual_coins[i] = (0, price)  # This coin is now used up
                else:
                    virtual_coins[i] = (virtual_coin_amount - amount, price) # Update the virtual coin price to the current price
                    amount = 0
                    break

            virtual_coins = [coin for coin in virtual_coins if coin[0] > 0]  # Remove used up coins

        realized_balance = sum(coin_amount * coin_price for coin_amount, coin_price in virtual_coins)
        data_for_df.append({'address': address, 'timestamp': timestamp, 'realized_balance': realized_balance})

    return pd.DataFrame(data_for_df)


def main():
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path)
    dune = DuneClient.from_env()  # Setup Dune Python client

    max_attempts = 100  # Maximum number of attempts to run the query
    print("Starting to initiate virtual UTXO base table query execution...")
    for attempt in range(max_attempts):
        print("Attempt number:", attempt + 1)
        try:
            query_result_json = dune.run_query(
                query=QueryBase(query_id=3490743),  # https://dune.com/queries/3490743
                ping_frequency=10,
                performance='large'
            )
            query_result_df = pd.DataFrame.from_dict(query_result_json.result.rows)
            print("Successfully fetched virtual UTXO base table query result!")
            break
        except Exception as e:
            print(f"Attempt {attempt + 1} failed with error:", e)
            if attempt < max_attempts - 1:
                time.sleep(2)
            else:
                raise

    df = query_result_df

    # Convert 'minute' to datetime and sort the dataframe
    df['minute'] = pd.to_datetime(df['minute'])
    df.sort_values(by=['address', 'minute'], inplace=True)
    grouped = df.groupby('address')  # Group by address

    print("Starting to calculate realized cap / balance...")
    start_time = time.time()
    result_dfs = [calculate_realized_balance_with_timestamp_and_return_df(group) for _, group in grouped]
    end_time = time.time()
    print(f"Calculating realized cap / balance took {end_time - start_time} seconds to complete.")

    # Concatenate all the individual DataFrames into a single one
    final_df = pd.concat(result_dfs, ignore_index=True)
    
    # file_path = 'degen_realized_balance.csv'
    print("Saving the result to a CSV file...")
    file_path = os.path.join(os.path.dirname(__file__), '..', 'uploads/', 'degen_realized_balance.csv')
    final_df.to_csv(file_path, index=False)
    
    print("Uploading realized balance to Dune...")
    try:
        with open(file_path) as file:
            data = file.read()
            try:
                table = dune.upload_csv(
                    data=data,
                    table_name="degen_realized_balance",
                    is_private=False
                )
                print("Uploaded degen_realized_balance to Dune!")
            except Exception as e:
                print("Failed to upload CSV to Dune:", e)
    except FileNotFoundError:
        print(f"File {file_path} not found.")
    except Exception as e:
        print("An error occurred:", e)


if __name__ == '__main__':
    main()
