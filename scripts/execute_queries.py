import os
import yaml
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dotenv import load_dotenv
import sys
import codecs

# !!! This script is not used, just a proof of concept I wanted to save

# Set the default encoding to UTF-8
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

dune = DuneClient.from_env()

# Read the queries.yml file
queries_yml = os.path.join(os.path.dirname(__file__), '..', 'queries.yml')
with open(queries_yml, 'r', encoding='utf-8') as file:
    data = yaml.safe_load(file)

# Extract the query_ids from the data
query_ids = [id for id in data['query_ids']]

for id in query_ids:
    query = dune.get_query(id)
    print('Triggering execution for query {}, {}'.format(query.base.query_id, query.base.name))
    try:
        dune.execute_query(
            QueryBase(query_id=3490743)
        )
        print('Query execution successful')
    except Exception as e:
        print("Failed to execute: ", e)
