# https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-05-01_anthem_index.json.gz
import os.path
import typing
from io import TextIOWrapper, BufferedReader

import requests
from gzip import GzipFile
import ijson
import tqdm
import pandas as pd
import sys

URL = 'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-05-01_anthem_index.json.gz'

# I tested different buffer lengths when streaming the file from the URL. Here are the results:
# 1024 * 1024 (~1MB): Read 100000 file location objects in 31.5 seconds
# 1024 * 1024 * 10 (~10MB): Read 100000 file location objects in 24.3 seconds
# 1024 * 1024 * 100 (~100MB): Read 100000 file location objects in 14.5 seconds
# 1024 * 1024 * 150 (~150MB): Read 100000 file location objects in 22.5 seconds
# 1024 * 1024 * 1000 (~1GB): Read 100000 file location objects in 33.3 seconds
# This was pretty unscientific (I could tell it was changing when my internet connectivity changed), but it seems like
# 100MB is around the sweet spot.
BUFFER_SIZE = 1024 * 1024 * 100

# see notebooks/Explore Reporting Plans.ipynb for where I found these
NY_PPO_PLAN_NAMES = ['PPO NY - AUTEL. US INC. - ANTHEM',
                     'PPO NY - NEW YORK TECH BENEFITS PROGRAM - ANTHEM',
                     'PPO NY - METROPOLITAN COUNCIL ON JEWISH POVERTY - ANTHEM',
                     'PPO NY - HUDSON MX INC - ANTHEM',
                     'PPO NY - GUILD TIMES BENEFITS FUND - ANTHEM',
                     'PPO NY - DUTCHESS EDUCATION HEALTH INSURANCE CONSORTIUM - ANTHEM',
                     "PPO NY - SAINT ANN'S SCHOOL - ANTHEM",
                     'PPO NY - OASIS OUTSOURCING HOLDINGS INC. - ANTHEM',
                     'PPO NY - CEWW HEALTH INSURANCE CONSORTIUM - ANTHEM',
                     'PPO NY - UNITED FEDERATION OF TEACHERS STAFF & WELFARE PLAN - ANTHEM',
                     'PPO NY - LYRA TECHNOLOGIES, INC - DBA BLOCK RENOVATION - ANTHEM',
                     'PPO NY - COMMUNITY FEDERAL SAVINGS BANK - ANTHEM',
                     'PPO NY - MANHATTAN WEST LLC - ANTHEM',
                     'PPO NY - BALDOR SPECIALTY FOODS INC - ANTHEM',
                     'PPO NY - THE BERKSHIRE BANK - ANTHEM',
                     'PPO NY - OVERTIME SPORTS, INC - ANTHEM',
                     'PPO NY - DATAMINR INC - ANTHEM',
                     'PPO NY - RESTORIXHEALTH LLC - ANTHEM',
                     'PPO NY - DAMASCUS BAKERY INC - ANTHEM',
                     'PPO NY - IM PRO MAKEUP NY L.P. - ANTHEM',
                     'PPO NY - INTREPID MUSEUM FOUNDATION - ANTHEM',
                     'PPO NY - ADP TOTALSOURCE - ANTHEM',
                     'PPO NY - MAVIS TIRE SUPPLY CORP - ANTHEM',
                     'PPO NY - CAMBRIDGE UNIVERSITY PRESS - ANTHEM',
                     'PPO NY - WCM SERVICES LLC - ANTHEM',
                     'PPO NY - ASSOCIATION TO BENEFIT CHILDREN - ANTHEM',
                     'PPO NY - LIVEONNY - ANTHEM', 'PPO NY - BUZZFEED INC - ANTHEM',
                     'PPO NY - MGM YONKERS INC. - ANTHEM',
                     'PPO NY - SHENENDEHOWA CENTRAL SCHOOL DISTRICT - ANTHEM',
                     'PPO NY - TNTP, INC. - ANTHEM',
                     'PPO NY - BERLINROSEN HOLDINGS LLC - ANTHEM',
                     'PPO NY - JOY CONSTRUCTION CORPORATION - ANTHEM',
                     'PPO NY - BWD MILE DEVELOPMENT LLC - ANTHEM',
                     'PPO NY - NAACP LEGAL DEFENSE AND EDUCATIONAL FUND, INC. - ANTHEM',
                     'PPO NY - CHRISTIAN LOUBOUTIN LLC - ANTHEM',
                     'PPO NY - BALL CHAIN MANUFACTURING CO, INC. - ANTHEM',
                     'PPO NY - OELS ONONDAGA EMPLOYEE LEASING SERVICES - ANTHEM',
                     'PPO NY - BASIN HOLDINGS LLC - ANTHEM',
                     'PPO NY - GRAMERCY GROUP, INC. - ANTHEM',
                     'PPO NY - MAGNOLIA OPERATING LLC - ANTHEM',
                     'PPO NY - K HEALTH, INC. - ANTHEM',
                     'PPO NY - WONDER - ANTHEM']


def iterate_file_location_objects(text_stream: typing.TextIO):
    return ijson.items(text_stream, 'reporting_structure.item.in_network_files.item')


def iterate_reporting_structure(text_stream: typing.TextIO):
    return ijson.items(text_stream, 'reporting_structure.item')


def iterate_reporting_plan_objects(text_stream: typing.TextIO):
    return ijson.items(text_stream, 'reporting_structure.item.reporting_plans.item')


def extract_reporting_plans(text_stream: typing.TextIO):
    reporting_plan_ids_found = set()
    all_reporting_plans = []
    # use ijson to parse the json file. We want the path:
    # reporting_structure -> item
    for reporting_plan in iterate_reporting_plan_objects(text_stream):
        if 'plan_id' not in reporting_plan:
            print(f'Malformed reporting plan: {reporting_plan}')
            continue
        if reporting_plan['plan_id'] not in reporting_plan_ids_found:
            reporting_plan_ids_found.add(reporting_plan['plan_id'])
            # reporting plan objects should have:
            # plan_name, plan_id_type, plan_id, plan_market_type
            # fill missing fields with None
            all_reporting_plans.append({
                'plan_name': reporting_plan.get('plan_name'),
                'plan_id_type': reporting_plan.get('plan_id_type'),
                'plan_id': reporting_plan['plan_id'],
                'plan_market_type': reporting_plan.get('plan_market_type'),
            })
    print(f'Found {len(all_reporting_plans)} reporting plans')
    df = pd.DataFrame(all_reporting_plans)
    df.to_parquet('data/reporting_plans.parquet')


def extract_in_network_mrf_urls(text_stream: typing.TextIO, plan_names: set[str]):
    # the readme says that the output should be:
    # > the list of machine readable file URLs corresponding to Anthem's PPO in New York state
    # it's unclear based on this whether that includes the allowed amount files, too, but based on the earlier
    # line
    # > You should write code that can open the machine readable index file and extract some in-network file URLs
    # I'm assuming it does not include allowed amount files

    # use a set so that we don't load any duplicates into memory
    in_network_files = set()
    for reporting_structure in iterate_reporting_structure(text_stream):
        corresponding_plan_names = {
            str(plan['plan_name']) for plan in reporting_structure.get('reporting_plans', []) if 'plan_name' in plan
        }
        if corresponding_plan_names.intersection(plan_names):
            if 'in_network_files' in reporting_structure:
                for in_network_file in reporting_structure['in_network_files']:
                    in_network_files.add((in_network_file.get('description'), in_network_file.get('location')))

    print(f'Found {len(in_network_files)} in network file entries')
    df = pd.DataFrame(list(in_network_files), columns=['description', 'location'])
    df.to_parquet('data/in_network_files.parquet')
    return sorted(list(df['location']))


def stream_file_from_url(url: str):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get('content-length', 0))

    buffered_reader = BufferedReader(response.raw, buffer_size=1024 * 1024)
    return buffered_reader, total_size


def load_file_from_fs(file_path: str):
    f = open(file_path, 'rb')
    return f, os.path.getsize(file_path)


def main():
    file_location = sys.argv[1]
    if file_location.startswith('http'):
        file_stream, total_size = stream_file_from_url(file_location)
    else:
        # assume the file is on the local file system
        file_stream, total_size = load_file_from_fs(file_location)
    try:

        with tqdm.tqdm.wrapattr(file_stream, "read", total=total_size, desc="Processing") as stream:
            gzip_stream = GzipFile(fileobj=stream, mode='rb')

            text_stream = TextIOWrapper(gzip_stream, encoding='utf-8')

            # uncomment to extract reporting_plans.parquet file
            # extract_reporting_plans(text_stream)

            # uncomment to extract in network files
            urls = extract_in_network_mrf_urls(text_stream, set(NY_PPO_PLAN_NAMES))
            # write the urls to SOLUTION.txt
            with open('SOLUTION.txt', 'w') as f:
                for url in urls:
                    f.write(f'{url}\n')

    finally:
        # ensure the file is closed, since we aren't using a context manager
        file_stream.close()


# these are here for my reference when I was running the script
# '/Users/emmaknospe/Desktop/2024-05-01_anthem_index.json.gz'
# 'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-05-01_anthem_index.json.gz'

if __name__ == '__main__':
    main()
