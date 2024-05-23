import pandas as pd
import re


def check_work():
    in_network_files = pd.read_parquet('data/in_network_files.parquet')
    in_network_file_locations = in_network_files['location'].values
    in_network_file_locations_cleaned = [location.split('?')[0] for location in in_network_file_locations]
    with open('data/ein_45-2320063_in_network_html.html', 'r') as f:
        manual_lookup_html = f.read()

    # extract links from the html
    html_links = re.findall(r'href=[\'"]?([^\'" >]+)', manual_lookup_html)
    html_links_cleaned = [link.split('?')[0] for link in html_links]
    for html_link in html_links_cleaned:
        if html_link not in in_network_file_locations_cleaned:
            print(f'Could not find {html_link} in the extracted in network files')
    for file_location in in_network_file_locations_cleaned:
        if file_location not in html_links:
            print(f'Could not find {file_location} in the manual lookup html')


if __name__ == '__main__':
    check_work()
