import requests
import pandas as pd
import pprint
pp = pprint.PrettyPrinter(indent=4)


def list_contents(url):
    contents = []
    data = fetch_json(url)
    contents.append([f['name'] for f in data])
    return contents[0]


def fetch_json(url):
    return requests.get(url).json()


def fetch_data(url, key=None):
    data = fetch_json(url)
    if isinstance(data, list):
        return pd.DataFrame(data)
    else:
        data = data[key]
        return pd.DataFrame(data)


def df_from_list(df_data):
    return pd.DataFrame(df_data)


if __name__ == '__main__':
    
    base_git_url = 'https://raw.githubusercontent.com/simonw/irma-scraped-data/master/'
    contents_url = 'https://api.github.com/repos/simonw/irma-scraped-data/contents/'

    git_contents = list_contents(contents_url)
    not_shelter_file = ['duke-fl-outages.json',
                        'duke-ncsc-outages.json',
                        'fpl-county-outages.json',
                        'fpl-storm-outages.json',
                        'georgiapower-outages.json',
                        'jemc-outages.json',
                        'north-georgia-outages.json',
                        'tampa-electric-outages.json',
                        'README.md',
                        'irma-shelters-dupes.json',
                        ]

    for c in git_contents:
        if c not in not_shelter_file:
            git_url = base_git_url + c
            df_shelter_data = fetch_data(git_url)
            print df_shelter_data.shape, 'is shape of ', c

    """
    # csv
    #df_scraped_shelters.to_csv('scraped.csv', encoding='utf-8')
    
    # api reader code
    url = 'https://irma-api.herokuapp.com/api/v1/shelters'
    key = 'shelters'
    df_api_shelters = fetch_data(url, key)
    print('irma-api shape = ', df_api_shelters.shape)
    #df_api_shelters.to_csv('api.csv', encoding='utf-8')
    """

