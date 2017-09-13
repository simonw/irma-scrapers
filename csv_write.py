import requests
import pandas as pd


def fetch_json(url):
    return requests.get(url).json()


def fetch_api_data(url):
    data = fetch_json(url)
    j_data = data['shelters']
    return pd.DataFrame(j_data)


def fetch_scraped_data(url):
    j_data = fetch_json(url)
    return pd.DataFrame(j_data)


if __name__ == '__main__':
    url = 'https://irma-api.herokuapp.com/api/v1/shelters'
    df_api_shelters = fetch_api_data(url)
    print('irma-api shape = ', df_api_shelters.shape)
    df_api_shelters.to_csv('api.csv', encoding='utf-8')

    url = 'https://raw.githubusercontent.com/simonw/irma-scraped-data/master/irma-shelters.json'
    df_scraped_shelters = fetch_scraped_data(url)
    print('irma-scraped shape = ', df_scraped_shelters.shape)
    df_scraped_shelters.to_csv('scraped.csv', encoding='utf-8')
