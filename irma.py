from common import Scraper
from BeautifulSoup import BeautifulSoup as Soup
import requests
import os
import time


class BaseScraper(Scraper):
    owner = 'simonw'
    repo = 'irma-scraped-data'
    committer = {
        'name': 'irma-scraper',
        'email': 'irma-scraper@example.com',
    }


class FemaOpenShelters(BaseScraper):
    filepath = 'fema-open-shelters.json'
    url = 'https://gis.fema.gov/REST/services/NSS/OpenShelters/MapServer/0/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry=%7B%22xmin%22%3A-10018754.171396945%2C%22ymin%22%3A2504688.5428529754%2C%22xmax%22%3A-7514065.628548954%2C%22ymax%22%3A5009377.085700965%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100'

    def fetch_data(self):
        data = requests.get(self.url).json()
        shelters = [feature['attributes'] for feature in data['features']]
        shelters.sort(key=lambda s: s['OBJECTID'])
        return shelters


class FemaNSS(FemaOpenShelters):
    filepath = 'fema-nss.json'
    url = 'https://gis.fema.gov/REST/services/NSS/FEMA_NSS/MapServer/0/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry=%7B%22xmin%22%3A-10018754.171396945%2C%22ymin%22%3A2504688.5428529754%2C%22xmax%22%3A-7514065.628548954%2C%22ymax%22%3A5009377.085700965%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100'


class IrmaShelters(BaseScraper):
    filepath = 'irma-shelters.json'
    url = 'https://irma-api.herokuapp.com/api/v1/shelters'

    def fetch_data(self):
        data = requests.get(self.url).json()
        shelters = data['shelters']
        shelters.sort(key=lambda s: s['shelter'])
        return shelters


def is_heading(tr):
    return tr.findAll('td')[1].text == 'Shelter Name'


def is_shelter(tr):
    return len(tr.findAll('td')) == 4 and not is_heading(tr)


def is_county_heading(tr):
    if tr.find('td').get('colspan') == '5' and (u'#d4d4d4' in tr.find('td').get('style', '')) and tr.text != '&nbsp;':
        return tr.text
    else:
        return None


class FloridaDisasterShelters(BaseScraper):
    filepath = 'florida-shelters.json'
    url = 'http://www.floridadisaster.org/shelters/summary.aspx'

    def fetch_data(self):
        r = requests.get(self.url)
        if r.status_code != 200:
            print "Oh no - status code = %d" % r.status_code
            return None
        table = Soup(r.content).findAll('table')[9]
        current_county = None
        shelters = []
        for tr in table.findAll('tr'):
            heading = is_county_heading(tr)
            if heading:
                current_county = heading
            if is_shelter(tr):
                shelters.append({
                    'type': tr.findAll('td')[0].text,
                    'county': current_county.title(),
                    'name': tr.findAll('td')[1].text,
                    'address': tr.findAll('td')[2].text,
                    'map_url': tr.findAll('td')[2].find('a')['href'].split(' ')[0],
                    'city': tr.findAll('td')[3].text,
                })
        shelters.sort(key=lambda s: (s['county'], s['name']))
        return shelters


if __name__ == '__main__':
    github_token = os.environ['GITHUB_API_TOKEN']
    scrapers = [
        FemaOpenShelters(github_token),
        FemaNSS(github_token),
        IrmaShelters(github_token),
        FloridaDisasterShelters(github_token),
    ]
    while True:
        for scraper in scrapers:
            scraper.scrape_and_store()
        time.sleep(60)
