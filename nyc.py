# In case a hurricane hits New York...
from base_scraper import BaseDeltaScraper

import requests
import csv


class NewYorkShelters(BaseDeltaScraper):
    record_key = 'BLDG_ID'
    filepath = 'new-york-shelters.json'
    url = 'https://maps.nyc.gov/hurricane/data/center.csv'
    source_url = 'https://maps.nyc.gov/hurricane/'
    noun = 'shelter'
    test_mode = True

    def display_record(self, record):
        display = []
        display.append('  %s' % record['BLDG_ADD'])
        display.append('    Accessible: %s' % record['ACCESSIBLE'])
        if record['ACC_FEAT']:
            display.append('    %s' % record['ACC_FEAT'])
        display.append('')
        return '\n'.join(display)

    def fetch_data(self):
        data = requests.get(self.url).content
        rows = csv.reader(data.split('\r\n'))
        headers = next(rows)
        shelters = []
        for row in rows:
            shelter = dict(zip(headers, row))
            if shelter:
                shelters.append(shelter)
        return shelters
