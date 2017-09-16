# In case a hurricane hits New York...
from base_scraper import BaseDeltaScraper

import requests
import csv
from pyproj import Proj, transform


class NewYorkShelters(BaseDeltaScraper):
    record_key = 'BLDG_ID'
    filepath = 'new-york-shelters.json'
    url = 'https://maps.nyc.gov/hurricane/data/center.csv'
    source_url = 'https://maps.nyc.gov/hurricane/'
    noun = 'shelter'

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
        from_projection = Proj(init='epsg:2263', preserve_units=True)
        to_projection = Proj(proj='latlong', ellps='WGS84', datum='WGS84')
        for row in rows:
            shelter = dict(zip(headers, row))
            if not shelter:
                continue
            # Convert from epsg:2263 - preserve_units=True because this is in feet
            x, y = shelter['X'], shelter['Y']
            longitude, latitude = transform(
                from_projection, to_projection, x, y
            )
            shelter['longitude'] = longitude
            shelter['latitude'] = latitude
            shelters.append(shelter)
        return shelters
