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
    slack_botname = 'Irma Scraper'
    slack_channel = '#shelters'


class FemaOpenShelters(BaseScraper):
    filepath = 'fema-open-shelters.json'
    url = 'https://gis.fema.gov/REST/services/NSS/OpenShelters/MapServer/0/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry=%7B%22xmin%22%3A-10018754.171396945%2C%22ymin%22%3A2504688.5428529754%2C%22xmax%22%3A-7514065.628548954%2C%22ymax%22%3A5009377.085700965%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100'

    def update_message(self, old_data, new_data):
        new_objects = [o for o in new_data if not any(o2 for o2 in old_data if o2['OBJECTID'] == o['OBJECTID'])]
        removed_objects = [o for o in old_data if not any(o2 for o2 in new_data if o2['OBJECTID'] == o['OBJECTID'])]
        message = []

        def name(row):
            if 'COUNTY_PARISH' in row:
                return '%s (%s County)' % (row['SHELTER_NAME'], row['COUNTY_PARISH'].title())
            else:
                return '%s (%s, %s)' % (row['SHELTER_NAME'], row['CITY'].title(), row['STATE'])

        for new_object in new_objects:
            message.append('Added shelter: %s' % name(new_object))
        if new_objects:
            message.append('')
        for removed_object in removed_objects:
            message.append('Removed shelter: %s' % name(removed_object))
        if removed_objects:
            message.append('')
        num_updated = 0
        for new_object in new_data:
            old_object = [o for o in old_data if o['OBJECTID'] == new_object['OBJECTID']]
            if not old_object:
                continue
            old_object = old_object[0]
            if new_object != old_object:
                message.append('Updated shelter: %s' % name(new_object))
                num_updated += 1
        body = '\n'.join(message)
        summary = []
        if new_objects:
            summary.append('%d shelter%s added' % (
                len(new_objects), '' if len(new_objects) == 1 else 's',
            ))
        if removed_objects:
            summary.append('%d shelter%s removed' % (
                len(removed_objects), '' if len(removed_objects) == 1 else 's',
            ))
        if num_updated:
            summary.append('%d shelter%s updated' % (
                num_updated, '' if num_updated == 1 else 's',
            ))
        if summary:
            summary_text = self.filepath + ': ' + (', '.join(summary))
        else:
            summary_text = 'Updated %s' % self.filepath
        return summary_text + '\n\n' + body

    def fetch_data(self):
        data = requests.get(self.url).json()
        shelters = [feature['attributes'] for feature in data['features']]
        shelters.sort(key=lambda s: s['OBJECTID'])
        return shelters


class FemaNSS(FemaOpenShelters):
    filepath = 'fema-nss.json'
    url = 'https://gis.fema.gov/REST/services/NSS/FEMA_NSS/MapServer/0/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry=%7B%22xmin%22%3A-10018754.171396945%2C%22ymin%22%3A2504688.5428529754%2C%22xmax%22%3A-7514065.628548954%2C%22ymax%22%3A5009377.085700965%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100'


class ZeemapsScraper(BaseScraper):
    url = 'https://zeemaps.com/emarkers?g=2682928'
    filepath = 'zeemaps-2682928.json'
    slack_channel = None

    def fetch_data(self):
        data = requests.get(self.url).json()
        data.sort(key=lambda d: d['nm'])
        return data


class IrmaShelters(BaseScraper):
    filepath = 'irma-shelters.json'
    url = 'https://irma-api.herokuapp.com/api/v1/shelters'
    slack_channel = None

    def update_message(self, old_data, new_data):
        def name(n):
            return '%s (%s)' % (n['shelter'], n['county'])

        current_names = [name(n) for n in new_data]
        previous_names = [name(n) for n in old_data]
        return update_message_from_names(current_names, previous_names, self.filepath)

    def fetch_data(self):
        data = requests.get(self.url).json()
        shelters = data['shelters']
        shelters.sort(key=lambda s: s['shelter'])
        return shelters


def update_message_from_names(current_names, previous_names, filepath):
    added_names = [n for n in current_names if n not in previous_names]
    removed_names = [n for n in previous_names if n not in current_names]
    message = []
    for name in added_names:
        message.append('Added shelter: %s' % name)
    if added_names:
        message.append('')
    for name in removed_names:
        message.append('Removed shelter: %s' % name)
    body = '\n'.join(message)
    summary = []
    if added_names:
        summary.append('%d shelter%s added' % (
            len(added_names), '' if len(added_names) == 1 else 's',
        ))
    if removed_names:
        summary.append('%d shelter%s removed' % (
            len(removed_names), '' if len(removed_names) == 1 else 's',
        ))
    if summary:
        summary_text = filepath + ': ' + (', '.join(summary))
    else:
        summary_text = 'Updated %s' % filepath
    return summary_text + '\n\n' + body


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

    def update_message(self, old_data, new_data):
        current_names = [n['name'] for n in new_data]
        previous_names = [n['name'] for n in old_data]
        message = update_message_from_names(current_names, previous_names, self.filepath)
        message += '\n\nChange detected on %s' % self.url
        return message

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
    slack_token = os.environ['SLACK_TOKEN']
    scrapers = [
        klass(github_token, slack_token)
        for klass in (
            FemaOpenShelters,
            FemaNSS,
            IrmaShelters,
            FloridaDisasterShelters,
            ZeemapsScraper,
        )
    ]
    while True:
        for scraper in scrapers:
            scraper.scrape_and_store()
        time.sleep(60)
