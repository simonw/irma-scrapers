from common import Scraper
from BeautifulSoup import BeautifulSoup as Soup
import Geohash
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
    slack_channel = '#shelter_scraper_data'


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


class PascoCounty(BaseScraper):
    # From http://www.pascocountyfl.net/index.aspx?NID=2816
    # in particular this iframe:
    # https://secure.pascocountyfl.net/sheltersdisplay
    filepath = 'pascocountyfl.json'
    url = 'https://secure.pascocountyfl.net/SheltersDisplay/Home/GetShelterInfo'

    def create_message(self, new_data):
        return self.update_message([], new_data, verb='Created')

    def update_message(self, old_data, new_data, verb='Updated'):
        def name(n):
            return '%s (Pasco County FL)' % n['Name']

        current_names = [name(n) for n in new_data]
        previous_names = [name(n) for n in old_data]
        message = update_message_from_names(
            current_names,
            previous_names,
            self.filepath,
            verb=verb
        )
        message += '\n\nChange detected on http://www.pascocountyfl.net/index.aspx?NID=2816'
        return message

    def fetch_data(self):
        data = requests.post(self.url).json()
        data.sort(key=lambda d: d['Name'])
        return data


class LedgerPolkCounty(BaseScraper):
    filepath = 'ledger-polk-county.json'
    url = 'http://www.ledgerdata.com/hurricane-guide/shelter/'

    def create_message(self, new_data):
        return self.update_message([], new_data, verb='Created')

    def update_message(self, old_data, new_data, verb='Updated'):
        current_names = [n['name'] for n in new_data]
        previous_names = [n['name'] for n in old_data]

        added_names = [name for name in current_names if name not in previous_names]
        removed_names = [name for name in previous_names if name not in current_names]

        message = []
        for name in added_names:
            shelter = [n for n in new_data if n['name'] == name][0]
            message.append('Added shelter: %s, %s' % (
                shelter['name'], shelter['city']
            ))
            message.append('  %s' % shelter['url'])
        if added_names and removed_names:
            message.append('')
        for name in removed_names:
            shelter = [n for n in old_data if n['name'] == name][0]
            message.append('Removed shelter: %s, %s' % (
                shelter['name'], shelter['city']
            ))
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
            summary_text = '%s %s: %s' % (
                verb, self.filepath, (', '.join(summary))
            )
        else:
            summary_text = '%s %s' % (verb, self.filepath)
        return '%s\n\n%s\n\nChange detected on %s' % (
            summary_text, body, self.url
        )

    def fetch_data(self):
        s = Soup(requests.get(self.url).content)
        trs = s.find('table').findAll('tr')[1:]
        shelters = []
        for tr in trs:
            tds = tr.findAll('td')
            shelters.append({
                'name': tds[1].getText(),
                'url': 'http://www.ledgerdata.com/' + tds[1].find('a')['href'],
                'city': tds[2].getText(),
                'type': tds[3].getText(),
            })
        return shelters


class IrmaShelters(BaseScraper):
    filepath = 'irma-shelters.json'
    url = 'https://irma-api.herokuapp.com/api/v1/shelters'
    slack_channel = None

    def update_message(self, old_data, new_data):
        def name(n):
            return '%s (%s)' % (n['shelter'], n['county'])

        current_ids = [n['id'] for n in new_data]
        previous_ids = [n['id'] for n in old_data]

        added_ids = [id for id in current_ids if id not in previous_ids]
        removed_ids = [id for id in previous_ids if id not in current_ids]

        message = []
        for id in added_ids:
            shelter = [n for n in new_data if n['id'] == id][0]
            message.append('Added shelter: %s' % name(shelter))
        if added_ids:
            message.append('')
        for id in removed_ids:
            shelter = [n for n in old_data if n['id'] == id][0]
            message.append('Removed shelter: %s' % name(shelter))
        body = '\n'.join(message)
        summary = []
        if added_ids:
            summary.append('%d shelter%s added' % (
                len(added_ids), '' if len(added_ids) == 1 else 's',
            ))
        if removed_ids:
            summary.append('%d shelter%s removed' % (
                len(removed_ids), '' if len(removed_ids) == 1 else 's',
            ))
        if summary:
            summary_text = self.filepath + ': ' + (', '.join(summary))
        else:
            summary_text = 'Updated %s' % self.filepath
        return summary_text + '\n\n' + body

    def fetch_data(self):
        data = requests.get(self.url).json()
        shelters = data['shelters']
        shelters.sort(key=lambda s: s['shelter'])
        return shelters


class IrmaShelterDupes(BaseScraper):
    # Detect possible dupes in irma-api
    filepath = 'irma-shelters-dupes.json'
    url = 'https://irma-api.herokuapp.com/api/v1/shelters'

    def update_message(self, old_data, new_data):
        previous_geohashes = [
            dupe_group['geohash'] for dupe_group in old_data['dupe_groups']
        ]
        current_geohashes = [
            dupe_group['geohash'] for dupe_group in new_data['dupe_groups']
        ]
        added_geohashes = [
            geohash for geohash in current_geohashes if geohash not in previous_geohashes
        ]
        removed_geohashes = [
            geohash for geohash in previous_geohashes if geohash not in current_geohashes
        ]

        message = []
        for geohash in added_geohashes:
            dupe_group = [group for group in new_data['dupe_groups'] if group['geohash'] == geohash][0]
            message.append('New potential duplicates:')
            for shelter in dupe_group['shelters']:
                message.append('  ' + shelter['name'])
                if shelter.get('address'):
                    message.append('    ' + shelter['address'])
                message.append('    ' + shelter['google_maps'])
                message.append('    ' + shelter['view_url'])
                message.append('')

        if added_geohashes and removed_geohashes:
            message.append('')

        for geohash in removed_geohashes:
            dupe_group = [group for group in old_data['dupe_groups'] if group['geohash'] == geohash][0]
            message.append('This previous duplicate looks to be resolved:')
            for shelter in dupe_group['shelters']:
                message.append('  ' + shelter['name'])
                if shelter.get('address'):
                    message.append('    ' + shelter['address'])
                message.append('    ' + shelter['google_maps'])
                message.append('    ' + shelter['view_url'])
                message.append('')

        current_no_latlon_ids = [
            shelter['id'] for shelter in new_data['no_latitude_longitude']
        ]
        # Older data in our repo doesn't have the 'id' property, so we
        # have to allow it to be None here
        previous_no_latlon_ids = [
            shelter.get('id') for shelter in old_data['no_latitude_longitude']
        ]

        new_no_latlon_ids = [
            id for id in current_no_latlon_ids
            if id not in previous_no_latlon_ids
        ]
        resolved_no_latlon_ids = [
            id for id in previous_no_latlon_ids
            if id not in current_no_latlon_ids
            and id is not None
        ]

        if new_no_latlon_ids:
            message.append('')
            message.append('New shelters detected with no latitude/longitude:')
            for id in new_no_latlon_ids:
                shelter = [
                    s for s in new_data['no_latitude_longitude']
                    if s['id'] == id
                ][0]
                message.append('    ' + shelter['name'])
                if shelter.get('address'):
                    message.append('    ' + shelter['address'])
                message.append('    ' + shelter['view_url'])
                message.append('')

        if resolved_no_latlon_ids:
            message.append('')
            message.append('Fixed shelters that had no latitude/longitude:')
            for id in resolved_no_latlon_ids:
                shelter = [
                    s for s in old_data['no_latitude_longitude']
                    if s['id'] == id
                ][0]
                message.append('  ' + shelter['name'])
                message.append('  ' + (shelter.get('address') or ''))
                message.append('  ' + shelter['view_url'])

        body = '\n'.join(message)
        summary = []
        if added_geohashes:
            summary.append('%d new dupe%s detected' % (
                len(added_geohashes), '' if len(added_geohashes) == 1 else 's',
            ))
        if removed_geohashes:
            summary.append('%d dupe%s resolved' % (
                len(removed_geohashes), '' if len(removed_geohashes) == 1 else 's',
            ))
        if new_no_latlon_ids:
            summary.append('%d new no-lat-lon shelter%s' % (
                len(new_no_latlon_ids), '' if len(new_no_latlon_ids) == 1 else 's',
            ))
        if resolved_no_latlon_ids:
            summary.append('%d fixed no-lat-lon shelter%s' % (
                len(resolved_no_latlon_ids), '' if len(resolved_no_latlon_ids) == 1 else 's',
            ))
        if summary:
            summary_text = self.filepath + ': ' + (', '.join(summary))
        else:
            summary_text = 'Updated %s' % self.filepath
        return summary_text + '\n\n' + body

    def fetch_data(self):
        data = requests.get(self.url).json()
        shelters = data['shelters']
        # Scan for potential dupes by lat/lon (using geohash)
        by_geohash = {}
        for shelter in shelters:
            geohash = Geohash.encode(
                shelter['latitude'],
                shelter['longitude'],
                precision=8
            )
            by_geohash.setdefault(geohash, []).append(shelter)
        dupe_groups = [
            pair for pair in by_geohash.items()
            if len(pair[1]) > 1 and pair[0] != '00000000'
        ]
        no_latlons = by_geohash.get('00000000') or []
        return {
            'dupe_groups': [{
                'geohash': dupe_group[0],
                'shelters': [{
                    'id': shelter['id'],
                    'name': shelter['shelter'],
                    'address': shelter['address'],
                    'latitude': shelter['latitude'],
                    'longitude': shelter['longitude'],
                    'google_maps': 'https://www.google.com/maps/search/%(latitude)s,%(longitude)s' % shelter,
                    'view_url': 'https://irma-api.herokuapp.com/shelters/%s' % shelter['id'],
                } for shelter in dupe_group[1]],
            } for dupe_group in dupe_groups],
            'no_latitude_longitude': [{
                'id': shelter['id'],
                'name': shelter['shelter'],
                'address': shelter['address'],
                'view_url': 'https://irma-api.herokuapp.com/shelters/%s' % shelter['id'],
            } for shelter in no_latlons]
        }


def update_message_from_names(current_names, previous_names, filepath, verb='Updated'):
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
        summary_text = '%s %s' % (verb, filepath)
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
        def name(n):
            return '%s (%s County)' % (n['name'], n['county'])

        current_names = [name(n) for n in new_data]
        previous_names = [name(n) for n in old_data]
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


class CrowdSourceRescue(BaseScraper):
    filepath = 'crowdsourcerescue.json'
    owner = 'simonw'
    repo = 'private-irma-data'
    slack_channel = None
    url = 'https://crowdsourcerescue.com/rescuees/searchApi/'

    def fetch_data(self):
        return requests.post(self.url, {
            'needstring': '',
            'lat_min': '23.882475192722612',
            'lat_max': '29.761185051094046',
            'lng_min': '-86.76083325000002',
            'lng_max': '-77.97177075000002',
            'status': '0',
        }).json()


if __name__ == '__main__':
    github_token = os.environ['GITHUB_API_TOKEN']
    slack_token = os.environ['SLACK_TOKEN']
    scrapers = [
        klass(github_token, slack_token)
        for klass in (
            FemaOpenShelters,
            # FemaNSS,
            IrmaShelters,
            IrmaShelterDupes,
            FloridaDisasterShelters,
            ZeemapsScraper,
            PascoCounty,
            CrowdSourceRescue,
            LedgerPolkCounty,
        )
    ]
    while True:
        for scraper in scrapers:
            scraper.scrape_and_store()
        time.sleep(60)
