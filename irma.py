from base_scraper import BaseScraper
from irma_shelters import (
    IrmaShelters,
    IrmaShelterDupes,
)
from gis_scrapers import (
    FemaOpenShelters,
    FemaNSS,
    GemaAnimalShelters,
    GemaActiveShelters,
)

from BeautifulSoup import BeautifulSoup as Soup
import requests
import os
import time
import json


class ZeemapsScraper(BaseScraper):
    url = 'https://zeemaps.com/emarkers?g=2682928'
    filepath = 'zeemaps-2682928.json'
    slack_channel = None

    def fetch_data(self):
        data = requests.get(self.url).json()
        data.sort(key=lambda d: d['nm'])
        return data


class FplStormOutages(BaseScraper):
    filepath = 'fpl-storm-outages.json'
    url = 'https://www.fplmaps.com/data/storm-outages.js'
    slack_channel = None

    def fetch_data(self):
        content = requests.get(self.url).content
        # Stripe the 'define(' and ');'
        if content.startswith('define('):
            content = content.split('define(')[1]
        if content.endswith(');'):
            content = content.rsplit(');', 1)[0]
        return json.loads(content)


class FplCountyOutages(BaseScraper):
    filepath = 'fpl-county-outages.json'
    url = 'https://www.fplmaps.com/customer/outage/CountyOutages.json'
    slack_channel = None

    def fetch_data(self):
        return requests.get(self.url).json()


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
        message += '\nChange detected on http://www.pascocountyfl.net/index.aspx?NID=2816'
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
        return '%s\n\n%s\nChange detected on %s' % (
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


class HernandoCountyShelters(BaseScraper):
    filepath = 'hernando-county.json'
    url = 'http://www.hernandocounty.us/em/shelter-information'

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
            message.append('Added shelter: %s, Hernando County' % (
                shelter['name']
            ))
            message.append('  %s, %s' % (
                shelter['type'], shelter['status']
            ))
            message.append('  %s' % shelter['address'])
        if added_names and removed_names:
            message.append('')
        for name in removed_names:
            shelter = [n for n in old_data if n['name'] == name][0]
            message.append('Removed shelter: %s, Hernando County' % (
                shelter['name']
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
        return '%s\n\n%s\nChange detected on %s' % (
            summary_text, body, self.url
        )

    def fetch_data(self):
        s = Soup(requests.get(self.url).content)
        shelters = []
        for tr in s.find('table').findAll('tr'):
            tds = tr.findAll('td')
            img = tds[1].find('img')
            if img is not None:
                shelter_type = img['alt'].title()
            else:
                shelter_type = 'General'
            shelters.append({
                'name': tds[2].getText(),
                'type': shelter_type,
                'address': tds[3].getText(),
                'status': tds[4].getText(),
            })
        return shelters


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
        message += '\nChange detected on %s' % self.url
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
            FemaNSS,
            IrmaShelters,
            IrmaShelterDupes,
            FloridaDisasterShelters,
            ZeemapsScraper,
            PascoCounty,
            CrowdSourceRescue,
            LedgerPolkCounty,
            HernandoCountyShelters,
            FplStormOutages,
            FplCountyOutages,
            GemaAnimalShelters,
            GemaActiveShelters,
        )
    ]
    while True:
        for scraper in scrapers:
            try:
                scraper.scrape_and_store()
            except Exception, e:
                print "!!!! %s: %s !!!!!" % (
                    scraper.__class__.__name__, e
                )
        time.sleep(120)
