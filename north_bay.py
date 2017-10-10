from base_scraper import BaseScraper, BaseDeltaScraper
from BeautifulSoup import Comment, BeautifulSoup as Soup
from xml.etree import ElementTree
import requests
import re


class PGEOutagesIndividual(BaseDeltaScraper):
    url = 'https://apim.pge.com/cocoutage/outages/getOutagesRegions?regionType=city&expand=true'
    filepath = 'pge-outages-individual.json'
    slack_channel = None
    record_key = 'outageNumber'
    noun = 'outage'

    def fetch_data(self):
        data = requests.get(
            self.url,
            timeout=10,
        ).json()
        # Flatten into a list of outages
        outages = []
        for region in data['outagesRegions']:
            for outage in region['outages']:
                outage['regionName'] = region['regionName']
                outages.append(outage)
        return outages

    def display_record(self, outage):
        display = []
        display.append('  %(outageNumber)s in %(regionName)s affecting %(estCustAffected)s' % outage)
        display.append('    https://www.google.com/maps/search/%(latitude)s,%(longitude)s' % outage)
        display.append('    %(cause)s - %(crewCurrentStatus)s' % outage)
        display.append('')
        return '\n'.join(display)


class SantaRosaEmergencyInformation(BaseScraper):
    url = 'https://srcity.org/610/Emergency-Information'
    filepath = 'santa-rosa-emergency.json'
    slack_channel = None

    def fetch_data(self):
        html = requests.get(self.url).content
        soup = Soup(html)
        main_content = soup.find('div', {'data-cprole': 'mainContentContainer'})
        # Remove scripts
        [s.extract() for s in main_content.findAll('script')]
        # Remove source comments
        comments = soup.findAll(text=lambda text: isinstance(text, Comment))
        [comment.extract() for comment in comments]
        # Remove almost all attributes
        for tag in main_content.recursiveChildGenerator():
            try:
                tag.attrs = [
                    (key, value) for key, value in tag.attrs
                    if key in ('href', 'src')
                    and not value.startswith('#')
                ]
            except AttributeError:
                pass

        return {
            'html_lines': unicode(main_content).split(u'\n'),
        }


class SonomaRoadConditions(BaseScraper):
    url = 'http://roadconditions.sonoma-county.org/'
    filepath = 'sonoma-road-conditions.json'
    slack_channel = None

    def fetch_data(self):
        soup = Soup(requests.get(self.url).content)
        road_closures = {}
        for id in ('divTableCounty', 'divTableCity'):
            name = {'divTableCounty': 'county_roads', 'divTableCity': 'city_roads'}[id]
            div = soup.find('div', {'id': id})
            table = div.find('table')
            headers = [th.text for th in table.findAll('th')]
            closures = []
            for tr in table.find('tbody').findAll('tr'):
                values = [td.text for td in tr.findAll('td')]
                closures.append(dict(zip(headers, values)))
            road_closures[name] = closures
        return road_closures


class CaliforniaDOTRoadInfo(BaseScraper):
    url = 'http://www.dot.ca.gov/hq/roadinfo/Hourly'
    filepath = 'dot-ca-roadinfo-hourly.json'
    slack_channel = None

    def fetch_data(self):
        text = requests.get(self.url).content
        return {
            'text_lines': [l.rstrip('\r') for l in text.split('\n')],
        }


class CaliforniaHighwayPatrolIncidents(BaseDeltaScraper):
    url = 'http://quickmap.dot.ca.gov/data/chp-only.kml'
    filepath = 'chp-incidents.json'
    slack_channel = None
    record_key = 'name'
    noun = 'incident'

    def display_record(self, incident):
        display = []
        display.append('  %s' % incident['name'])
        display.append('    https://www.google.com/maps/search/%(latitude)s,%(longitude)s' % incident)
        display.append('    ' + incident['description'])
        display.append('')
        return '\n'.join(display)

    def fetch_data(self):
        kml = requests.get(self.url).content
        et = ElementTree.fromstring(kml)
        incidents = []
        for placemark in et.findall('.//{http://www.opengis.net/kml/2.2}Placemark'):
            coords = placemark.find('.//{http://www.opengis.net/kml/2.2}coordinates').text.strip()
            latitude, longitude, blah = map(float, coords.split(','))
            description = placemark.find('{http://www.opengis.net/kml/2.2}description').text.strip()
            name = placemark.find('{http://www.opengis.net/kml/2.2}name').text.strip()
            incidents.append({
                'name': name,
                'description': strip_tags(description),
                'latitude': latitude,
                'longitude': longitude,
            })
        return incidents


tag_re = re.compile('<.*?>')


def strip_tags(s):
    return tag_re.sub('', s)
