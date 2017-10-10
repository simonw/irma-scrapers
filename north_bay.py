from base_scraper import BaseScraper
from BeautifulSoup import Comment, BeautifulSoup as Soup
import requests


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
