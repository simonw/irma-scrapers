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

        return {
            'html_lines': unicode(main_content).split(u'\n'),
        }
