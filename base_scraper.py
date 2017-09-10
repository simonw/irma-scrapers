from common import Scraper


class BaseScraper(Scraper):
    owner = 'simonw'
    repo = 'irma-scraped-data'
    committer = {
        'name': 'irma-scraper',
        'email': 'irma-scraper@example.com',
    }
    slack_botname = 'Irma Scraper'
    slack_channel = '#shelter_scraper_data'
