import requests
import json


class Scraper(object):
    owner = None
    repo = None
    filepath = None

    def __init__(self, github_token):
        self.last_data = None
        self.last_sha = None
        self.github_token = github_token

    def fetch_data(self):
        return []

    def scrape_and_store(self):
        data = self.fetch_data()
        if data is None:
            print '%s; Data was None' % self.filepath
            return
        github_url = 'https://api.github.com/repos/{}/{}/contents/{}'.format(
            self.owner, self.repo, self.filepath
        )
        # We need to store the data
        if not self.last_data:
            # Check and see if it exists yet
            response = requests.get(github_url)
            if response.status_code == 200:
                self.last_sha = response.json()['sha']
                self.last_data = json.loads(response.json()['content'].decode('base64'))

        if self.last_data == data:
            print '%s: Nothing changed' % self.filepath
            return

        kwargs = {
            'path': self.filepath,
            'message': 'Updating %s' % self.filepath,
            'content': json.dumps(data, indent=2).encode('base64'),
        }
        if self.last_sha:
            kwargs['sha'] = self.last_sha
            print 'Updating %s' % self.filepath
        else:
            print 'Creating %s' % self.filepath
        updated = requests.put(
            github_url,
            json=kwargs,
            headers={
                'Authorization': 'token %s' % self.github_token
            }
        ).json()
        self.last_sha = updated['content']['sha']
        self.last_data = data
