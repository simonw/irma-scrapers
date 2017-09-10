import requests
import json


class Scraper(object):
    owner = None
    repo = None
    filepath = None
    committer = None
    slack_channel = None
    slack_botname = None
    test_mode = False

    def __init__(self, github_token, slack_token=None):
        self.last_data = None
        self.last_sha = None
        self.github_token = github_token
        self.slack_token = slack_token

    def post_to_slack(self, message, commit_hash):
        if not (self.slack_channel and self.slack_token):
            return
        headline = message.split('\n')[0]
        try:
            body = message.split('\n', 1)[1]
        except IndexError:
            body = ''
        github_url = 'https://github.com/%s/%s/commit/%s' % (
            self.owner, self.repo, commit_hash
        )
        requests.post('https://slack.com/api/chat.postMessage', {
            'token': self.slack_token,
            'channel': self.slack_channel,
            'attachments': json.dumps([{
                'fallback': github_url,
                'pretext': headline,
                'title': '%s: %s' % (self.filepath, commit_hash[:8]),
                'title_link': github_url,
                'text': body.strip(),
            }]),
            'icon_emoji': ':robot_face:',
            'username': self.slack_botname,
        }).json()

    def create_message(self, new_data):
        return 'Created %s' % self.filepath

    def update_message(self, old_data, new_data):
        return 'Updated %s' % self.filepath

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
        if not self.last_data or not self.last_sha:
            # Check and see if it exists yet
            response = requests.get(
                github_url,
                headers={
                    'Authorization': 'token %s' % self.github_token
                }
            )
            if response.status_code == 403 and 'rate-limiting' in response.json().get('documentation_url', ''):
                raise Exception('Oh no it looks like we are rate limited')
            if response.status_code == 200:
                self.last_sha = response.json()['sha']
                self.last_data = json.loads(response.json()['content'].decode('base64'))
            elif response.status_code == 404:
                pass
            else:
                raise Exception(str(response.status_code) + ': ' + response.content)

        if self.last_data == data:
            print '%s: Nothing changed' % self.filepath
            return

        kwargs = {
            'path': self.filepath,
            'content': json.dumps(data, indent=2).encode('base64'),
        }
        if self.committer:
            kwargs['committer'] = self.committer
        if self.last_sha:
            kwargs['sha'] = self.last_sha
            kwargs['message'] = self.update_message(self.last_data, data)
            print 'Updating %s' % self.filepath
        else:
            kwargs['message'] = self.create_message(data)
            print 'Creating %s' % self.filepath

        if self.test_mode:
            print json.dumps(kwargs, indent=2)
            return

        response = requests.put(
            github_url,
            json=kwargs,
            headers={
                'Authorization': 'token %s' % self.github_token
            }
        )
        assert str(response.status_code).startswith('2'), response.content
        updated = response.json()
        self.last_sha = updated['content']['sha']
        self.last_data = data
        commit_url = updated['commit']['html_url']
        commit_hash = updated['commit']['sha']
        self.post_to_slack(kwargs['message'], commit_hash)
        print commit_url
