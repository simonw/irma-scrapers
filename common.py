from github_read_write import GithubContent

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

        if self.test_mode and not self.github_token:
            print json.dumps(data, indent=2)
            return

        # We need to store the data
        github = GithubContent(self.owner, self.repo, self.github_token)
        if not self.last_data or not self.last_sha:
            # Check and see if it exists yet
            try:
                content, sha = github.read(self.filepath)
                self.last_data = json.loads(content)
                self.last_sha = sha
            except GithubContent.NotFound:
                pass

        if self.last_data == data:
            print '%s: Nothing changed' % self.filepath
            return

        if self.last_sha:
            print 'Updating %s' % self.filepath
            message = self.update_message(self.last_data, data)
        else:
            print 'Creating %s' % self.filepath
            message = self.create_message(data)

        if self.test_mode:
            print message
            print
            print json.dumps(data, indent=2)
            return

        content_sha, commit_sha = github.write(
            filepath=self.filepath,
            content=json.dumps(data, indent=2),
            sha=self.last_sha,
            commit_message=message,
            committer=self.committer,
        )

        self.last_sha = content_sha
        self.last_data = data

        self.post_to_slack(message, commit_sha)
        print 'https://github.com/%s/%s/commit/%s' % (
            self.owner, self.repo, commit_sha
        )
