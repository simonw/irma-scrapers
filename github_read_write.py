"""
This class knows how to read and write LARGE files to Github. The regular
GitHub Contents API can't handle files larger than 1MB - this class knows how
to spot that problem and switch to the large-file-supporting low level Git Data
API instead.

https://developer.github.com/v3/repos/contents/
https://developer.github.com/v3/git/
"""
import requests


class GithubContent(object):
    class NotFound(Exception):
        pass

    class UnknownError(Exception):
        pass

    def __init__(self, owner, repo, token):
        self.owner = owner
        self.repo = repo
        self.token = token

    def base_url(self):
        return 'https://api.github.com/repos/%s/%s' % (
            self.owner, self.repo
        )

    def read(self, filepath):
        # Try reading using content API
        content_url = self.base_url() + '/contents/%s' % filepath
        response = requests.get(
            content_url,
            headers={
                'Authorization': 'token %s' % self.token
            }
        )
        if response.status_code == 200:
            data = response.json()
            return data['content'].decode('base64'), data['sha']
        elif response.status_code == 404:
            raise self.NotFound(filepath)
        elif response.status_code == 403:
            # It's probably too large
            if response.json()['errors'][0]['code'] != 'too_large':
                raise self.UnknownError(response.content)
            else:
                return self.read_large(filepath)
        else:
            raise self.UnknownError(response.content)

    def read_large(self, filepath):
        master = requests.get(
            self.base_url() + '/git/trees/master?recursive=1',
            headers={
                'Authorization': 'token %s' % self.token
            }
        ).json()
        try:
            tree_entry = [t for t in master['tree'] if t['path'] == filepath][0]
        except IndexError:
            raise self.NotFound(filepath)
        data = requests.get(
            tree_entry['url'],
            headers={
                'Authorization': 'token %s' % self.token
            }
        ).json()
        return data['content'].decode('base64'), data['sha']

    def write(self, filepath, content, sha=None, commit_message=None, committer=None):
        github_url = self.base_url() + '/contents/%s' % filepath
        payload = {
            'path': filepath,
            'content': content.encode('base64'),
            'message': commit_message,
        }
        if sha:
            payload['sha'] = sha
        if committer:
            payload['committer'] = committer

        response = requests.put(
            github_url,
            json=payload,
            headers={
                'Authorization': 'token %s' % self.token
            }
        )
        if response.status_code == 403 and response.json()['errors'][0]['code'] == 'too_large':
            return self.write_large(filepath, content, commit_message, committer)
        elif sha is None and response.status_code == 422 and 'sha' in response.json().get('message', ''):
            # Missing sha - we need to figure out the sha and try again
            old_content, old_sha = self.read(filepath)
            return self.write(
                filepath,
                content,
                sha=old_sha,
                commit_message=commit_message,
                committer=committer,
            )
        elif response.status_code in (201, 200):
            updated = response.json()
            return updated['content']['sha'], updated['commit']['sha']
        else:
            raise self.UnknownError(str(response.status_code) + ':' + response.content)

    def write_large(self, filepath, content, commit_message=None, committer=None):
        # Create a new blob with the file contents
        created_blob = requests.post(self.base_url() + '/git/blobs', json={
            'encoding': 'utf8',
            'content': content,
        }, headers={'Authorization': 'token %s' % self.token}).json()
        # Retrieve master tree sha
        master_sha = requests.get(
            self.base_url() + '/git/trees/master?recursive=1',
            headers={
                'Authorization': 'token %s' % self.token
            }
        ).json()['sha']
        # Construct a new tree
        created_tree = requests.post(
            self.base_url() + '/git/trees',
            json={
                'base_tree': master_sha,
                'tree': [{
                    'mode': '100644', # file (blob),
                    'path': filepath,
                    'type': 'blob',
                    'sha': created_blob['sha'],
                }]
            },
            headers={'Authorization': 'token %s' % self.token}
        ).json()
        # Create a commit which references the new tree
        payload = {
            'message': commit_message,
            'parents': [master_sha],
            'tree': created_tree['sha'],
        }
        if committer:
            payload['committer'] = committer
        created_commit = requests.post(
            self.base_url() + '/git/commits',
            json=payload,
            headers={'Authorization': 'token %s' % self.token}
        ).json()
        # Move HEAD reference on master to the new commit
        requests.patch(
            self.base_url() + '/git/refs/heads/master',
            json={'sha': created_commit['sha']},
            headers={'Authorization': 'token %s' % self.token}
        ).json()
        return created_blob['sha'], created_commit['sha']
