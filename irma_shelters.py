from base_scraper import BaseScraper
import requests
import Geohash
import re

IGNORE_DUPE_IDS = {
    456, # Hialeah Middle School
    442, # Amelia Earhart Elementary
}

GEOHASH_PRECISION = 7


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
            if shelter['id'] in IGNORE_DUPE_IDS:
                continue
            geohash = Geohash.encode(
                shelter['latitude'],
                shelter['longitude'],
                precision=GEOHASH_PRECISION,
            )
            by_geohash.setdefault(geohash, []).append(shelter)
        dupe_groups = [
            pair for pair in by_geohash.items()
            if (
                # More than one shelter in this group
                len(pair[1]) > 1
                # Group is not invalid lat/lon
                and pair[0] != ('0' * GEOHASH_PRECISION)
            )
        ]
        no_latlons = by_geohash.get('0' * GEOHASH_PRECISION) or []
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


map_url_re = re.compile(
    r'http://maps.google.com/maps\?saddr=&daddr=-?\d+\.\d+,-?\d+\.\d+'
)


class IrmaSheltersFloridaMissing(BaseScraper):
    filepath = 'florida-shelters-missing.json'
    our_url = 'https://raw.githubusercontent.com/simonw/disaster-data/master/irma-shelters.json'
    their_url = 'https://raw.githubusercontent.com/simonw/disaster-data/master/florida-shelters.json'
    issue_comments_url = 'https://api.github.com/repos/simonw/disaster-data/issues/2/comments'

    def create_message(self, new_data):
        return self.update_message([], new_data, 'Created')

    def update_message(self, old_data, new_data, verb='Updated'):
        previous_map_urls = [
            d['map_url'] for d in old_data
        ]
        current_map_urls = [
            d['map_url'] for d in new_data
        ]
        added_map_urls = [
            map_url for map_url in current_map_urls
            if map_url not in previous_map_urls
        ]
        removed_map_urls = [
            map_url for map_url in previous_map_urls
            if map_url not in current_map_urls
        ]

        message = []

        if added_map_urls:
            message.append('New potentially missing shelters:')

        for map_url in added_map_urls:
            shelter = [s for s in new_data if s['map_url'] == map_url][0]
            message.append('  %s (%s County)' % (shelter['name'], shelter['county']))
            message.append('  Type: ' + shelter['type'])
            message.append('  ' + shelter['address'])
            message.append('  ' + shelter['city'])
            message.append('  ' + shelter['map_url'])
            message.append('')

        if added_map_urls and removed_map_urls:
            message.append('')

        if removed_map_urls:
            message.append('Previous missing shelters now resolved:')

        for map_url in removed_map_urls:
            shelter = [s for s in old_data if s['map_url'] == map_url][0]
            message.append('  %s (%s County)' % (shelter['name'], shelter['county']))

        body = '\n'.join(message)
        summary = []
        if added_map_urls:
            summary.append('%d potentially missing shelter%s detected' % (
                len(added_map_urls), '' if len(added_map_urls) == 1 else 's',
            ))
        if removed_map_urls:
            summary.append('%d shelter%s resolved' % (
                len(removed_map_urls), '' if len(removed_map_urls) == 1 else 's',
            ))
        if current_map_urls:
            summary.append('%d total' % (
                len(current_map_urls)
            ))
        if summary:
            summary_text = self.filepath + ': ' + (', '.join(summary))
        else:
            summary_text = '%s %s' % (verb, self.filepath)
        return summary_text + '\n\n' + body

    def fetch_data(self):
        our_shelters = requests.get(self.our_url).json()
        their_shelters = requests.get(self.their_url).json()
        our_geohashes = set([
            Geohash.encode(s['latitude'], s['longitude'], 6)
            for s in our_shelters
        ])
        for shelter in their_shelters:
            coords = shelter['map_url'].split('daddr=')[1]
            latitude, longitude = map(float, coords.split(','))
            geohash = Geohash.encode(latitude, longitude, 6)
            shelter['geohash'] = geohash
        maybe_missing_shelters = [
            s for s in their_shelters
            if s['geohash'] not in our_geohashes
        ]
        ignore_map_urls = []
        for comment in all_comments(self.issue_comments_url, self.github_token):
            ignore_map_urls.extend(map_url_re.findall(comment['body']))
        maybe_missing_shelters = [
            s for s in maybe_missing_shelters
            if s['map_url'] not in ignore_map_urls
        ]
        return maybe_missing_shelters


def all_comments(issue_comments_url, github_token):
    # Paginate through all comments on an issue
    while issue_comments_url:
        response = requests.get(
            issue_comments_url,
            headers={
                'Authorization': 'token %s' % github_token,
            })
        try:
            issue_comments_url = response.links['next']['url']
        except KeyError:
            issue_comments_url = None
        for item in response.json():
            yield item
