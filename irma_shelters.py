from base_scraper import BaseScraper
import requests
import Geohash

IGNORE_DUPE_IDS = {
    456, # Hialeah Middle School
    442, # Amelia Earhart Elementary
}


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
                precision=7
            )
            by_geohash.setdefault(geohash, []).append(shelter)
        dupe_groups = [
            pair for pair in by_geohash.items()
            if (
                # More than one shelter in this group
                len(pair[1]) > 1
                # Group is not invalid lat/lon
                and pair[0] != '00000000'
            )
        ]
        no_latlons = by_geohash.get('00000000') or []
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
