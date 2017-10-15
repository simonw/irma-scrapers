from base_scraper import BaseScraper
import requests


def objectid(d):
    # Different datasets represent objectid in different ways
    return d.get('OBJECTID') or d['ObjectID']


def shelter_name(d):
    return d.get('SHELTER_NAME') or d['label']


def shelter_county(d):
    return d.get('COUNTY_PARISH') or d['county']


class BaseGisScraper(BaseScraper):
    source_url = None

    def create_message(self, new_data):
        return self.update_message([], new_data, verb='Created')

    def update_message(self, old_data, new_data, verb='Updated'):
        new_objects = [o for o in new_data if not any(o2 for o2 in old_data if objectid(o2) == objectid(o))]
        removed_objects = [o for o in old_data if not any(o2 for o2 in new_data if objectid(o2) == objectid(o))]
        message = []

        def name(row):
            if 'COUNTY_PARISH' in row or 'county' in row:
                s = '%s (%s County)' % (shelter_name(row), shelter_county(row).title())
            elif 'CITY' in row and 'STATE' in row:
                s = '%s (%s, %s)' % (shelter_name(row), row['CITY'].title(), row['STATE'])
            else:
                s = shelter_name(row)
            return s.replace('County County', 'County')

        for new_object in new_objects:
            message.append('Added shelter: %s' % name(new_object))
        if new_objects:
            message.append('')
        for removed_object in removed_objects:
            message.append('Removed shelter: %s' % name(removed_object))
        if removed_objects:
            message.append('')
        num_updated = 0
        for new_object in new_data:
            old_object = [o for o in old_data if objectid(o) == objectid(new_object)]
            if not old_object:
                continue
            old_object = old_object[0]
            if new_object != old_object:
                message.append('Updated shelter: %s' % name(new_object))
                num_updated += 1
        body = '\n'.join(message)
        summary = []
        if new_objects:
            summary.append('%d shelter%s added' % (
                len(new_objects), '' if len(new_objects) == 1 else 's',
            ))
        if removed_objects:
            summary.append('%d shelter%s removed' % (
                len(removed_objects), '' if len(removed_objects) == 1 else 's',
            ))
        if num_updated:
            summary.append('%d shelter%s updated' % (
                num_updated, '' if num_updated == 1 else 's',
            ))
        if summary:
            summary_text = self.filepath + ': ' + (', '.join(summary))
        else:
            summary_text = '%s %s' % (verb, self.filepath)
        if self.source_url:
            body += '\nChange detected on %s' % self.source_url
        return summary_text + '\n\n' + body

    def fetch_data(self):
        data = requests.get(self.url).json()
        shelters = [feature['attributes'] for feature in data['features']]
        shelters.sort(key=lambda s: objectid(s))
        return shelters


class FemaOpenShelters(BaseGisScraper):
    filepath = 'fema-open-shelters.json'
    url = 'https://gis.fema.gov/REST/services/NSS/OpenShelters/MapServer/0/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry=%7B%22xmin%22%3A-10018754.171396945%2C%22ymin%22%3A2504688.5428529754%2C%22xmax%22%3A-7514065.628548954%2C%22ymax%22%3A5009377.085700965%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100'


class FemaNSS(BaseGisScraper):
    filepath = 'fema-nss.json'
    url = 'https://gis.fema.gov/REST/services/NSS/FEMA_NSS/MapServer/0/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry=%7B%22xmin%22%3A+-14404742.108649602%2C+%22ymin%22%3A+-55660.4518654215%2C+%22ymax%22%3A+6782064.328749425%2C+%22xmax%22%3A+-5988988.6046781195%2C+%22spatialReference%22%3A+%7B%22wkid%22%3A+102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100'


class GemaAnimalShelters(BaseGisScraper):
    filepath = 'georgia-gema-animal-shelters.json'
    url = 'https://services1.arcgis.com/2iUE8l8JKrP2tygQ/arcgis/rest/services/AnimalShelters/FeatureServer/0/query?f=json&where=status%20%3D%20%27OPEN%27&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=102100&resultOffset=0&resultRecordCount=1000'
    source_url = 'https://gema-soc.maps.arcgis.com/apps/webappviewer/index.html?id=279ef7cfc1da45edb640723c12b02b18'


class GemaActiveShelters(BaseGisScraper):
    filepath = 'georgia-gema-active-shelters.json'
    url = 'https://services1.arcgis.com/2iUE8l8JKrP2tygQ/arcgis/rest/services/SheltersActive/FeatureServer/0/query?f=json&where=shelter_information_shelter_type%20%3C%3E%20%27Reception%20Care%20Ctr.%27&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=102100&resultOffset=0&resultRecordCount=1000'
    source_url = 'https://gema-soc.maps.arcgis.com/apps/webappviewer/index.html?id=279ef7cfc1da45edb640723c12b02b18'
