#coding: utf-8
import logging

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from hashlib import sha1

from ckanext.rdf.consume import consume_one
from ckanext.rdf.vocab import Graph
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject

try:
    import simplejson as json
except ImportError:
    import json

import requests

log = logging.getLogger(__name__)

class PatStatWebHarvester(HarvesterBase):
    INDEX_URL = "http://www.statweb.provincia.tn.it/IndicatoriStrutturali/expJSON.aspx"

    def info(self):
        return {
            'name': 'PatStatWeb',
            'title': 'PatStatWeb harvester',
            'description': 'Harvester for the PatStatWeb'
        }

    def gather_stage(self, harvest_job):
        log.debug('In PatStatWebHarvester gather stage')
        # Get feed contents

        r = requests.get(self.INDEX_URL)

        if not r.ok:
            return []

        try:
            indicatori = r.json['IndicatoriStrutturali']
        except KeyError, json.JSONDecodeError:
            return []

        ids = []
        for elem in indicatori:
            obj = HarvestObject(guid=elem['id'], job=harvest_job, content=json.dumps(elem))
            obj.save()
            ids.append(obj.id)
        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In PatStatWebHarvester fetch_stage')

        elem = json.loads(harvest_object.content)
        r = requests.get(elem['URL'])
        if not r.ok:
            return False

        elem['metadata'] = r.json.values()[0]

        for resource_key in ("Indicatore", "TabNumeratore", "TabNumeratore"):
            try:
                resource_url = elem['metadata'][resource_key]
            except KeyError:
                pass
            else:
                r1 = requests.get(resource_url)
                if r1.ok:
                    elem[resource_key] = r1.json

        harvest_object.content = json.dumps(elem)
        harvest_object.save()

        return True

#            log.exception(e)
#            self._save_object_error('Unable to get content for dataset: %s: %r' % \
#                                        (url, e), harvest_object)

    def import_stage(self, harvest_object):
        log.debug('In PatStatWebHarvester import_stage')
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False

        package_dict['id'] = harvest_object.guid
        title = package_dict['title'] or package_dict['name'] 
        package_dict['name'] = self._gen_new_name(title)

        # Set the modification date
        if 'date_modified' in package_dict['extras']:
            package_dict['metadata_modified'] = package_dict['extras']['date_modified']

        # Common extras
        package_dict['extras']['harvest_catalogue_name'] = u'Dades Obertes Gencat'
        package_dict['extras']['harvest_catalogue_url'] = u'http://dadesobertes.gencat.cat'
        package_dict['extras']['harvest_dataset_url'] = self.DATASET_URL % package_dict['extras']['rdf_source_id'].strip('#')
        package_dict['extras']['eu_country'] = u'ES'
        package_dict['extras']['eu_nuts2'] = u'ES51'

        return self._create_or_update_package(package_dict, harvest_object)


