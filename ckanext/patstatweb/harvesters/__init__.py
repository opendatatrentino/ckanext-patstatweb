#coding: utf-8
import os
import logging

from hashlib import sha1

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject

from ckan.logic import get_action
from ckan import model

import ckanclient

try:
    import simplejson as json
except ImportError:
    import json

import requests
import datetime

import csv
from tempfile import mkstemp

log = logging.getLogger(__name__)


def create_csv_from_json(rows):
    temp_file, path = mkstemp()
    fieldnames = rows[0].keys()
    writer = csv.DictWriter(temp_file, fieldnames)
    writer.writerows(rows)
    temp_file.close()
    return path


class PatStatWebHarvester(HarvesterBase):
    INDEX_URL = \
        "http://www.statweb.provincia.tn.it/IndicatoriStrutturali/expJSON.aspx"
    datasetkeys = ("Indicatore", "TabNumeratore", "TabDenominatore")

    def __init__(self, *args, **kwargs):
        super(PatStatWebHarvester, self).__init__(*args, **kwargs)

        user = get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )
        api_key = user.get('apikey')
        self.ckan_client = ckanclient.CkanClient(
            base_location="http://localhost:5000",
            API_KEY=api_key,
        )

    def info(self):
        return {
            'name': 'PATstatweb',
            'title': 'Servizio Statistica - Provincia Autonoma di Trento',
            'description': 'Harvester for www.statistica.provincia.tn.it'
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
            obj = HarvestObject(
                guid=sha1(elem['URL']).hexdigest(),
                job=harvest_job,
                content=json.dumps(elem)
            )
            obj.save()
            ids.append(obj.id)
        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In PatStatWebHarvester fetch_stage')

        elem = json.loads(harvest_object.content)
        r = requests.get(elem['URL'])
        if not r.ok:
            return False

        elem['metadata'] = r.json.values()[0][0]

        for resource_key in self.datasetkeys:
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

    def import_stage(self, harvest_object):
        log.debug('In PatStatWebHarvester import_stage')
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error(
                'Empty content for object %s' % harvest_object.id,
                harvest_object,
                'Import'
            )
            return False

        elem = json.loads(harvest_object.content)

        package_dict = {
            'id': sha1(elem['URL']).hexdigest(),
            'title': elem['Descrizione'],
            'url': elem['URL'],
            'notes': elem['metadata']["Note"],
            'author': elem['Fonte'],
            'maintainer':  elem['Fonte'],
            'maintainer_email': '',
            'tags': ['stats'],
            'license_id': '',
            'extras': {k: v for k, v in elem['metadata'].items()
                       if k not in self.datasetkeys},
            'resources': []
        }

        for resource_key in self.datasetkeys:
            try:
                resource_url = elem['metadata'][resource_key]
            except KeyError:
                continue

            date = package_dict['extras']['UltimoAggiornamento']
            day, month, year = [int(a) for a in date.split('/')]
            modified = datetime.datetime(year, month, day)
            name = elem[resource_key].keys()[0]

            res_dict = {
                'url': resource_url,
                'format': 'json',
                'mimetype': 'application/json',
                'description': name,
                'name': name,
                'last_modified': modified.isoformat()
            }
            package_dict['resources'].append(res_dict)

            # After creating a link to the original source we want a CSV
            rows = elem[resource_key][name]
            file_path = create_csv_from_json(rows)
            url, errmsg = self.ckan_client.upload_file(file_path)
            os.remove(file_path)

            res_dict_csv = dict(res_dict)
            res_dict["url"] = url
            package_dict['resources'].append(res_dict_csv)

        package_dict['name'] = self._gen_new_name(package_dict['title'])

        # Set the modification date
        package_dict['metadata_modified'] = \
                package_dict['extras']['UltimoAggiornamento']

        return self._create_or_update_package(package_dict, harvest_object)
