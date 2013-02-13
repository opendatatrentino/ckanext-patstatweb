# CKAN Harvester per http://www.statweb.provincia.tn.it/
# coding: utf-8

import os
import logging

from hashlib import sha1

try:
    import simplejson as json
except ImportError:
    import json

import requests
import datetime

import csv
from tempfile import mkstemp

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject

from ckan.logic import get_action
from ckan import model

def _post_multipart(self, selector, fields, files):
    '''Post fields and files to an http host as multipart/form-data.

    :param fields: a sequence of (name, value) tuples for regular form
        fields
    :param files: a sequence of (name, filename, value) tuples for data to
        be uploaded as files

    :returns: the server's response page

    '''

    from urlparse import urljoin, urlparse

    content_type, body = self._encode_multipart_formdata(fields, files)

    headers = self._auth_headers()
    url = urljoin(self.base_location + urlparse(self.base_location).netloc, selector)
    req = requests.post(url, data=dict(fields), files={files[0][0]: files[0][1:]}, headers=headers)
    return req.status_code, req.error, req.headers, req.text


import ckanclient

# FIXME: no monkey patching here
ckanclient.CkanClient._post_multipart = _post_multipart

log = logging.getLogger(__name__)

DATASET_KEYS = ("Indicatore", "TabNumeratore", "TabDenominatore")
DOCTEC = '''http://www.statweb.provincia.tn.it/INDICATORISTRUTTURALI/ElencoIndicatori.aspx'''

# patched ckanclient functions for upload

def metadata_mapping(infodict):
    """
    Mapping secondo specifiche tratte da:
    http://www.innovazione.provincia.tn.it/binary/pat_innovazione/notizie/
    AllegatoB_formati_21Dicembre_def.1356705197.pdf
    """
    origmeta = {k: v for k, v in infodict['metadata'].items()
                if k not in DATASET_KEYS}
    date = origmeta['UltimoAggiornamento']

    day, month, year = [int(a) for a in date.split('/')]
    modified = datetime.datetime(year, month, day)
    Anno = origmeta['AnnoInizio'] or '1970'
    created = datetime.datetime(int(Anno), 1, 1)

    def format_description():
        d = u''.join((
            infodict['Descrizione'],
            u'. Area: ', origmeta['Area'],
            u'. Settore: ', origmeta['Settore'],
            u'. Algoritmo: ', origmeta['Algoritmo'],
            u'. Unità di misura: ', origmeta['UM'],
            u'. Fenomeno: ', origmeta['Fenomeno'],
            u'. Confronti territoriali: ',
            origmeta['ConfrontiTerritoriali'],
            u'. Note: ', origmeta['Note'],
        ))
        return d

    extras = {}
    try:
        extras = {
            u'Titolo': infodict['Descrizione'],
            u'Titolare': 'Provincia Autonoma di Trento',
            u'Referente': 'Servizio Statistica',
            u'Contatto': 'serv.statistica@provincia.tn.it',
            u'Descrizione': format_description(),
            u'Categorie': 'Statistica',
            u'Tag/Parole chiave': ', '.join((origmeta['Area'], origmeta['Settore'])),
            u'Documentazione Tecnica': DOCTEC,
            u'Copertura Geografica': 'Provincia di Trento',
            u'Copertura Temporale (Data di inizio)': created.isoformat(),
            u'Copertura Temporale (Data di fine)': modified.isoformat(),
            u'Aggiornamento': origmeta['FreqAggiornamento'],
            u'Data di pubblicazione': datetime.datetime.now().isoformat(),
            u'Data di Aggiornamento': modified.isoformat(),
            u'Licenza': 'CC-BY',
            u'Formato': 'JSON',
            u'Codifica Caratteri': 'utf8',
            u'Autore': 'Servizio Statistica',
            u"Email dell'autore": 'serv.statistica@provincia.tn.it',
            u'URL sito': 'http://www.statistica.provincia.tn.it',
        }
    except KeyError:
        log.error("Input format changed, fix the code")
    except UnicodeDecodeError:
        log.error("Encoding error, fix the code")

    return extras


def create_csv_from_json(rows):
    fd, path = mkstemp(suffix='.csv')
    f = os.fdopen(fd, "w")
    fieldnames = rows[0].keys()
    writer = csv.DictWriter(f, fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    f.close()
    return path


class PatStatWebHarvester(HarvesterBase):
    INDEX_URL = \
        "http://www.statweb.provincia.tn.it/IndicatoriStrutturali/exp.aspx"

    def info(self):
        return {
            u'name': u'PATstatweb',
            u'title': u'Servizio Statistica - Provincia Autonoma di Trento',
            u'description': u'Harvester for www.statistica.provincia.tn.it'
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
            log.error('Cannot get "%s"', elem['URL'])
            return False

        elem['metadata'] = r.json.values()[0][0]

        for resource_key in DATASET_KEYS:
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
            log.error('Harvest object contentless')
            self._save_object_error(
                'Empty content for object %s' % harvest_object.id,
                harvest_object,
                'Import'
            )
            return False

        # get api user & keys
        user = get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )

        api_key = user.get('apikey')

        from pylons import config
        base_location = config['ckan.site_url']

        ckan_client = ckanclient.CkanClient(
            base_location=base_location + '/api',
            api_key=api_key,
            is_verbose=True,
        )

        elem = json.loads(harvest_object.content)

        package_dict = {
            'id': sha1(elem['URL']).hexdigest(),
            'title': elem['Descrizione'],
            'groups': ['statistica'],
            'url': elem['URL'],
            'notes': elem['metadata']["Note"],
            'author': elem['Fonte'],
            'maintainer':  elem['Fonte'],
            'maintainer_email': 'serv.statistica@provincia.tn.it',
            'tags': [elem['metadata']['Area'], elem['metadata']['Settore']],
            'license_id': 'cc-by',
            'license': u'Creative Commons Attribution',
            'license_title': u'Creative Commons Attribution 3.0 it',
            'license_url': u'http://creativecommons.org/licenses/by/3.0/it/',
            'isopen': True,
            'extras': metadata_mapping(elem),
            'resources': []
        }

        modified = package_dict['extras']['Data di Aggiornamento']

        for resource_key in DATASET_KEYS:
            try:
                resource_url = elem['metadata'][resource_key]
            except KeyError:
                continue

            name = elem[resource_key].keys()[0]

            res_dict = {
                'url': resource_url,
                'format': 'json',
                'mimetype': 'application/json',
                'resource_type': 'api',
                'description': name,
                'name': name,
                'last_modified': modified,
            }
            package_dict['resources'].append(res_dict)

            # After creating a link to the original source we want a CSV
            rows = elem[resource_key][name]
            file_path = create_csv_from_json(rows)
            junkurl, errmsg = ckan_client.upload_file(file_path)
            url = junkurl.replace('http://', base_location)
            os.remove(file_path)

            res_dict_csv = dict(res_dict)
            res_dict_csv["url"] = url
            res_dict_csv["format"] = 'csv'
            res_dict_csv["mimetype"] = 'text/csv'
            res_dict_csv["resource_type"] = 'file'
            package_dict['resources'].append(res_dict_csv)

        package_dict['name'] = self._gen_new_name(package_dict['title'])

        # Set the modification date
        package_dict['metadata_modified'] = modified

        return self._create_or_update_package(package_dict, harvest_object)
