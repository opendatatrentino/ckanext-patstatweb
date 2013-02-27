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

from tempfile import mkstemp
from urllib2 import urlparse

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
CHUNK_SIZE = 10 * 1024 * 1024 # 10 MB


def download_big_file(url):
    """
    Download a file on a tempfile without exploding in memory
    return the created file name
    """
    log.debug('Downloading: %s', url)
    basefile = os.path.basename(urlparse.urlsplit(url).path)
    fd, big_filename = mkstemp(prefix=basefile + '_XXXX')
    with os.fdopen(fd, "w") as f:
        #r = requests.get(url, stream=True)
        r = requests.get(url)

        if not r.ok:
            log.error('Cannot get "%s"', url)
            return None

        for chunk in r.iter_content(CHUNK_SIZE):
            f.write(chunk)

    return big_filename

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
            u'.\nArea: ', origmeta['Area'],
            u'.\nSettore: ', origmeta['Settore'],
            u'.\nAlgoritmo: ', origmeta['Algoritmo'],
            u'.\nUnità di misura: ', origmeta['UM'],
            u'.\nFenomeno: ', origmeta['Fenomeno'],
            u'.\nConfronti territoriali: ',
            origmeta['ConfrontiTerritoriali'],
            u'. Note: ', origmeta['Note'],
        ))
        return d

    extras = {}
    try:
        extras = {
            u'Titolare': 'Provincia Autonoma di Trento',
            u'Categorie': 'Statistica',
            u'Copertura Geografica': 'Provincia di Trento',
            u'Copertura Temporale (Data di inizio)': created.isoformat(),
            u'Copertura Temporale (Data di fine)': modified.isoformat(),
            u'Aggiornamento': origmeta['FreqAggiornamento'],
            u'Data di pubblicazione': datetime.datetime.now().isoformat(),
            u'Data di Aggiornamento': modified.isoformat(),
        }
    except KeyError:
        log.error("Input format changed, fix the code")
    except UnicodeDecodeError:
        log.error("Encoding error, fix the code")

    return extras



class PatStatWebHarvester(HarvesterBase):
    INDEX_URL = \
        "http://www.statweb.provincia.tn.it/IndicatoriStrutturali/exp.aspx"

    # in v2 groups are identified by ids instead of names, so stick with v1
    config = {'api_version': 1}

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
                # download json
                r1 = requests.get(resource_url)
                if r1.ok:
                    elem[resource_key] = r1.json
                # download csv
                csv_url = resource_url.replace('fmt=json', 'fmt=csv')
                csv_path = download_big_file(csv_url)
                if csv_path:
                     elem['metadata'][resource_key + '_csv_path'] = csv_path


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

        extras = metadata_mapping(elem)

        package_dict = {
            u'id': sha1(elem['URL']).hexdigest(),
            u'title': extras[u'Titolo'],
            u'groups': ['statistica'],
            u'url': DOCTEC,
            u'notes': extras[u'Descrizione'],
            u'author': extras[u'Autore'],
            u'author_email': extras[u'Contatto'],
            u'maintainer': extras[u'Autore'],
            u'maintainer_email': extras[u'Contatto'],
            u'tags': [elem['metadata']['Area'], elem['metadata']['Settore']],
            u'license_id': 'cc-by',
            u'license': u'Creative Commons Attribution',
            u'license_title': u'Creative Commons Attribution 3.0 it',
            u'license_url': u'http://creativecommons.org/licenses/by/3.0/it/',
            u'isopen': True,
            u'extras': extras,
            u'resources': []
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
            csv_path = elem['metadata'][resource_key + '_csv_path']
            junkurl, errmsg = ckan_client.upload_file(csv_path)
            url = junkurl.replace('http://', base_location)
            os.remove(csv_path)

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
