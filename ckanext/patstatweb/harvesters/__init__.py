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

import csv
import re

tags_remove = [
    'rdnt', 'siat', 'pup', 'db prior 10k', 'pup; rndt',
    'inquadramenti di base', 'suap', 'scritte', 'pupagri', 'pupasc', 'pupbos',
]

tags_subs = {
    'bosc': 'boschi',
    'comun': 'comuni',
    'siti archeolog': 'siti archeologici',
    'archeolog': 'archeologia',
    'specchio d\'acqua': 'specchi d\'acqua',
    'tratte': 'tratte ferroviarie',
    'viabilità di progetto': 'viabilità',
    'viabilità ferroviaria':  'viabilità',
    'viafer': 'viabilità',
    'viabilità forestale': 'viabilità',
    'zps': 'zone protezione speciale',
    'udf': 'distretti forestali',
    'uffici distrettuali forestali': 'distretti forestali',
    'pascolo': 'pascoli',
    'idrografici': 'idrografia',
}

# mappa Settore verso Categorie
cat_map_stat = {
    u'agricoltura': 'Economia',
    u'pesca': 'Economia',
    u'silvicoltura': 'Economia',
    u'commercio con l\'estero': 'Economia',
    u'commercio con l\'estero e internazionalizzazione': 'Economia',
    u'internazionalizzazione': 'Economia',
    u'conti economici': 'Economia',
    u'pubblica amministrazione': 'Amministrazione',
    u'istruzione formazione': 'Conoscenza',
    u'ricerca': 'Conoscenza',
    u'sviluppo e innovazione': 'Conoscenza',
    u'mercato del lavoro': 'Welfare',
    u'salute': 'Welfare',
    u'famiglie e comportamenti sociali': 'Welfare',
    u'assistenza e protezione sociale': 'Welfare',
    u'popolazione': 'Demografia',
    u'società dell\'informazione': 'Demografia',
}

cat_map_sp = {
    u"l'ambiente e il territorio": "Gestione del territorio",
    u'le infrastrutture': "Gestione del territorio",
    u'popolazione': "Demografia",
    u'famiglie e comportamenti sociali': 'Demografia',
    u'istruzione e formazione': 'Conoscenza',
    u'mercato del lavoro': 'Economia',
    u'le imprese, la formazione e la valorizzazione del capitale produttivo':
    'Economia',
    u'agricoltura': 'Economia',
    u'servizi': 'Economia',
    u'agricoltura, silvicoltura, pesca': 'Economia',
}

tipoindicatore_map = {
    u'R': 'Rapporto',
    u'M': 'Media',
    u'I': 'Incremento anno precedente',
}


def clean_tags(taglist):
    """
    Tags are only alphanum with '_-.'
    """
    tags = []
    for word in (tag.lower().replace('  ', ' ') for tag in taglist):
        for cleaned in (w.strip() for w in word.split(',')):
            if cleaned in tags_remove:
                continue
            tag = tags_subs.get(cleaned, cleaned)
            if len(tag) > 1:
                # "'" are not accepted by ckan
                tags.append(tag.replace("'", " "))
    return tags


# FIXME: This is to monkey patch ckanclient
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
    url = urljoin(self.base_location + urlparse(self.base_location).netloc,
                  selector)
    req = requests.post(
        url, data=dict(fields), files={files[0][0]: files[0][1:]},
        headers=headers
    )
    return req.status_code, req.error, req.headers, req.text


import ckanclient

# FIXME: no monkey patching here
ckanclient.CkanClient._post_multipart = _post_multipart

log = logging.getLogger(__name__)

DATASET_KEYS = ("Indicatore", "TabNumeratore", "TabDenominatore",
                "URLIndicatoreD", "URLTabNumMD", "URLTabDenMD")

# patched ckanclient functions for upload
CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB


def download_big_file(url):
    """
    Download a file on a tempfile without exploding in memory
    return the created file name
    """
    log.debug('Downloading: %s', url)
    basefile, ext = os.path.basename(urlparse.urlsplit(url).path). \
        rpartition('.')[0::2]
    if ext != "":
        ext = '.' + ext
    fd, big_filename = mkstemp(prefix=basefile, suffix=ext)
    with os.fdopen(fd, "w") as f:
        #r = requests.get(url, stream=True)
        r = requests.get(url)

        if not r.ok:
            log.error('Cannot get "%s"', url)
            return None

        for chunk in r.iter_content(CHUNK_SIZE):
            f.write(chunk)

    return big_filename


def convert_csv(semicolon_csv):
    """
    converts from semicolon separated to comma separated
    and give the new file name
    """
    with open(semicolon_csv, mode="rU") as infile:
        reader = csv.reader(infile, delimiter=';')
        prefix = semicolon_csv.rpartition('.csv')[0] + '_'
        fd, comma_csv = mkstemp(prefix=prefix, suffix='.csv')

        with os.fdopen(fd, "w") as outfile:
            writer = csv.writer(outfile)

            fpattern = re.compile(r'^(-?\d+),(\d+)$')
            for line in reader:
                # changes numbers floating comma in floating point
                newline = [fpattern.sub(r'\1.\2', s) for s in line]
                writer.writerow(newline)

    return comma_csv


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

    def dateformat(d):
#       return d.strftime(r"%d/%m/%Y %H:%M")
        return d.isoformat()

    def format_description_stat():
        # This is markdown syntax for rich text on CKAN
        return u''.join((
            u'**%s**' % infodict['Descrizione'],
            u'.  \n**Area:** ', origmeta['Area'],
            u'.  \n**Settore:** ', origmeta['Settore'],
            u'.  \n**Algoritmo:** ', origmeta['Algoritmo'],
            u'.  \n**Unità di misura:** ', origmeta['UM'],
            u'.  \n**Fenomeno:** ', origmeta['Fenomeno'],
            u'.  \n**Confronti territoriali:** ',
            origmeta['ConfrontiTerritoriali'],
            u'.  \n**Note:** ', origmeta['Note'],
        ))

    def format_description_sp():
        return u''.join((
            u'**%s**' % infodict['Descrizione'],
            u'.  \n**Area:** ', origmeta['Area'],
            u'.  \n**Settore:** ', origmeta['Settore'],
            u'.  \n**Algoritmo:** ', origmeta['Algoritmo'],
            u'.  \n**Unità di misura:** ', origmeta[u'UnitàMisura'],
            u'.  \n**Confronti territoriali:** ',
            origmeta['ConfrontiTerritoriali'],
            u'.  \n**Livello Geografico Minimo:** ',
            origmeta['LivelloGeograficoMinimo'],
            u'.  \n**Anno Base:** ', origmeta['AnnoBase'],
            u'.  \n**Tipo Indicatore:** ',
            tipoindicatore_map.get(origmeta['TipoIndicatore'], u''),
        ))

    extras = {}
    if infodict["stat_type"] == "SP":
        category = cat_map_sp.get(
            origmeta.get('Settore', 'default').lower(), 'Conoscenza'
        )
        description = format_description_sp()
    else:
        category = cat_map_stat.get(
            origmeta.get('Settore', 'default').lower(), 'Conoscenza'
        )
        description = format_description_stat()
    try:
        extras = {
            u'Notes': description,
            u'Titolare': 'Provincia Autonoma di Trento',
            u'Categorie': category,
            u'Copertura Geografica': 'Provincia di Trento',
            u'Copertura Temporale (Data di inizio)': dateformat(created),
            u'Aggiornamento': origmeta.get('FreqAggiornamento') or
            origmeta.get('FrequenzaAggiornamento'),
            u'Data di pubblicazione': dateformat(datetime.datetime.now()),
            u'Data di aggiornamento': dateformat(modified),
            u'Codifica Caratteri': 'UTF-8',
        }
    except KeyError:
        log.error("Input format changed, fix the code")
    except UnicodeDecodeError:
        log.error("Encoding error, fix the code")

    return extras


class PatStatWebHarvester(HarvesterBase):
    INDEX_URLS = [
        "http://www.statweb.provincia.tn.it/IndicatoriStrutturali/exp.aspx",

        "http://www.statweb.provincia.tn.it/INDICATORISTRUTTURALISubPro/"
        "exp.aspx?list=i",
    ]

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
        ids = []
        for index in self.INDEX_URLS:
            r = requests.get(index)

            if not r.ok:
                return []

            try:
                json_data = r.json
                json_key = json_data.keys()[0]
                indicatori = json_data.values()[0]
            except KeyError, json.JSONDecodeError:
                return []

            for elem in indicatori:
                if "URL" in elem:  # old IndicatoriStrutturali
                    elem["stat_type"] = "stat"
                # IndicatoriStrutturaliSP don't require a get for the metadata
                else:
                    elem["stat_type"] = "SP"
                    elem["metadata"] = dict(elem)  # just for copying

                elem['json_root_key'] = json_key

                try:
                    id_ = elem['URL']  # This is just to keep the old id,
                except KeyError:
                    id_ = elem['URLIndicatoreD']
                elem['package_id'] = id_

                obj = HarvestObject(
                    guid=sha1(elem['package_id']).hexdigest(),
                    job=harvest_job,
                    content=json.dumps(elem)
                )
                obj.save()
                ids.append(obj.id)
        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In PatStatWebHarvester fetch_stage')

        elem = json.loads(harvest_object.content)

        if elem["stat_type"] == "stat":
            r = requests.get(elem['URL'])
            if not r.ok:
                log.error('Cannot get "%s"', elem['URL'])
                return []
            elem['metadata'] = r.json.values()[0][0]

        for resource_key in DATASET_KEYS:
            try:
                resource_url = elem['metadata'][resource_key]
            except KeyError:
                pass
            else:
                if not resource_url:
                    continue

                # download json
                log.debug("Downloading JSON: %s", resource_url)
                r1 = requests.get(resource_url)
                if not r1.ok:
                    # delete resources with broken urls
                    del elem[resource_key]
                else:
                    try:
                        r1_json = r1.json
                    except ValueError:
                        log.error("Empty or invalid JSON %s", resource_url)
                        continue
                    try:
                        first_elem = r1_json.values()[0][0]
                    except (IndexError, AttributeError):
                        log.error("Invalid JSON %s", resource_url)
                        continue
                    # if it's metadata follow the URLTabD link and download
                    # the real data
                    if "URLTabD" in first_elem:
                        resource_url = first_elem["URLTabD"]
                        elem['metadata'][resource_key] = resource_url
                        elem[resource_key] = \
                            r1_json.values()[0][0]['descrizione']
                    # Otherwise that's the real data
                    else:
                        elem[resource_key] = r1_json.keys()[0]
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
        modified = extras['Data di aggiornamento']

        tags = clean_tags(
            [elem['metadata']['Area'], elem['metadata']['Settore']]
        )

        package_dict = {
            u'id': sha1(elem['package_id']).hexdigest(),
            u'title': elem[u'Descrizione'],
            u'groups': ['statistica'],
            u'url': "http://www.statistica.provincia.tn.it",
            u'notes': extras.pop(u'Notes'),
            u'author': 'Servizio Statistica',
            u'author_email': 'serv.statistica@provincia.tn.it',
            u'maintainer': 'Servizio Statistica',
            u'maintainer_email': 'serv.statistica@provincia.tn.it',
            u'metadata_modified': modified,
            u'tags': tags,
            u'license_id': 'cc-by',
            u'license': u'Creative Commons Attribution',
            u'license_title': u'Creative Commons Attribution 3.0 it',
            u'license_url': u'http://creativecommons.org/licenses/by/3.0/it/',
            u'isopen': True,
            u'Categorie': extras.pop('Categorie'),
            u'extras': extras,
            u'resources': []
        }

        for resource_key in DATASET_KEYS:
            try:
                resource_url = elem['metadata'][resource_key]
            except KeyError:
                continue
            if not elem[resource_key]:
                continue

            name = elem[resource_key]

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
            csv_semicolon = elem['metadata'][resource_key + '_csv_path']
            csv_path = convert_csv(csv_semicolon)
            try:
                os.remove(csv_semicolon)

                junkurl, errmsg = ckan_client.upload_file(csv_path)
                url = junkurl.replace('http://', base_location)
                os.remove(csv_path)

                res_dict_csv = dict(res_dict)
                res_dict_csv["url"] = url
                res_dict_csv["format"] = 'csv'
                res_dict_csv["mimetype"] = 'text/csv'
                res_dict_csv["resource_type"] = 'file'
                package_dict['resources'].append(res_dict_csv)

            except OSError:
                log.error("Missing CSV: %s", csv_semicolon)
        package_dict['name'] = self._gen_new_name(package_dict['title'])
        return self._create_or_update_package(package_dict, harvest_object)
