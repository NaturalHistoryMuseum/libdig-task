#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

Basic task for creating a dataset from digitised library images

python task.py LibraryDigitisationTask  --local-scheduler

"""

import luigi
import os
import xml.etree.ElementTree as ET
import urllib
import urllib2
import json
import re
from PIL import Image
from ConfigParser import ConfigParser

config = ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'client.cfg'))



class LibraryDigitisationTask(luigi.Task):

    thumbnail_size = 128, 128
    image_size = 1000, 1000

    rgx = re.compile('([0-9]+)_([0-9]+)')

    package = {
            'name': u'library-digitisation',
            'notes': u'The Natural History Museum Library\'s digitised books',
            'title': "Library digitisation",
            'author': 'Natural History Museum',
            'license_id': u'other-open',
            'resources': [],
            'dataset_type': 'Library and archives',
            'owner_org': config.get('ckan', 'owner_org')
        }

    def __init__(self, *args, **kwargs):
        """
        Override init to retrieve the resource id
        @param args:
        @param kwargs:
        @return:
        """

        self.source_dir = config.get('pipeline', 'source_dir')
        # Where to output the resized images
        self.target_dir = config.get('pipeline', 'target_dir')

        # Load metadata
        self._load_metadata()

        super(LibraryDigitisationTask, self).__init__(*args, **kwargs)

    def _load_metadata(self):

        metadata_file = config.get('pipeline', 'metadata')

        tree = ET.parse(metadata_file)
        root = tree.getroot()

        self.metadata_objects = {}

        for child in root:

            metadata_object = {}

            catalogue_key = child.find('{http://www.loc.gov/mods/v3}recordInfo/{http://www.loc.gov/mods/v3}recordIdentifier').text
            catalogue_key = catalogue_key.replace('Catkey', '').strip()

            titles = child.findall('{http://www.loc.gov/mods/v3}titleInfo')

            metadata_object['title'] = titles[1].find('{http://www.loc.gov/mods/v3}title').text

            metadata_object['date'] = child.find('{http://www.loc.gov/mods/v3}originInfo/{http://www.loc.gov/mods/v3}place/{http://www.loc.gov/mods/v3}placeTerm').text

            metadata_object['issuance'] = child.find('{http://www.loc.gov/mods/v3}originInfo/{http://www.loc.gov/mods/v3}issuance').text

            metadata_object['extent'] = child.find('{http://www.loc.gov/mods/v3}physicalDescription/{http://www.loc.gov/mods/v3}extent').text

            metadata_object['name'] = child.find('{http://www.loc.gov/mods/v3}name/{http://www.loc.gov/mods/v3}namePart').text

            contents_elem = child.find('{http://www.loc.gov/mods/v3}tableOfContents')

            if contents_elem:
                metadata_object['table_of_contents'] = contents_elem.text

            self.metadata_objects[int(catalogue_key)] = metadata_object


    def call_action(self, action, data_dict):
        """
        API Call
        @param action: API action
        @param data: dict
        @return:
        """
        url = '{site_url}/api/3/action/{action}'.format(
            site_url=config.get('ckan', 'site_url'),
            action=action
        )

        # Use the json module to dump a dictionary to a string for posting.
        data_string = urllib.quote(json.dumps(data_dict))

        # New urllib request
        request = urllib2.Request(url)

        request.add_header('Authorization', config.get('ckan', 'api_key'))

        # Make the HTTP request.
        response = urllib2.urlopen(request, data_string)
        # Ensure we have correct response code 200
        assert response.code == 200

        # Use the json module to load CKAN's response into a dictionary.
        response_dict = json.loads(response.read())

        # Check the contents of the response.
        assert response_dict['success'] is True
        result = response_dict['result']

        return result

    def parse_filename(self, f):
        m = re.search('([0-9]+)[\-_]*([0-9]{3})', f)
        return int(m.group(1)), int(m.group(2))

    def run(self):

        records = []

        try:
            package = self.call_action('package_show', {'id': self.package['name']})
        except urllib2.HTTPError:
            # Dataset does not exist, so create it now
            package = self.call_action('package_create', self.package)

        # Target dir for files, with package ID appended
        dir = os.path.join(self.target_dir, package['name'])

        # Create if it doesn't exist
        if not os.path.exists(dir):
            os.makedirs(dir)

        # Recursively loop through all the files
        for root, dirs, files in os.walk(self.source_dir):

            for f in files:

                try:
                    catkey, page = self.parse_filename(f)
                except AttributeError:
                    # Could not extract key and page num eg: .DS_Store
                    continue

                record = self.metadata_objects[catkey].copy()

                record['catalogue_key'] = catkey
                record['page'] = page

                # Resize images
                thumbnail_name = 'thumb_' + f
                thumbnail = os.path.join(dir, thumbnail_name)
                image = os.path.join(dir, f)

                # Save thumbnail
                if not os.path.isfile(thumbnail):
                    try:
                        im = Image.open(os.path.join(root, f))
                    except IOError:
                        print 'ERROR reading image %s' % f
                        continue
                    else:
                        im.thumbnail(self.thumbnail_size, Image.ANTIALIAS)
                        im.save(thumbnail, "JPEG")

                # Save large image
                if not os.path.isfile(image):
                    try:
                        im = Image.open(os.path.join(root, f))
                    except IOError:
                        print 'ERROR reading image %s' % f
                        continue
                    else:
                        im.thumbnail(self.image_size, Image.ANTIALIAS)
                        im.save(image, "JPEG")

                record['thumbnail'] = '/' + os.path.join(package['name'], thumbnail_name)
                record['image'] = '/' + os.path.join(package['name'], f)

                records.append(record)

        # And now save to the datastore
        datastore_params = {
            'records': records,
            'resource': {
                'name': 'Sykes collection',
                'description': 'Manuscript and drawing collection of William Henry Sykes (1790-1872)',
                'package_id': package['id'],
                'format': 'csv'
            },
        }

        # API call to create the datastore
        datastore = self.call_action('datastore_create', datastore_params)

if __name__ == "__main__":
    luigi.run()



