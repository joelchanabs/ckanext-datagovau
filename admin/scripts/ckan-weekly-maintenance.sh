#!/bin/bash

CKAN_DIR="/usr/lib/ckan/dga"

${CKAN_DIR}"/bin/paster" --plugin=ckanext-datagovau cleanupdatastoregeoserver clean-all --config=/etc/ckan/dga/harvester.ini