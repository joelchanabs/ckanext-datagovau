export NEW_RELIC_CONFIG_FILE="newrelic.ini"
export VIRTUAL_ENV="/var/lib/ckan/dga/pyenv"
export PATH="/var/lib/ckan/dga/pyenv:/var/lib/ckan/dga/pyenv/bin:$PATH"
cd /var/lib/ckan/dga/pyenv/src/ckan
newrelic-admin run-program paster serve development.ini
