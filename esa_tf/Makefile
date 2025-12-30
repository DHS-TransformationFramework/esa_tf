ESACCI_ZIP := ESACCI-LC-L4-ALL-FOR-SEN2COR.zip
ESACCI_URL := ftp://geo10.elie.ucl.ac.be/CCI/LandCover/$(ESACCI_ZIP)
ESACCI_TAR := CCI4SEN2COR/ESACCI-LC-L4-ALL-FOR-SEN2COR.tar
DOCKERFLAGS := --platform linux/amd64
define HUBS_CREDENTIALS
# example for: config/hubs_credentials.yaml
cda:
  api_type: csc-api
  auth: oauth2
  query_api: odata
  query_auth: False
  download_auth: True
  credentials:
    api_url: https://cda.copernicus.eu/cda/
    user: my-cda-username
    password: my-cda-password
    token_endpoint: https://auth.cda.copernicus.eu/auth/realms/CollaborativeDataAccess/protocol/openid-connect/token
    client_id: cda-gss
cdse:
  api_type: csc-api
  auth: oauth2
  query_api: stac
  query_auth: False
  download_auth: True
  credentials:
    api_url: https://catalogue.dataspace.copernicus.eu
    user: my-cdse-username
    password: my-cdse-password
    token_endpoint: https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token
    client_id: cdse-public
gss:
  api_type: csc-api
  auth: oauth2
  query_api: stac
  query_auth: True
  download_auth: True
  credentials:
    api_url: https://dhs-test.onda-dias.eu/stac-02/stac/
    user: my-gss-username
    password: my-gss-password
    token_endpoint: https://collaborative-dhs.onda-dias.eu/auth/realms/collaborative-dhs/protocol/openid-connect/token
    client_id: ivv-gss1-client
endef

define ESA_TF_CONFIG
# Mandatory
# it is the minimum number of minutes from the CompletedDate
# that a TransformationOrder will be kept in memory.
# The keeping_period shall be an integer.
keeping_period: 14400  # minutes

# Optional, default empty list
# The list of workflows id to be excluded
excluded_workflows: []

# Optional, default true
# it enables authorization check
enable_authorization_check: true

# Optional, default true
# it enables the resource monitoring
enable_monitoring: true

# Optional, default 10
# define the polling time for monitoring statistics computation
monitoring_polling_time_s: 10

# Optional, default true
# it enables quota configuration
enable_quota_check: true


# role configuration
# - quota is maximum number of transformation orders in progress or queued per user
# - profile can ben "manager" or "user"

# Optional, default quota: 2, profile: user
# Default role configuration
default_role:
    quota: 2
    profile: user

# Optional, default empty dictionary
# Custom roles configuration
# roles:
#   <role>:
#       quota: 1000
#       profile: manager
endef

# default target
default:
	echo No default target

$(ESACCI_ZIP):
	curl -LO $(ESACCI_URL)

data/land-cover/.timestamp: $(ESACCI_ZIP)
	mkdir -p data/land-cover
	unzip $< -d tmp
	tar -xvmf tmp/$(ESACCI_TAR) -C data/land-cover
	$(RM) -r tmp
	touch data/land-cover/.timestamp

export HUBS_CREDENTIALS
config/hubs_credentials.yaml:
	echo "$$HUBS_CREDENTIALS" > $@

setup: config/hubs_credentials.yaml

export ESA_TF_CONFIG
config/esa_tf.config:
	echo "$$ESA_TF_CONFIG" > $@

setup: config/esa_tf.config

plugins:
	mkdir $@

build: setup
	$(MAKE) -C ../esa_tf_restapi image DOCKERFLAGS="${DOCKERFLAGS}"
	$(MAKE) -C ../esa_tf_platform image DOCKERFLAGS="${DOCKERFLAGS}"

up:
	docker compose up

down:
	docker compose down
