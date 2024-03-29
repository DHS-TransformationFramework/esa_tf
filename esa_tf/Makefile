ESACCI_ZIP := ESACCI-LC-L4-ALL-FOR-SEN2COR.zip
ESACCI_URL := ftp://geo10.elie.ucl.ac.be/CCI/LandCover/$(ESACCI_ZIP)
ESACCI_TAR := CCI4SEN2COR/ESACCI-LC-L4-ALL-FOR-SEN2COR.tar
define HUBS_CREDENTIALS
# example for: config/hubs_credentials.yaml
scihub:
  api_type: dhus-api
  credentials:
    api_url: https://scihub.copernicus.eu/dhus
    user: my-scihub-username
    password: my-scihub-password
cdse:
  api_type: csc-api
  auth: oauth2
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
  auth: basic
  query_auth: True
  download_auth: True
  credentials:
    api_url: https://dhs-test.onda-dias.eu/gss-cat-01
    user: my-gss-username
    password: my-gss-password
endef

define TRACEABILITY_CONFIG
service_url: https://demo.trace.gael-systems.com
url_access_token: https://demo.trace.gael-systems.com/auth/realms/ts/protocol/openid-connect/token
url_push_trace: https://demo.trace.gael-systems.com/trace-api/Traces
username: my-traceability-username
password: my-traceability-password
key_fingerprint: my-key-fingerprint
passphrase: my-key-passphrase
service_context: my-service
service_provider: my-service-provider
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

# Optional, default true
# it enables traceability pushing traces on the configured Traceability Service
enable_traceability: true

# list of workflows for which the trace should not be sent
# default empty list
untraced_workflows: []

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

export TRACEABILITY_CONFIG
config/traceability_config.yaml:
	echo "$$TRACEABILITY_CONFIG" > $@

setup: config/traceability_config.yaml

export ESA_TF_CONFIG
config/esa_tf.config:
	echo "$$ESA_TF_CONFIG" > $@

setup: config/esa_tf.config

plugins:
	mkdir $@

traces:
	mkdir $@

setup: plugins traces

build: setup
	make -C ../esa_tf_restapi image
	make -C ../esa_tf_platform image

up:
	docker compose up

down:
	docker compose down
