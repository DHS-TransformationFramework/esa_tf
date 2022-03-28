ESACCI_ZIP := ESACCI-LC-L4-ALL-FOR-SEN2COR.zip
ESACCI_URL := ftp://geo10.elie.ucl.ac.be/CCI/LandCover/$(ESACCI_ZIP)
ESACCI_TAR := CCI4SEN2COR/ESACCI-LC-L4-ALL-FOR-SEN2COR.tar
define HUBS_CREDENTIALS
# example for: config/hubs_credentials.yaml
scihub:
  api_url: https://scihub.copernicus.eu/dhus
  user: my-scihub-username
  password: my-scihub-password
apihub:
  api_url: https://apihub.copernicus.eu/apihub
  user: my-apihub-username
  password: my-apihub-password
endef

define ESA_TF_CONFIG
# The keeping_period shall be an integer. It represents the minimum number of minutes from the
# CompletedDate that a TransformationOrder will be kept in memory.
keeping-period: 14400  # minutes

# The list of workflows to be excluded
exclude-workflow:

# enable authorization check
enable_authorization_check: true

# enable quota configuration
enable_quota_check: true

# roles configuration
# - quota is maximum number of transformation orders in progress or queued per user
# - profile can ben "manager", "user" or "unauthorized"
# default define the defaults if no specific role is associated to a given user
# or if the user's roles are not explicit defined in the current config file
roles:
  default:
    quota: 0
    profile: unauthorized
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

build: setup
	make -C ../esa_tf_restapi image
	make -C ../esa_tf_platform image

up:
	docker-compose up

down:
	docker-compose down
