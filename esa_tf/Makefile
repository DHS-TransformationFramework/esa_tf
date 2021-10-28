ESACCI_ZIP := ESACCI-LC-L4-ALL-FOR-SEN2COR.zip
ESACCI_URL := ftp://geo10.elie.ucl.ac.be/CCI/LandCover/$(ESACCI_ZIP)
ESACCI_TAR := CCI4SEN2COR/ESACCI-LC-L4-ALL-FOR-SEN2COR.tar

$(ESACCI_ZIP):
	curl -LO $(ESACCI_URL)

data/land-cover/.timestamp: $(ESACCI_ZIP)
	mkdir -p data/land-cover
	unzip $< -d tmp
	tar -xvmf tmp/$(ESACCI_TAR) -C data/land-cover
	$(RM) -r tmp
	touch data/land-cover/.timestamp

setup:
	make -C ../esa_tf_platform setup

up:
	docker-compose up

down:
	docker-compose down
