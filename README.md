# ESA transformation framework

## Docker compose startup

Required docker engine configuration:
* resources -> advanced -> memory >4Gb
* resources -> advanced -> disk image size >50Gb

Software on the VM:
* docker-compose
* make
* wget

Change folder to `esa_tf` and start the docker compose:
```shell
    cd esa_tf
    make up
```
