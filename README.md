# ESA transformation framework

The Transformation Framework is a component of ESA DHS intended to provide 
data transformation capabilities via the integration of processing elements 
applied on-demand to Copernicus Sentinel products, prior to delivery to the users. 

In its current development status *Alpha*, it features:

* The ability to [obtain a list of available workflows via the REST API](#List-of-plugins)
* The ability to [submit a Transformation Order via the REST API](#Request-a-new-transformation)
* The ability to [monitor the status of an ongoing process via the REST API](#Monitoring-status-of-a-transformation-order)
* The installation of [Sen2Cor plugin](https://step.esa.int/main/snap-supported-plugins/sen2cor/)
* *Pluggability*, i.e. the capability to add new plugins
* *SRTM DEM* download in the processing directory 
  during the transformation, then removed after the end of the processing 

It can currently be deployed via docker-compose, as explained in the 
in [the Docker compose startup](#Docker-compose-startup) section.

## Notes

* Plugin memory requirements are set to a minimum of 6GB of RAM
* The activation of ESA-CCI data-package necessary for Sen2Cor plugin 
  to generate products compatible with L2A Core products is not included in this release
* The selection of a Region of Interest (ROI) 
  with Sen2Cor plugin is not yet supported by the REST API

## Docker compose startup

Required docker engine configuration:
* resources -> advanced -> memory `>4Gb`
* resources -> advanced -> disk image size `>50Gb`

Required software on the VM:
* `docker-compose`
* `make`
* `curl`
* `unzip`
* `tar`

Change folder to `esa_tf` and start the docker compose:
```bash
    cd esa_tf
    make setup
    make up
```

The API endpoints will be available on `http://localhost:8080`

## How to test API endpoints

Required software on the VM:
* `jq`

### Common Schema Definition Language (CSDL)

```bash
curl "http://localhost:8080/\$metadata"
```

### List of plugins

```bash
curl http://localhost:8080/Workflows | jq
```

### Access a plugin definition

```bash
curl "http://localhost:8080/Workflows('sen2cor_l1c_l2a')" | jq
```

### List of transformation orders

```bash
curl http://localhost:8080/TransformationOrders | jq
```

It is also possible to filter accessible orders (**TODO**):

```bash
curl "http://localhost:8080/TransformationOrders?\$filter=`jq -rn --arg x "Status eq 'completed'" '$x|@uri'`" | jq
```

### Request a new transformation

```bash
curl -v -d '{"WorkflowId": "sen2cor_l1c_l2a", "InputProductReference": {"Reference": "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"}, "WorkflowOptions": {"aerosol_type": "maritime", "mid_latitude": "auto", "ozone_content": 0, "cirrus_correction": true, "dem_terrain_correction": true, "row0": 600, "col0": 1200, "nrow_win": 600, "ncol_win": 600}}' -H "Content-Type: application/json" http://localhost:8080/TransformationOrders | jq
```

### Monitoring status of a transformation order

```bash
curl "http://localhost:8080/TransformationOrders('fe950364a8e9b37057d64f9d056edc05')" | jq # -r '.Id'
```

To submit a transformation order and monitor it's state, in one shot:

```bash
curl -v -d '{"WorkflowId": "sen2cor_l1c_l2a", "InputProductReference": {"Reference": "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"}, "WorkflowOptions": {"aerosol_type": "maritime", "mid_latitude": "auto", "ozone_content": 0, "cirrus_correction": true, "dem_terrain_correction": true, "row0": 600, "col0": 1200, "nrow_win": 600, "ncol_win": 600}}' -H "Content-Type: application/json" http://localhost:8080/TransformationOrders | jq -r '.Id' | curl "http://localhost:8080/TransformationOrders('`cat -`')" | jq
```



# License information

Copyright 2021-2022, European Space Agency (ESA)

Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE Version 3 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://opensource.org/licenses/AGPL-3.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.