# ESA transformation framework

The ESA Transformation Framework is a component of the
Copernicus Sentinels Collaborative Data Hub Software (DHS) intended to provide
data transformation capabilities via the integration of processing elements
applied on-demand to Copernicus Sentinel products, prior to delivery to the users.

## Features

In its current development status *Alpha*, it features:

- The ability to define and configure workflows
  (i.e. processing elements), via a plugin architecture. (DHS-EVO-31, DHS-EVO-36)
- The ability to perform parallel processing on the local host
  or over a distributed architecture. (DHS-EVO-33)
- The functionalities which allow setting workflow parameters and options
  [via a REST API](#How-to-test-API-endpoints). (DHS-EVO-32)
- A [Sen2Cor plugin](https://step.esa.int/main/snap-supported-plugins/sen2cor/)
  which uses Sen2Cor v2.10 tool to convert Sentinel-2 L1C products into L2A output products,
  also using the *SRTM DEM* for terrain correction, classification and atmospheric correction. (DHS-EVO-35)
- A deployment [via docker-compose](#Docker-compose-startup). (DHS-MNT-16)
- Continuous Integration and Continuous Delivery (CI/CD) via GitHub Actions.

Other features in the roadmap:

- Product reformatting plugin (GeoTIFF, etc) (DHS-EVO-34)
- Add support for multiple users.
- Compute traceability record and upload it to the traceability service.
- Integrate logging and monitoring in the DHS.
- Installation and administration manual.

### Notes

- The activation of ESA-CCI data-package necessary for Sen2Cor plugin
  to generate products compatible with L2A Core products is not included in this release.
- The selection of a Region of Interest (ROI)
  with Sen2Cor plugin is not yet supported by the REST API.
- Failed requests are not re-tried.

## Docker compose setup

Required docker engine configuration:

- resources -> advanced -> memory `>6Gb`
- resources -> advanced -> disk image size `>50Gb`

Required software on the VM:

- `docker-compose`
- `make`
- `curl`
- `unzip`
- `tar`

Change folder to `esa_tf` and download the external resources:

```bash
cd esa_tf
make setup
```

Configure the users names and passwords to access the external data sources in the file
`config/hubs_credentials.yaml`, the folder where the ESA Transformation Framework
will place the outputs and the owner userid in the `.env` file.

Finally, start the docker compose:

```bash
make up
```

The API endpoints will be available on `http://localhost:8080`.

## How to test API endpoints

To easily test REST API from the command line you can use the following softwares:

- `curl`
- `jq`

### List of plugins

```bash
curl http://localhost:8080/Workflows | jq
```

### Access plugin definition

```bash
curl "http://localhost:8080/Workflows('sen2cor_l1c_l2a')" | jq
```

### List of transformation orders

```bash
curl http://localhost:8080/TransformationOrders | jq
```

#### Filters transformation orders

It is also possible to filter accessible orders.
Filters are applied using standard OData `$filter` query parameter; following clauses can be used:

`Status`
: Filter by transformation status (e.g: `in_progress`, `completed`, …)  
  Example:

  ```bash
  curl "http://localhost:8080/TransformationOrders?\$filter=Status%20eq%20'completed'" | jq
  ```
  
`SubmissionDate`
: Filter by submission date. Date must be a string in ISO format.  
  Example:

  ```bash
  curl "http://localhost:8080/TransformationOrders?\$filter=SubmissionDate%20eq%20'2022-01-25T08:53:47.961866'" | jq
  ```

`CompletedDate`
: Filter by date of completion. Date must be a string in ISO format.  
  Example:

  ```bash
  curl "http://localhost:8080/TransformationOrders?\$filter=CompletedDate%20eq%20'2022-01-25T09:07:51.908863'" | jq
  ```

`InputProductReference`
: Filter by input product filename. This will be treated as an exact match  
  Example:

  ```bash
  curl "http://localhost:8080/TransformationOrders?\$filter=InputProductReference%20eq%20'S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip'" | jq
  ```

It's also possible to combine filters by using then `and` operator:

```bash
curl "http://localhost:8080/TransformationOrders?\$filter=CompletedDate%20eq%20'2022-01-25T09:07:51.908863'%20and%20InputProductReference%20eq%20'S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip'" | jq
```

The `and` operator is not the only one supported.
You can also use:

- `lt`
- `gt`
- `ge`
- `le`

A filter request like the following is perfectly valid:

```bash
curl "http://localhost:8080/TransformationOrders?\$filter=CompletedDate%20ge%20'2022-01-01'%20and%20CompletedDate%20lt%20'2022-02-01'" | jq
```

#### Count results

You can ask for number of results by using the `$count` parameter:

```bash
curl "http://localhost:8080/TransformationOrders?\$filter=Status%20eq%20'completed'&\$count=true" | jq
```

This will add the `odata.count` field to the results set.

To obtain directly the number of transformation you can access, you can use the `$count` path suffix:

```bash
curl "http://localhost:8080/TransformationOrders/\$count" | jq
```

### Request a new transformation

```bash
curl -v -d '{"WorkflowId": "sen2cor_l1c_l2a", "InputProductReference": {"Reference": "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip", "DataSourceName": "scihub"}, "WorkflowOptions": {"Aerosol_Type": "MARITIME", "Mid_Latitude": "AUTO", "Ozone_Content": 0, "Cirrus_Correction": true, "DEM_Terrain_Correction": true}}' -H "Content-Type: application/json" http://localhost:8080/TransformationOrders | jq
```

### Monitoring status of a transformation order

```bash
curl "http://localhost:8080/TransformationOrders('9e58ff8a4553a15607eae4ce85736811')" | jq
```

You can also inspect the current output from the transformation itself.

```bash
curl "http://localhost:8080/TransformationOrders('9e58ff8a4553a15607eae4ce85736811')/Log/\$value"
```

## License information

```text
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
```
