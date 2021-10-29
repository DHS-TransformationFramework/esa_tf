# ESA transformation framework REST API

## How to run

### Development

```bash
make start
```

### Run service

```bash
make serve
```

Remember to define the `WEB_CONCURRENCY` envvar (default is 1).

## Running tests

```bash
make test
```

## Endpoints

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
curl "http://localhost:8080/TransformationOrders('cd1c192c-7dd2-4250-af0f-13528680d371')" | jq # -r '.Id'
```

To submit a transformation order and monitor it's state, in one shot:

```bash
curl -v -d '{"WorkflowId": "sen2cor_l1c_l2a", "InputProductReference": {"Reference": "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"}, "WorkflowOptions": {"aerosol_type": "maritime", "mid_latitude": "auto", "ozone_content": 0, "cirrus_correction": true, "dem_terrain_correction": true, "row0": 600, "col0": 1200, "nrow_win": 600, "ncol_win": 600}}' -H "Content-Type: application/json" http://localhost:8080/TransformationOrders | jq -r '.Id' | curl "http://localhost:8080/TransformationOrders('`cat -`')" | jq
```

## License information

```
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
