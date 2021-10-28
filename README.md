# ESA transformation framework

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
curl "http://localhost:8080/TransformationOrders('cd1c192c-7dd2-4250-af0f-13528680d371')" | jq # -r '.Id'
```

To submit a transformation order and monitor it's state, in one shot:

```bash
curl -v -d '{"WorkflowId": "sen2cor_l1c_l2a", "InputProductReference": {"Reference": "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"}, "WorkflowOptions": {"aerosol_type": "maritime", "mid_latitude": "auto", "ozone_content": 0, "cirrus_correction": true, "dem_terrain_correction": true, "row0": 600, "col0": 1200, "nrow_win": 600, "ncol_win": 600}}' -H "Content-Type: application/json" http://localhost:8080/TransformationOrders | jq -r '.Id' | curl "http://localhost:8080/TransformationOrders('`cat -`')" | jq
```
