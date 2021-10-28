# ESA transformation framework

## Docker compose startup

Required docker engine configuration:
* resources -> advanced -> memory `>4Gb`
* resources -> advanced -> disk image size `>50Gb`

Required software on the VM:
* `docker-compose`
* `make`
* `curl`

Change folder to `esa_tf` and start the docker compose:
```bash
cd esa_tf
make up
```

The API endpoints will be available on `http://localhost:8080`

## How to test API endpoints

### Common Schema Definition Language (CSDL) - **TODO**

```bash
curl "http://localhost:8080/\$metadata"
```

### List of plugins

```bash
curl http://localhost:8080/Workflows
```

### Access a plugin definition

```bash
curl "http://localhost:8080/Workflows('sen2cor_l1c_l2a')"
```

### List of transformation orders

```bash
curl http://localhost:8080/TransformationOrders
```

It is also possible to filter accessible orders:

```bash
curl "http://localhost:8080/TransformationOrders?\$filter=Status%20eq%20'completed'"
```

### Request a new transformation

```bash
curl -v -d '{"WorkflowId": "sen2cor_l1c_l2a", "InputProductReference": {"Reference": "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"}, "WorkflowOptions": {"aerosol_type": "maritime", "mid_latitude": "auto", "ozone_content": 0, "cirrus_correction": true, "dem_terrain_correction": true, "row0": 600, "col0": 1200, "nrow_win": 600, "ncol_win": 600}}' -H "Content-Type: application/json" http://localhost:8080/TransformationOrders
```

### Monitoring status of a transformation order

```bash
curl "http://localhost:8080/TransformationOrders('cd1c192c-7dd2-4250-af0f-13528680d371')"
```
