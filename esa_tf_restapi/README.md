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
curl "http://localhost:8000/\$metadata"
```

### List of plugins

```bash
curl http://localhost:8000/Workflows | jq
```

Response:

```json
{
  "@odata.context": "http://127.0.0.1:8080/Workflows/$metadata#Workflow",
  "value": [
    {
      "Description": "Product processing from Sentinel-2 L1C to L2A. Processor V2.3.6",
      "InputProductType": "S2MSILC",
      "OutputProductType": "S2MSI2A",
      "WorkflowVersion": "0.1",
      "WorkflowOptions": [
        {
          "Name": "aerosol_type",
          "Description": "Default processing via configuration is the rural (continental) aerosol type with mid latitude summer and an ozone concentration of 331 Dobson Units",
          "Type": "string",
          "Default": "rural",
          "Values": [
            "maritime",
            "rural"
          ]
        },
        {
          "Name": "mid_latitude",
          "Description": "If  'AUTO' the atmosphere profile will be determined automatically by the processor, selecting WINTER or SUMMER atmosphere profile based on the acquisition date and geographic location of the tile",
          "Type": "string",
          "Default": "summer",
          "Values": [
            "summer",
            "winter",
            "auto"
          ]
        },
        {
          "Name": "ozone_content",
          "Description": "0: to get the best approximation from metadata (this is the smallest difference between metadata and column DU), else select for midlatitude summer (MS) atmosphere: 250, 290, 331 (standard MS), 370, 410, 450; for midlatitude winter (MW) atmosphere: 250, 290, 330, 377 (standard MW), 420, 460",
          "Type": "integer",
          "Default": 331,
          "Values": [
            0,
            250,
            290,
            330,
            331,
            370,
            377,
            410,
            420,
            450,
            460
          ]
        },
        {
          "Name": "cirrus_correction",
          "Description": "FALSE: no cirrus correction applied, TRUE: cirrus correction applied",
          "Type": "boolean",
          "Default": false,
          "Values": [
            true,
            false
          ]
        },
        {
          "Name": "dem_terrain_correction",
          "Description": "Use DEM for Terrain Correction, otherwise only used for WVP and AOT",
          "Type": "boolean",
          "Default": true,
          "Values": [
            true,
            false
          ]
        },
        {
          "Name": "resolution",
          "Description": "Target resolution, can be 10, 20 or 60m. If omitted, only 20 and 10m resolutions will be processed",
          "Type": "boolean",
          "Default": true,
          "Values": [
            10,
            20,
            60
          ]
        }
      ],
      "Id": "sen2cor_l1c_l2a"
    }
  ]
}
```

### Access a single plugin definition

```bash
curl "http://localhost:8000/Workflows('6c18b57d-fgk4-1236-b539-12h305c26z89')" | jq
```

Response:

```json
{
  "@odata.id": "http://127.0.0.1:8080/Workflows('sen2cor_l1c_l2a')/Workflows('sen2cor_l1c_l2a')",
  "@odata.context": "http://127.0.0.1:8080/Workflows('sen2cor_l1c_l2a')/$metadata#Workflow('sen2cor_l1c_l2a')",
  "Id": "sen2cor_l1c_l2a",
  "Description": "Product processing from Sentinel-2 L1C to L2A. Processor V2.3.6",
  "InputProductType": "S2MSILC",
  "OutputProductType": "S2MSI2A",
  "WorkflowVersion": "0.1",
  "WorkflowOptions": [
    {
      "Name": "aerosol_type",
      "Description": "Default processing via configuration is the rural (continental) aerosol type with mid latitude summer and an ozone concentration of 331 Dobson Units",
      "Type": "string",
      "Default": "rural",
      "Values": [
        "maritime",
        "rural"
      ]
    },
    {
      "Name": "mid_latitude",
      "Description": "If  'AUTO' the atmosphere profile will be determined automatically by the processor, selecting WINTER or SUMMER atmosphere profile based on the acquisition date and geographic location of the tile",
      "Type": "string",
      "Default": "summer",
      "Values": [
        "summer",
        "winter",
        "auto"
      ]
    },
    {
      "Name": "ozone_content",
      "Description": "0: to get the best approximation from metadata (this is the smallest difference between metadata and column DU), else select for midlatitude summer (MS) atmosphere: 250, 290, 331 (standard MS), 370, 410, 450; for midlatitude winter (MW) atmosphere: 250, 290, 330, 377 (standard MW), 420, 460",
      "Type": "integer",
      "Default": 331,
      "Values": [
        0,
        250,
        290,
        330,
        331,
        370,
        377,
        410,
        420,
        450,
        460
      ]
    },
    {
      "Name": "cirrus_correction",
      "Description": "FALSE: no cirrus correction applied, TRUE: cirrus correction applied",
      "Type": "boolean",
      "Default": false,
      "Values": [
        true,
        false
      ]
    },
    {
      "Name": "dem_terrain_correction",
      "Description": "Use DEM for Terrain Correction, otherwise only used for WVP and AOT",
      "Type": "boolean",
      "Default": true,
      "Values": [
        true,
        false
      ]
    },
    {
      "Name": "resolution",
      "Description": "Target resolution, can be 10, 20 or 60m. If omitted, only 20 and 10m resolutions will be processed",
      "Type": "boolean",
      "Default": true,
      "Values": [
        10,
        20,
        60
      ]
    }
  ]
}
```

### List of transformation orders

```bash
curl http://localhost:8000/TransformationOrders | jq
```

It is also possible to filter accessible orders:

```bash
curl "http://localhost:8000/TransformationOrders?\$filter=`jq -rn --arg x "Status eq 'completed'" '$x|@uri'`" | jq
```

### Request a new transformation

```bash
curl -d '{"key1":"value1", "key2":"value2"}' -H "Content-Type: application/json" http://localhost:8000/TransformationOrders | jq
```

### State of a transformation

```bash
curl -v -d '{"WorkflowId": "sen2cor_l1c_l2a", "InputProductReference": {"Reference": "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"}, "WorkflowOptions": {"aerosol_type": "maritime", "mid_latitude": "auto", "ozone_content": 0, "cirrus_correction": true, "dem_terrain_correction": true, "row0": 600, "col0": 1200, "nrow_win": 600, "ncol_win": 600}}' -H "Content-Type: application/json" http://localhost:8080/TransformationOrders | jq
```
