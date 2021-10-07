from fastapi import Request, Response, Query
from typing import Optional, Any, List

from . import app
from .csdl import loadDefinition
from .odata import parseQS


@app.get("/$metadata")
async def metadata():
    return loadDefinition()


@app.get("/Workflows")
async def workflows(request: Request):
    base = request.url_for("workflows")
    id = "6c18b57d-fgk4-1236-b539-12h305c26z89"
    return {
        "@odata.context": f"{base}/$metadata#Workflow",
        # "@odata.nextLink": "https://services.odata.org/V4/TripPinService/People?%24select=FirstName&%24skiptoken=8",
        "value": [
            {
                "Id": id,
                "Name": "S2_L1C_L2A",
                "Description": "Product processing from Sentinel-2 L1C to L2A. Processor V2.3.6",
                "InputProductType": "S2MSILC",
                "OutputProductType": "S2MSI2A",
                "WorkflowVersion": "1.2",
                "WorkflowOptions": [
                    {
                        "Name": "Aerosol_Type",
                        "Description": "Default processing via configuration is the rural (continental) aerosol type with mid latitude summer and an ozone concentration of 331 Dobson Units",
                        "Type": "String",
                        "Default": "RURAL",
                        "Value": ["MARITIME", "RURAL", "AUTO"],
                    },
                ],
            }
        ],
    }


@app.get("/Workflows('{id}')", name="workflow")
async def workflow(request: Request, id: str):
    base = request.url_for("workflow", id=id)
    return {
        "@odata.id": f"{base}/Workflows('{id}')",
        "@odata.context": f"{base}/$metadata",
        "Id": "6c18b57d-fgk4-1236-b539-12h305c26z89",
        "Name": "S2_L1C_L2A",
        "Description": "Product processing from Sentinel-2 L1C to L2A. Processor V2.3.6",
        "InputProductType": "S2MSILC",
        "OutputProductType": "S2MSI2A",
        "WorkflowVersion": "1.2",
        "WorkflowOptions": [
            {
                "Name": "Aerosol_Type",
                "Description": "Default processing via configuration is the rural (continental) aerosol type with mid latitude summer and an ozone concentration of 331 Dobson Units",
                "Type": "String",
                "Default": "RURAL",
                "Value": ["MARITIME", "RURAL", "AUTO"],
            },
        ],
    }


@app.get("/TransformationOrders")
async def tranformation_orders(
    request: Request,
    rawfilter: Optional[str] = Query(
        None, alias="$filter", title="OData $filter query", max_length=50
    ),
):
    odata_params = parseQS(filter=rawfilter)
    filter = odata_params.filter
    print(filter)
    base = request.url_for("tranformation_orders")
    return {
        "@odata.context": f"{base}/$metadata#TransformationOrder",
        # "@odata.nextLink": "https://services.odata.org/V4/TripPinService/People?%24select=FirstName&%24skiptoken=8",
        "value": [
            {
                "Id": "2b17b57d-fff4-4645-b539-91f305c26x53",
                "Status": "queued",
                "InputProductReference": {
                    "Reference": "S2B_MSIL1C_20191025T085939_N0208_R007_T37VCC_20191025T112031.zip",
                    "ContentDate": {
                        "Start": "2019-10-25T08:59:39.922Z",
                        "End": "2019-10-25T11:20:31.922Z",
                    },
                },
                "WorkflowId": "6c18b57d-fgk4-1236-b539-12h305c26z89",
                "WorkflowName": "S2_L1C_L2A",
                "WorkflowOptions": [
                    {
                        "Aerosol_Type": "RURAL",
                        "Mid_Latitude": "SUMMER",
                        "Ozone_Content": 0,
                        "Cirrus_Correction": True,
                        "DEM": True,
                        "DEM_directory": None,
                        "Resolution": 10,
                    }
                ],
            }
        ],
    }


@app.get("/TransformationOrders('{id}')", name="transformation_order")
async def transformation_order(request: Request, id: str):
    base = request.url_for("transformation_order", id=id)
    return {
        "@odata.id": f"{base}/TransformationOrders('{id}')",
        "@odata.context": f"{base}/$metadata",
        # "@odata.nextLink": "https://services.odata.org/V4/TripPinService/People?%24select=FirstName&%24skiptoken=8",
        "Id": id,
        "Status": "queued",
        "InputProductReference": {
            "Reference": "S2B_MSIL1C_20191025T085939_N0208_R007_T37VCC_20191025T112031.zip",
            "ContentDate": {
                "Start": "2019-10-25T08:59:39.922Z",
                "End": "2019-10-25T11:20:31.922Z",
            },
        },
        "WorkflowId": "6c18b57d-fgk4-1236-b539-12h305c26z89",
        "WorkflowName": "S2_L1C_L2A",
        "WorkflowOptions": [
            {
                "Aerosol_Type": "RURAL",
                "Mid_Latitude": "SUMMER",
                "Ozone_Content": 0,
                "Cirrus_Correction": True,
                "DEM": True,
                "DEM_directory": None,
                "Resolution": 10,
            }
        ],
    }


@app.post("/TransformationOrders", status_code=201)
async def transformation_order_create(request: Request, response: Response):
    id = "2b17b57d-fff4-4645-b539-91f305c26x53"
    url = request.url_for("transformation_order", id=id)
    response.headers["Location"] = url
    return await transformation_order(request=request, id=id)


# ******* METADATA DEFINITIONS *******
