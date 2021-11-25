import pytest

import esa_tf_restapi


WORKFLOW_OPTIONS = [
    {
        "Name": "Aerosol_Type",
        "Description": "Default processing via configuration is the rural (continental) aerosol type with mid latitude summer and an ozone concentration of 331 Dobson Units",
        "Type": "string",
        "Default": "RURAL",
        "Enum": ["MARITIME", "RURAL"],
    },
    {
        "Name": "Mid_Latitude",
        "Description": "If  'AUTO' the atmosphere profile will be determined automatically by the processor, selecting WINTER or SUMMER atmosphere profile based on the acquisition date and geographic location of the tile",
        "Type": "string",
        "Default": "SUMMER",
        "Enum": ["SUMMER", "WINTER", "AUTO"],
    },
    {
        "Name": "Ozone_Content",
        "Description": "0: to get the best approximation from metadata (this is the smallest difference between metadata and column DU), else select for midlatitude summer (MS) atmosphere: 250, 290, 331 (standard MS), 370, 410, 450; for midlatitude winter (MW) atmosphere: 250, 290, 330, 377 (standard MW), 420, 460",
        "Type": "integer",
        "Default": 331,
        "Enum": [0, 250, 290, 330, 331, 370, 377, 410, 420, 450, 460],
    },
    {
        "Name": "Cirrus_Correction",
        "Description": "FALSE: no cirrus correction applied, TRUE: cirrus correction applied",
        "Type": "boolean",
        "Default": False,
    },
    {
        "Name": "DEM_Terrain_Correction",
        "Description": "Use DEM for Terrain Correction, otherwise only used for WVP and AOT",
        "Type": "boolean",
        "Default": True,
    },
    {
        "Name": "Resolution",
        "Description": "Target resolution, can be 10, 20 or 60m. If omitted, 10, 20 and 60m resolutions will be processed",
        "Type": "integer",
        "Enum": [10, 20, 60],
    },
]


@pytest.mark.parametrize(
    "product_type,input_product_reference_name",
    [
        ("S2MSI1C", "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"),
        ("S2MSI2A", "S2B_MSIL2A_20211123T094019_N0301_R007_T18CVQ_20211123T123849.zip"),
        (
            "IW_SLC__1S",
            "S1B_IW_SLC__1SDV_20211125T040332_20211125T040401_029739_038CB1_1A18.zip",
        ),
    ],
)
def test_check_products_consistency(product_type, input_product_reference_name):

    esa_tf_restapi.api.check_products_consistency(
        product_type, input_product_reference_name, workflow_id="sen2cor_l1c_l2a"
    )


@pytest.mark.parametrize(
    "product_type,input_product_reference_name",
    [
        ("S2MSI1C", "S2B_MSIL2A_20211123T094019_N0301_R007_T18CVQ_20211123T123849.zip"),
        ("S2MSI2A", "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"),
        (
            "IW_SLC__1S",
            "S1B_EW_SLC__1SDV_20211125T040332_20211125T040401_029739_038CB1_1A18.zip",
        ),
    ],
)
def test_check_products_consistency_wrong_product(
    product_type, input_product_reference_name
):

    with pytest.raises(ValueError, match=r"input product reference"):
        esa_tf_restapi.api.check_products_consistency(
            product_type, input_product_reference_name, workflow_id="sen2cor_l1c_l2a"
        )


def test_check_products_consistency_wrong_product_type():
    workflow_id = "sen2cor_l1c_l2a"
    product_type = "S2LSI1C"
    input_product_reference_name = (
        "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"
    )
    with pytest.raises(
        ValueError, match=f"workflow {workflow_id} product type not recognized"
    ):
        esa_tf_restapi.api.check_products_consistency(
            product_type, input_product_reference_name, workflow_id=workflow_id
        )


def test_extract_worflow_options_defaults():

    res = esa_tf_restapi.api.extract_workflow_defaults(WORKFLOW_OPTIONS)

    assert "Aerosol_Type" in res
    assert res["Aerosol_Type"] == "RURAL"

    assert "Mid_Latitude" in res
    assert res["Mid_Latitude"] == "SUMMER"

    assert "Ozone_Content" in res
    assert res["Ozone_Content"] == 331

    assert "Cirrus_Correction" in res
    assert not res["Cirrus_Correction"]

    assert "DEM_Terrain_Correction" in res
    assert res["DEM_Terrain_Correction"]


def test_fill_with_defaults():

    workflow_options = {
        "Mid_Latitude": "WINTER",
        "DEM_Terrain_Correction": False
    }

    res = esa_tf_restapi.api.fill_with_defaults(
        workflow_options,
        WORKFLOW_OPTIONS
    )

    assert "Aerosol_Type" in res
    assert res["Aerosol_Type"] == "RURAL"

    assert "Mid_Latitude" in res
    assert res["Mid_Latitude"] == "WINTER"

    assert "Ozone_Content" in res
    assert res["Ozone_Content"] == 331

    assert "Cirrus_Correction" in res
    assert not res["Cirrus_Correction"]

    assert "DEM_Terrain_Correction" in res
    assert not res["DEM_Terrain_Correction"]

