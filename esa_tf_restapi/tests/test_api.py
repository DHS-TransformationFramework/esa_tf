import pytest

import esa_tf_restapi


@pytest.mark.parametrize(
    "product_type,input_product_reference_name",
    [
        ("S2MSI1C", "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"),
        ("S2MSI2A", "S2B_MSIL2A_20211123T094019_N0301_R007_T18CVQ_20211123T123849.zip"),
        ("IW_SLC__1S", "S1B_IW_SLC__1SDV_20211125T040332_20211125T040401_029739_038CB1_1A18.zip"),
    ]
)
def test_pass_check_products_consistency(product_type, input_product_reference_name):

    esa_tf_restapi.api.check_products_consistency(
        product_type, input_product_reference_name, workflow_id=None
    )
