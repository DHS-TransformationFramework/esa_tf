import os
from xml.etree import ElementTree

import pytest

from esa_tf_platform import esa_tf_plugin_sen2cor

VALID_OZONE_VALUES = [0, 250, 290, 330, 331, 370, 377, 410, 420, 450, 460]


def test_set_sen2cor_options():
    sample_config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "esa_tf_platform",
        "resources",
        "L2A_GIPP.xml",
    )
    et = ElementTree.parse(sample_config_path)

    # check values before setting the user options
    assert et.findall(".//Aerosol_Type")[0].text == "RURAL"
    assert et.findall(".//Mid_Latitude")[0].text == "SUMMER"
    assert et.findall(".//Ozone_Content")[0].text == "331"
    assert et.findall(".//Cirrus_Correction")[0].text == "FALSE"

    options = {
        "Aerosol_Type": "MARITIME",
        "Mid_Latitude": "AUTO",
        "Ozone_Content": 0,
        "Cirrus_Correction": True,
    }
    srtm_dir = ""
    et_with_options = esa_tf_plugin_sen2cor.set_sen2cor_options(et, options, srtm_dir)

    # check values after setting the user options
    assert et_with_options.findall(".//Aerosol_Type")[0].text == "MARITIME"
    assert et_with_options.findall(".//Mid_Latitude")[0].text == "AUTO"
    assert et_with_options.findall(".//Ozone_Content")[0].text == "0"
    assert et_with_options.findall(".//Cirrus_Correction")[0].text == "TRUE"


def test_set_sen2cor_options_dem():
    sample_config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "esa_tf_platform",
        "resources",
        "L2A_GIPP.xml",
    )
    et = ElementTree.parse(sample_config_path)

    # check values before setting the user options
    assert et.findall(".//DEM_Directory")[0].text == "NONE"
    assert et.findall(".//DEM_Reference")[0].text == "NONE"

    options = {"dem_terrain_correction": True}
    srtm_path = "/dummy_folder/very_dummy_folder"
    et_with_options = esa_tf_plugin_sen2cor.set_sen2cor_options(et, options, srtm_path)

    # check values after setting the user options
    assert et_with_options.findall(".//DEM_Directory")[0].text == srtm_path
    assert (
        et_with_options.findall(".//DEM_Reference")[0].text
        == esa_tf_plugin_sen2cor.SRTM_DOWNLOAD_ADDRESS
    )


def test_create_sen2cor_confile_no_option(tmpdir):
    processing_dir = tmpdir.join("processing_dir").strpath
    os.mkdir(processing_dir)
    srtm_path = tmpdir.join("dem").strpath
    os.mkdir(srtm_path)
    options = {}
    output_confile = esa_tf_plugin_sen2cor.create_sen2cor_confile(
        processing_dir, srtm_path, options
    )

    assert os.path.isfile(output_confile)

    et = ElementTree.parse(output_confile)
    # check default values
    assert et.findall(".//Aerosol_Type")[0].text == "RURAL"
    assert et.findall(".//Mid_Latitude")[0].text == "SUMMER"
    assert et.findall(".//Ozone_Content")[0].text == "331"
    assert et.findall(".//Cirrus_Correction")[0].text == "FALSE"


def test_create_sen2cor_confile_with_options(tmpdir):
    processing_dir = tmpdir.join("processing_dir").strpath
    os.mkdir(processing_dir)
    srtm_path = tmpdir.join("dem").strpath
    os.mkdir(srtm_path)
    options = {
        "Aerosol_Type": "MARITIME",
        "Mid_Latitude": "AUTO",
        "Ozone_Content": 0,
        "Cirrus_Correction": True,
    }
    output_confile = esa_tf_plugin_sen2cor.create_sen2cor_confile(
        processing_dir, srtm_path, options
    )

    assert os.path.isfile(output_confile)

    et = ElementTree.parse(output_confile)
    # check values after setting the user options
    assert et.findall(".//Aerosol_Type")[0].text == "MARITIME"
    assert et.findall(".//Mid_Latitude")[0].text == "AUTO"
    assert et.findall(".//Ozone_Content")[0].text == "0"
    assert et.findall(".//Cirrus_Correction")[0].text == "TRUE"


def test_find_option_definition():
    options_names = [
        "Aerosol_Type",
        "Mid_Latitude",
        "Ozone_Content",
        "Cirrus_Correction",
        "DEM_Terrain_Correction",
        "Resolution",
    ]
    for oname in options_names:
        odef = esa_tf_plugin_sen2cor.find_option_definition(oname)
        assert type(odef) is dict
        assert odef["Name"] == oname


def test_check_ozone_content_valid_summer():
    options = {
        "mid_latitude": "SUMMER",
        "ozone_content": 370,
        "cirrus_correction": True,
    }
    assert (
        esa_tf_plugin_sen2cor.check_ozone_content(options, VALID_OZONE_VALUES) is True
    )


def test_check_ozone_content_invalid_summer():
    options = {
        "mid_latitude": "SUMMER",
        "ozone_content": 377,
        "cirrus_correction": True,
    }
    with pytest.raises(ValueError):
        esa_tf_plugin_sen2cor.check_ozone_content(options, VALID_OZONE_VALUES)


def test_check_ozone_content_valid_winter():
    options = {
        "mid_latitude": "WINTER",
        "ozone_content": 420,
        "cirrus_correction": True,
    }
    assert (
        esa_tf_plugin_sen2cor.check_ozone_content(options, VALID_OZONE_VALUES) is True
    )


def test_check_ozone_content_invalid_winter():
    options = {
        "mid_latitude": "WINTER",
        "ozone_content": 410,
        "cirrus_correction": True,
    }
    with pytest.raises(ValueError):
        esa_tf_plugin_sen2cor.check_ozone_content(options, VALID_OZONE_VALUES)


def test_check_ozone_content_valid_auto():
    options = {
        "mid_latitude": "AUTO",
        "ozone_content": 290,
        "cirrus_correction": True,
    }
    assert (
        esa_tf_plugin_sen2cor.check_ozone_content(options, VALID_OZONE_VALUES) is True
    )


def test_check_ozone_content_invalid_auto():
    options = {
        "mid_latitude": "AUTO",
        "ozone_content": 1000,
        "cirrus_correction": True,
    }
    with pytest.raises(ValueError):
        esa_tf_plugin_sen2cor.check_ozone_content(options, VALID_OZONE_VALUES)


def test_check_roi_options_missing_valid():
    roi_options = {}
    assert esa_tf_plugin_sen2cor.check_roi_options(roi_options) is True


def test_check_roi_options_missing_invalid():
    roi_options = {
        "row0": "OFF",
        "col0": "OFF",
        "cirrus_correction": True,
    }
    with pytest.raises(ValueError):
        esa_tf_plugin_sen2cor.check_roi_options(roi_options)


def test_check_roi_options_rowcol_different_type():
    roi_options = {
        "row0": "OFF",
        "col0": 60,
        "nrow_win": 600,
        "ncol_win": 600,
    }
    with pytest.raises(ValueError):
        esa_tf_plugin_sen2cor.check_roi_options(roi_options)


def test_check_roi_options_rowcol_different_str_value():
    roi_options = {
        "row0": "OFF",
        "col0": "AUTO",
        "nrow_win": 600,
        "ncol_win": 600,
    }
    with pytest.raises(ValueError):
        esa_tf_plugin_sen2cor.check_roi_options(roi_options)


def test_check_roi_options_rowcol_same_str_value():
    roi_options = {
        "row0": "OFF",
        "col0": "OFF",
        "nrow_win": 600,
        "ncol_win": 600,
    }
    assert esa_tf_plugin_sen2cor.check_roi_options(roi_options) is True


def test_check_roi_options_rowcol_int_value():
    roi_options = {
        "row0": 600,
        "col0": 1200,
        "nrow_win": 600,
        "ncol_win": 600,
    }
    assert esa_tf_plugin_sen2cor.check_roi_options(roi_options) is True


def test_check_roi_options_int_invalid_centre():
    options = {
        "row0": 311,  # wrong value, it is not divisible by 6
        "col0": 1200,
        "nrow_win": 650,
        "ncol_win": 600,
        "cirrus_correction": True,
    }
    with pytest.raises(ValueError):
        assert esa_tf_plugin_sen2cor.check_roi_options(options)


def test_check_roi_options_int_invalid_win():
    options = {
        "row0": 600,
        "col0": 1200,
        "nrow_win": 650,  # wrong value, it is not divisible by 6
        "ncol_win": 600,
        "cirrus_correction": True,
    }
    with pytest.raises(ValueError):
        assert esa_tf_plugin_sen2cor.check_roi_options(options)


def test_check_options_no_option():
    options = {}
    assert esa_tf_plugin_sen2cor.check_options(options) is True


def test_check_options_valid_no_roi():
    options = {
        "Aerosol_Type": "MARITIME",
        "Mid_Latitude": "AUTO",
        "Ozone_Content": 0,
        "Cirrus_Correction": True,
    }
    assert esa_tf_plugin_sen2cor.check_options(options) is True


def test_check_options_invalid_no_roi():
    options = {
        "Aerosol_Type": "dummy",
        "Mid_Latitude": "AUTO",
        "Ozone_Content": 0,
        "Cirrus_Correction": True,
    }
    with pytest.raises(ValueError):
        assert esa_tf_plugin_sen2cor.check_options(options)


def test_check_options_invalid_with_roi():
    options = {
        "aerosol_type": "MARITIME",
        "mid_latitude": "DUMMY",
        "ozone_content": 0,
        "cirrus_correction": True,
        "row0": 600,
        "col0": 1200,
        "nrow_win": 600,
        "ncol_win": 600,
    }
    with pytest.raises(ValueError):
        assert esa_tf_plugin_sen2cor.check_options(options)


def test_check_options_invalid_with_roi2():
    options = {
        "aerosol_type": "MARITIME",
        "mid_latitude": "DUMMY",
        "ozone_content": 0,
        "cirrus_correction": True,
        "row0": "OFF",
        "col0": 1200,
        "nrow_win": 600,
        "ncol_win": 600,
    }
    with pytest.raises(ValueError):
        assert esa_tf_plugin_sen2cor.check_options(options)
