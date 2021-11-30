import os
from pathlib import Path
from xml.etree import ElementTree

import pytest

from esa_tf_platform import esa_tf_plugin_sen2cor


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

    options = {"DEM_Terrain_Correction": True}
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


def test_check_ozone_content_valid_summer():
    options = {
        "Mid_Latitude": "SUMMER",
        "Ozone_Content": 370,
        "Cirrus_Correction": True,
    }
    assert esa_tf_plugin_sen2cor.check_ozone_content(options) is True


def test_check_ozone_content_invalid_summer():
    options = {
        "Mid_Latitude": "SUMMER",
        "Ozone_Content": 377,
        "Cirrus_Correction": True,
    }
    with pytest.raises(ValueError):
        esa_tf_plugin_sen2cor.check_ozone_content(options)


def test_check_ozone_content_valid_winter():
    options = {
        "Mid_Latitude": "WINTER",
        "Ozone_Content": 420,
        "Cirrus_Correction": True,
    }
    assert esa_tf_plugin_sen2cor.check_ozone_content(options) is True


def test_check_ozone_content_invalid_winter():
    options = {
        "Mid_Latitude": "WINTER",
        "Ozone_Content": 410,
        "Cirrus_Correction": True,
    }
    with pytest.raises(ValueError):
        esa_tf_plugin_sen2cor.check_ozone_content(options)


def test_check_ozone_content_valid_auto():
    options = {
        "Mid_Latitude": "AUTO",
        "Ozone_Content": 290,
        "Cirrus_Correction": True,
    }
    assert esa_tf_plugin_sen2cor.check_ozone_content(options) is True


def test_check_ozone_content_invalid_auto():
    options = {
        "Mid_Latitude": "AUTO",
        "Ozone_Content": 1000,
        "Cirrus_Correction": True,
    }
    with pytest.raises(ValueError):
        esa_tf_plugin_sen2cor.check_ozone_content(options)


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
        "Cirrus_Correction": True,
    }
    with pytest.raises(ValueError):
        assert esa_tf_plugin_sen2cor.check_roi_options(options)


def test_check_roi_options_int_invalid_win():
    options = {
        "row0": 600,
        "col0": 1200,
        "nrow_win": 650,  # wrong value, it is not divisible by 6
        "ncol_win": 600,
        "Cirrus_Correction": True,
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
        "Aerosol_Type": "MARITIME",
        "Mid_Latitude": "DUMMY",
        "Ozone_Content": 0,
        "Cirrus_Correction": True,
        "row0": 600,
        "col0": 1200,
        "nrow_win": 600,
        "ncol_win": 600,
    }
    with pytest.raises(ValueError):
        assert esa_tf_plugin_sen2cor.check_options(options)


def test_check_options_invalid_with_roi2():
    options = {
        "Aerosol_Type": "MARITIME",
        "Mid_Latitude": "DUMMY",
        "Ozone_Content": 0,
        "Cirrus_Correction": True,
        "row0": "OFF",
        "col0": 1200,
        "nrow_win": 600,
        "ncol_win": 600,
    }
    with pytest.raises(ValueError):
        assert esa_tf_plugin_sen2cor.check_options(options)


def test_print_options():
    workflow_options = {}
    assert esa_tf_plugin_sen2cor.print_options(workflow_options)

    workflow_options = {"Ozone_Content": 9999}
    assert (
        esa_tf_plugin_sen2cor.print_options(workflow_options)["Ozone_Content"]
        == workflow_options["Ozone_Content"]
    )

    workflow_options = {"Ozone_Content": 9999, "dummy": -9999}
    assert (
        esa_tf_plugin_sen2cor.print_options(workflow_options)["Ozone_Content"]
        == workflow_options["Ozone_Content"]
    )
    assert (
        esa_tf_plugin_sen2cor.print_options(workflow_options)["dummy"]
        == workflow_options["dummy"]
    )


def test_find_output_error(tmpdir):
    output_dir = tmpdir.join("output_dir").strpath
    os.mkdir(output_dir)

    dummy_file_path = os.path.join(output_dir, ".dummy_file.txt")
    Path(dummy_file_path).touch()
    with pytest.raises(RuntimeError):
        assert esa_tf_plugin_sen2cor.find_output(output_dir)


def test_find_output(tmpdir):
    output_dir = tmpdir.join("output_dir").strpath
    os.mkdir(output_dir)
    sen2cor_output_dir = os.path.join(
        output_dir, "S2A_MSIL2A_20211117T093251_N9999_R136_T33NTF_20211124T093440.SAFE"
    )
    os.mkdir(sen2cor_output_dir)
    dummy_file_path = os.path.join(output_dir, ".dummy_file.txt")
    Path(dummy_file_path).touch()
    output_path = esa_tf_plugin_sen2cor.find_output(output_dir)

    assert output_path == sen2cor_output_dir
