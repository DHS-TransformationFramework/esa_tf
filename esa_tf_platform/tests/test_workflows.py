from unittest import mock

import pkg_resources
import pytest

from esa_tf_platform import workflows


dummy_workflow_config1 = {"conf": "1"}
dummy_workflow_config2a = {"conf": "2a"}
dummy_workflow_config2b = {"conf": "2b"}
dummy_workflow_config3a = {"conf": "3a"}
dummy_workflow_config3b = {"conf": "3b"}


@pytest.fixture
def dummy_duplicated_entrypoints():
    specs = [
        "workflow1 = esa_tf_platform.tests.test_workflows:dummy_workflow_config1",
        "workflow2 = esa_tf_platform.tests.test_workflows:dummy_workflow_config2a",
        "workflow2 = esa_tf_platform.tests.test_workflows:dummy_workflow_config2b",
        "workflow3 = esa_tf_platform.tests.test_workflows:dummy_workflow_config3a",
        "workflow3 = esa_tf_platform.tests.test_workflows:dummy_workflow_config3b",
    ]
    eps = [pkg_resources.EntryPoint.parse(spec) for spec in specs]
    return eps


@pytest.mark.filterwarnings("ignore:Found")
def test_remove_duplicates(dummy_duplicated_entrypoints):
    with pytest.warns(RuntimeWarning):
        entrypoints = workflows.remove_duplicates(dummy_duplicated_entrypoints)
    assert len(entrypoints) == 3


def test_remove_duplicates_warnings(dummy_duplicated_entrypoints):

    with pytest.warns(RuntimeWarning) as record:
        _ = workflows.remove_duplicates(dummy_duplicated_entrypoints)

    assert len(record) == 2
    message0 = str(record[0].message)
    message1 = str(record[1].message)
    assert "entrypoints" in message0
    assert "entrypoints" in message1


@mock.patch("pkg_resources.EntryPoint.load", mock.MagicMock(return_value=None))
def test_workflows_dict_from_pkg():
    specs = [
        "workflow1 = esa_tf_platform.tests.test_workflows:dummy_workflow_config1",
        "workflow2 = esa_tf_platform.tests.test_workflows:dummy_workflow_config2a",
    ]
    entrypoints = [pkg_resources.EntryPoint.parse(spec) for spec in specs]
    wk = workflows.workflow_dict_from_pkg(entrypoints)
    assert len(wk) == 2
    assert wk.keys() == set(("workflow1", "workflow2"))
