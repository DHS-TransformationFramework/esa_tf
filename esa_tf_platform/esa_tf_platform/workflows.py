import itertools
import warnings

import pkg_resources


def remove_duplicates(pkg_entrypoints):
    # sort and group entrypoints by name
    pkg_entrypoints = sorted(pkg_entrypoints, key=lambda ep: ep.name)
    pkg_entrypoints_grouped = itertools.groupby(pkg_entrypoints, key=lambda ep: ep.name)
    # check if there are multiple entrypoints for the same name
    unique_pkg_entrypoints = []
    for name, matches in pkg_entrypoints_grouped:
        matches = list(matches)
        unique_pkg_entrypoints.append(matches[0])
        matches_len = len(matches)
        if matches_len > 1:
            selected_module_name = matches[0].module_name
            all_module_names = [e.module_name for e in matches]
            warnings.warn(
                f"Found {matches_len} entrypoints for the workflow name {name}:"
                f"\n {all_module_names}.\n It will be used: {selected_module_name}.",
                RuntimeWarning,
            )
    return unique_pkg_entrypoints


def workflow_dict_from_pkg(pkg_entrypoints):
    workflow_entrypoints = {}
    for pkg_ep in pkg_entrypoints:
        name = pkg_ep.name
        try:
            workflow_config = pkg_ep.load()
            workflow_entrypoints[name] = workflow_config
        except Exception as ex:
            warnings.warn(f"Workflow {name!r} loading failed:\n{ex}", RuntimeWarning)
    return workflow_entrypoints


def load_workflows_configurations(pkg_entrypoints):
    pkg_entrypoints = remove_duplicates(pkg_entrypoints)
    workflow_entrypoints = workflow_dict_from_pkg(pkg_entrypoints)
    return {name: {**workflows, "Id": name} for name, workflows in workflow_entrypoints.items()}


def filter_by_product_type(workflows, product_type=None):
    filtered_workflows = {}
    for name in workflows:
        if product_type == workflows[name]["InputProductType"]:
            filtered_workflows[name] = workflows[name]
    return filtered_workflows


def get_workflows(product_type=None):
    pkg_entrypoints = pkg_resources.iter_entry_points("esa_tf.plugin")
    workflows = load_workflows_configurations(pkg_entrypoints)
    if product_type:
        workflows = filter_by_product_type(workflows, product_type)
    return workflows


def get_workflow_by_id(workflow_id=None):
    workflows = get_workflows()
    try:
        workflow = workflows[workflow_id]
    except KeyError:
        raise ValueError(
            f"Workflow {workflow_id} not found, available workflows are {list(workflows.keys())}"
        )
    return workflow

