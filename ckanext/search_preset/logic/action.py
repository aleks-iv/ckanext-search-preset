from __future__ import annotations

import json
import logging

import ckan.plugins.toolkit as tk
from ckan.lib.search.query import solr_literal
from ckan.logic import validate

from ckanext.toolbelt.decorators import Collector

from .. import config
from . import schema

log = logging.getLogger(__name__)
action, get_actions = Collector("search_preset").split()


@action
@tk.side_effect_free
@validate(schema.preset_payload)
def preset_payload(context, data_dict):
    """Generate search dict produced by preset.

    Includes `fq` and `extras` keys. If `exclude_self` set to `True`, the
    preset itself will be exclueded from the search, even if it satisfies all
    the filters.

    Args:
        id(str): ID of the search preset(package)
        exclude_self(bool, optional): exclude self from the search payload

    Returns:
        dictionary with `fq` and `extras` keys
    """
    tk.check_access("search_preset_preset_payload", context, data_dict)
    pkg = tk.get_action("package_show")({}, {"id": data_dict["id"]})
    prefix: str = config.prefix()
    ef: str = config.extras_field()
    fq = ""

    for k, v in pkg.items():
        if not k.startswith(prefix) or not v:
            continue

        try:
            values = json.loads(v)
        except ValueError:
            log.warning(
                "Search preset %s contains non-JSON value inside the"
                " filter-filed %s: %s",
                pkg["id"],
                k,
                v,
            )
            continue

        if not isinstance(values, list):
            log.warning(
                "Search preset %s supports only list value inside the"
                " filter-filed %s: %s",
                pkg["id"],
                k,
                values,
            )
            continue

        if not values:
            continue

        field = k[len(prefix) :]
        # joined = " OR ".join(map(solr_literal, values))
        # fq += f" +{field}:({joined})"
        fq += " " + " ".join(
            f"{field}:{solr_literal(value)}" for value in values
        )

    if data_dict["exclude_self"]:
        fq += " -id:{}".format(pkg["id"])

    try:
        extras = json.loads(pkg.get(ef) or "{}")
    except ValueError:
        log.warning(
            "Search preset %s contains non-JSON value inside the"
            " extras-filed %s: %s",
            pkg["id"],
            ef,
            pkg[ef],
        )
        extras = {}

    return {"fq": fq.strip(), "extras": extras}


@action
@tk.side_effect_free
@validate(schema.preset_list)
def preset_list(context, data_dict):
    """Generate search dict produced by preset.

    Includes `fq` and `extras` keys. If `exclude_self` set to `True`, the
    preset itself will be exclueded from the search, even if it satisfies all
    the filters.

    Args:
        id(str): ID of the search preset(package)
        extra_fq(str, optional): exclude self from the search payload
        search_patch(dict[str, Any], optional): exclude self from the search payload
        rows(int, optional): exclude self from the search payload

    Returns:
        Result of the search by preset
    """
    payload = tk.get_action("search_preset_preset_payload")(
        context, {"id": data_dict["id"], "exclude_self": True}
    )

    payload["fq"] += " " + data_dict["extra_fq"]
    payload["rows"] = data_dict["rows"]

    payload.update(data_dict["search_patch"])
    result = tk.get_action("package_search")(context, payload)
    return result


@action
@tk.side_effect_free
@validate(schema.preset_count)
def preset_count(context, data_dict):
    """Generate search dict produced by preset.

    Includes `fq` and `extras` keys. If `exclude_self` set to `True`, the
    preset itself will be exclueded from the search, even if it satisfies all
    the filters.

    Args:
        id(str): ID of the search preset(package)
        extra_fq(str, optional): exclude self from the search payload

    Returns:
        Number of packages found by preset
    """
    result = tk.get_action("search_preset_preset_list")(
        context, dict(data_dict, rows=0)
    )
    return result["count"]
