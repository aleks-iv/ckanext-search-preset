from __future__ import annotations
import json
import logging
from typing import Any, Optional

import ckan.plugins.toolkit as tk
from ckan.lib.search.query import solr_literal
from ckanext.toolbelt.decorators import Collector

log = logging.getLogger(__name__)
helper, get_helpers = Collector("search_preset").split()

CONFIG_DEFAULT_TYPE = "ckanext.search_preset.default_type"
DEFAULT_DEFAULT_TYPE = "preset"

CONFIG_TYPES = "ckanext.search_preset.package_types"
DEFAULT_TYPES = []

CONFIG_GROUP_FIELD = "ckanext.search_preset.group_by_field"
DEFAULT_GROUP_FIELD = None

CONFIG_TTL = "ckanext.search_preset.stats_ttl"
DEFAULT_TTL = 0

CONFIG_PREFIX = "ckanext.search_preset.field_prefix"
DEFAULT_PREFIX = "search_preset_field_"

CONFIG_ALLOWED = "ckanext.search_preset.allowed_facets"
DEFAULT_ALLOWED = []


@helper
def default_preset_type() -> str:
    """Return the default package type of search preset.

    This value can be used to decide which preset to use on standard snippets
    whenever multiple preset types available.

    """
    return tk.config.get(CONFIG_DEFAULT_TYPE, DEFAULT_DEFAULT_TYPE)


@helper
def preset_types() -> set[str]:
    """Return all the possible package types of the search preset."""
    default: str = tk.h.search_preset_default_preset_type()
    types: set[str] = set(
        tk.aslist(tk.config.get(CONFIG_TYPES, DEFAULT_TYPES))
    )
    types.add(default)

    return types


@helper
def filter_field_prefix() -> str:
    """Prefix for the filter-fields of preset.

    Preset is just a normal dataset, so it also contains metadata.  Prefix is
    used for separating metadata-fields from filter-fields.

    """
    return tk.config.get(CONFIG_PREFIX, DEFAULT_PREFIX)


@helper
def group_by_field() -> Optional[str]:
    """Field used for combining packages on the preset page."""
    return tk.config.get(CONFIG_GROUP_FIELD, DEFAULT_GROUP_FIELD)


@helper
def accept_filters(filters: dict[str, list[str]]) -> bool:
    """Decide if search preset can be created.

    Can be redefined if more control over preset creation is required.
    """
    return bool(filters)

@helper
def prepare_filters(filters: dict[str, list[str]]) -> dict[str, list[str]]:
    """Prepare active facets before assigning them to the preset fields."""
    if not tk.h.search_preset_accept_filters(filters):
        return {}

    prefix = tk.h.search_preset_filter_field_prefix()
    allowed_fields = set(
        tk.aslist(tk.config.get(CONFIG_ALLOWED, DEFAULT_ALLOWED))
    )
    allow_everything = not allowed_fields

    return {
        prefix + k: json.dumps(v)
        for k, v in filters.items()
        if allow_everything or k in allowed_fields
    }


@helper
def count_preset(id_: str, extra_fq: str = "") -> int:
    """Count the number of packages included into preset."""
    result = tk.h.search_preset_list_preset(id_, extra_fq, 0)
    return result["count"]


@helper
def list_preset(
    id_: str,
    extra_fq: str = "",
    limit: int = 1000,
    extra_search: dict[str, Any] = {},
) -> dict[str, Any]:
    """Return the search result with all the packages included into preset."""
    fq = tk.h.search_preset_fq_from_preset(id_, exclude_preset=True) + extra_fq

    data_dict = {"fq": fq, "rows": limit}
    data_dict.update(extra_search)
    result = tk.get_action("package_search")({}, data_dict)
    return result


@helper
def fq_from_preset(id_: str, exclude_preset: bool = False) -> str:
    """Extract fq produced by preset.

    Essentially, get all the active facets that were used when the preset was created.
    """
    pkg = tk.get_action("package_show")({}, {"id": id_})
    prefix = tk.h.search_preset_filter_field_prefix()

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
                id_,
                k,
                v,
            )
            continue

        if not isinstance(values, list):
            log.warning(
                "Search preset %s supports only list value inside the"
                " filter-filed %s: %s",
                id_,
                k,
                values,
            )
            continue

        if not values:
            continue

        field = k[len(prefix) :]
        joined = " OR ".join(map(solr_literal, values))
        fq += f" +{field}:({joined})"

    if exclude_preset:
        type_ = pkg["type"]
        fq += f" -type:{type_} "

    return fq
