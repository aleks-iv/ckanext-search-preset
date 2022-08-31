from __future__ import annotations

import json
import logging
from typing import Any, Optional

import ckan.plugins.toolkit as tk

from ckanext.toolbelt.decorators import Collector

from . import config

log = logging.getLogger(__name__)
helper, get_helpers = Collector("search_preset").split()


@helper
def default_preset_type() -> Optional[str]:
    """Return the default package type of search preset.

    This value can be used to decide which preset to use on standard snippets
    whenever multiple preset types available.

    """
    return config.default_type()


@helper
def preset_types() -> set[str]:
    """Return all the possible package types of the search preset."""
    types: set[str] = set(config.types())

    default: Optional[str] = config.default_type()
    if default:
        types.add(default)

    return types


@helper
def filter_field_prefix() -> str:
    """Prefix for the filter-fields of preset.

    Preset is just a normal dataset, so it also contains metadata.  Prefix is
    used for separating metadata-fields from filter-fields.

    """
    return config.prefix()


@helper
def extras_field() -> Optional[str]:
    """Field that holds search extras."""
    return config.extras_field()


#
@helper
def group_by_field() -> Optional[str]:
    """Field used for combining packages on the preset page."""
    return config.group_field()


@helper
def accept_filters(filters: dict[str, list[str]]) -> bool:
    """Decide whether search filters should be processed(or completely ignored).

    Can be redefined if more control over preset creation is required.
    """
    return True


#
@helper
def prepare_filters(filters: dict[str, list[str]]) -> dict[str, str]:
    """Prepare active facets before assigning them to the preset fields."""
    if not tk.h.search_preset_accept_filters(filters):
        return {}

    prefix = tk.h.search_preset_filter_field_prefix()
    allowed_fields = set(config.allowed())
    allow_everything = not allowed_fields

    prepared = {
        prefix + k: json.dumps(v)
        for k, v in filters.items()
        if allow_everything or k in allowed_fields
    }

    ef: str = tk.h.search_preset_extras_field()

    if ef:
        allowed_extras = set(config.allowed_extras())
        allow_all_extras = not allowed_extras
        prepared[ef] = json.dumps(
            {
                k: v
                for k, v in tk.request.params.to_dict(flat=True).items()
                if k.startswith("ext_")
                and (allow_all_extras or k in allowed_extras)
            }
        )

    return prepared


#
@helper
def count_preset(id_: str, extra_fq: str = "") -> int:
    """Count the number of packages included into preset."""
    return tk.get_action("search_preset_preset_count")(
        {}, {"id": id_, "extra_fq": extra_fq}
    )


#
@helper
def list_preset(
    id_: str,
    extra_fq: str = "",
    limit: int = 1000,
    extra_search: dict[str, Any] = {},
) -> dict[str, Any]:
    """Return the search result with all the packages included into preset."""
    return tk.get_action("search_preset_preset_list")(
        {},
        {
            "id": id_,
            "extra_fq": extra_fq,
            "rows": limit,
            "search_patch": extra_search,
        },
    )


@helper
def payload_from_preset(
    id_: str, exclude_self: bool = False
) -> dict[str, Any]:
    """Extract search parameters produced by preset.

    Essentially, get all the active facets that were used when the preset was created.
    """
    return tk.get_action("search_preset_preset_payload")(
        {}, {"id": id_, "exclude_self": exclude_self}
    )
