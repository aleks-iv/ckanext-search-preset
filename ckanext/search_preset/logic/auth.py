from __future__ import annotations

import ckan.authz as authz
import ckan.plugins.toolkit as tk

from ckanext.toolbelt.decorators import Collector

auth, get_auth_functions = Collector("search_preset").split()


@auth
def preset_create(context, data_dict):
    return {"success": False}


@auth
@tk.auth_allow_anonymous_access
def preset_payload(context, data_dict):
    return authz.is_authorized("package_show", context, data_dict)


@auth
@tk.auth_allow_anonymous_access
def preset_list(context, data_dict):
    return authz.is_authorized(
        "search_preset_preset_payload", context, data_dict
    )


@auth
@tk.auth_allow_anonymous_access
def preset_count(context, data_dict):
    return authz.is_authorized("search_preset_preset_list", context, data_dict)
