from __future__ import annotations

from ckanext.toolbelt.decorators import Collector

auth, get_auth_functions = Collector("search_preset").split()


@auth
def preset_create(context, data_dict):
    return {"success": False}
