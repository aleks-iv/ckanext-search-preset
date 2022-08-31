from __future__ import annotations

import ckan.plugins as plugins
import ckan.plugins.toolkit as tk
from ckan.lib.plugins import DefaultDatasetForm

from ..config import DEFAULT_PREFIX


class SearchPresetTestPlugin(plugins.SingletonPlugin, DefaultDatasetForm):
    plugins.implements(plugins.IDatasetForm)

    def is_fallback(self):
        return False

    def package_types(self):
        return ["test_preset"]

    def create_package_schema(self):
        schema = super().create_package_schema()
        ignore_missing = tk.get_validator("ignore_missing")
        convert = tk.get_validator("convert_to_extras")
        schema[DEFAULT_PREFIX + "author"] = [ignore_missing, convert]
        schema[DEFAULT_PREFIX + "version"] = [ignore_missing, convert]
        schema[DEFAULT_PREFIX + "license_id"] = [ignore_missing, convert]
        return schema

    def show_package_schema(self):
        schema = super().show_package_schema()
        ignore_missing = tk.get_validator("ignore_missing")
        convert = tk.get_validator("convert_from_extras")
        schema[DEFAULT_PREFIX + "author"] = [convert, ignore_missing]
        schema[DEFAULT_PREFIX + "version"] = [convert, ignore_missing]
        schema[DEFAULT_PREFIX + "license_id"] = [convert, ignore_missing]
        return schema
