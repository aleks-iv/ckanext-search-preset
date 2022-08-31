import ckan.plugins.toolkit as tk

from ckanext.toolbelt.utils import config_getter

CONFIG_DEFAULT_TYPE = "ckanext.search_preset.default_type"
DEFAULT_DEFAULT_TYPE = None
default_type = config_getter(CONFIG_DEFAULT_TYPE, DEFAULT_DEFAULT_TYPE)

CONFIG_TYPES = "ckanext.search_preset.package_types"
DEFAULT_TYPES = []
types = config_getter(CONFIG_TYPES, DEFAULT_TYPES, tk.aslist)

CONFIG_GROUP_FIELD = "ckanext.search_preset.group_by_field"
DEFAULT_GROUP_FIELD = None
group_field = config_getter(CONFIG_GROUP_FIELD, DEFAULT_GROUP_FIELD)

CONFIG_PREFIX = "ckanext.search_preset.field_prefix"
DEFAULT_PREFIX = "search_preset_field_"
prefix = config_getter(CONFIG_PREFIX, DEFAULT_PREFIX)

CONFIG_ALLOWED = "ckanext.search_preset.allowed_facets"
DEFAULT_ALLOWED = []
allowed = config_getter(CONFIG_ALLOWED, DEFAULT_ALLOWED, tk.aslist)

CONFIG_EXTRAS_FIELD = "ckanext.search_preset.extras_field"
DEFAULT_EXTRAS_FIELD = None
extras_field = config_getter(CONFIG_EXTRAS_FIELD, DEFAULT_EXTRAS_FIELD)

CONFIG_ALLOWED_EXTRAS = "ckanext.search_preset.allowed_extras"
DEFAULT_ALLOWED_EXTRAS = []
allowed_extras = config_getter(
    CONFIG_ALLOWED_EXTRAS, DEFAULT_ALLOWED_EXTRAS, tk.aslist
)
