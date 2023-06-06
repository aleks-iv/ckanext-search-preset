import base64
from typing import Any

from ckanext.toolbelt.decorators import Collector

validator, get_validators = Collector("search_preset").split()

from .. import config


@validator
def decode_from_base64(value: str) -> Any:
    if not config.convert_to_base64:
        return value
    try:
        return base64.b64decode(value).decode('utf-8')
    except base64.binascii.Error:
        return value
