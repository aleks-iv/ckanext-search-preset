from ckan.logic.schema import validator_args


@validator_args
def preset_payload(not_missing, boolean_validator, unicode_safe):
    return {
        "id": [not_missing, unicode_safe],
        "exclude_self": [boolean_validator],
    }


@validator_args
def preset_list(
    not_missing,
    unicode_safe,
    default,
    int_validator,
    convert_to_json_if_string,
):
    return {
        "id": [not_missing, unicode_safe],
        "extra_fq": [default(""), unicode_safe],
        "rows": [default(10), int_validator],
        "search_patch": [default("{}"), convert_to_json_if_string],
    }


@validator_args
def preset_count(not_missing, unicode_safe, default):
    return {
        "id": [not_missing, unicode_safe],
        "extra_fq": [default(""), unicode_safe],
    }
