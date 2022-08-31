import json

import pytest

from .. import config
from .. import helpers as h


class TestDefaultPreset:
    def test_default(self):
        assert h.default_preset_type() is None

    @pytest.mark.ckan_config(config.CONFIG_DEFAULT_TYPE, "xxx")
    def test_configured(self):
        assert h.default_preset_type() == "xxx"


class TestPresetTypes:
    def test_default(self):
        assert h.preset_types() == set()

    @pytest.mark.ckan_config(config.CONFIG_DEFAULT_TYPE, "xxx")
    def test_default_only(self):
        assert h.preset_types() == {"xxx"}

    @pytest.mark.ckan_config(config.CONFIG_DEFAULT_TYPE, "xxx")
    @pytest.mark.ckan_config(config.CONFIG_TYPES, "aaa bbb")
    def test_with_default(self):
        assert h.preset_types() == {"xxx", "aaa", "bbb"}


class TestAcceptFilters:
    def test_empty(self):
        assert h.accept_filters({})

    def test_non_empty(self):
        assert h.accept_filters({"a": ["b"]})


class TestPrepareFilters:
    def _filters(self):
        return {"a": ["b"], "x": ["y"]}

    def test_empty(self):
        assert h.prepare_filters({}) == {}

    def test_default_allows_all(self):
        assert h.prepare_filters(self._filters()) == {
            config.DEFAULT_PREFIX + "a": '["b"]',
            config.DEFAULT_PREFIX + "x": '["y"]',
        }

    @pytest.mark.ckan_config(config.CONFIG_ALLOWED, "x")
    def test_controlled_allow(self):
        assert h.prepare_filters(self._filters()) == {
            config.DEFAULT_PREFIX + "x": '["y"]'
        }

    def test_extras_ignored_with_no_field(self, test_request_context):
        with test_request_context("?ext_a=1&ext_b=2"):
            assert h.prepare_filters({}) == {}

    @pytest.mark.ckan_config(config.CONFIG_EXTRAS_FIELD, "exfield")
    def test_extras_with_field_allow_all(self, test_request_context):
        with test_request_context("?ext_a=1&ext_b=2"):
            assert h.prepare_filters({}) == {
                "exfield": json.dumps({"ext_a": "1", "ext_b": "2"})
            }

    @pytest.mark.ckan_config(config.CONFIG_EXTRAS_FIELD, "exfield")
    @pytest.mark.ckan_config(config.CONFIG_ALLOWED_EXTRAS, "ext_b")
    def test_extras_with_field_allow_some(self, test_request_context):
        with test_request_context("?ext_a=1&ext_b=2"):
            assert h.prepare_filters({}) == {
                "exfield": json.dumps({"ext_b": "2"})
            }

    @pytest.mark.ckan_config(config.CONFIG_EXTRAS_FIELD, "exfield")
    @pytest.mark.ckan_config(config.CONFIG_ALLOWED, "x")
    @pytest.mark.ckan_config(config.CONFIG_ALLOWED_EXTRAS, "ext_b")
    def test_combined(self, test_request_context):
        with test_request_context("?ext_a=1&ext_b=2"):
            assert h.prepare_filters(self._filters()) == {
                config.DEFAULT_PREFIX + "x": '["y"]',
                "exfield": json.dumps({"ext_b": "2"}),
            }


@pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index")
class TestListPreset:
    def test_basic(self, package_factory):
        d1 = package_factory(license_id="notspecified")
        d2 = package_factory(license_id="cc-by")

        preset = package_factory(
            **{
                config.DEFAULT_PREFIX + "license_id": '["notspecified"]',
                "type": "test_preset",
            }
        )
        packages = h.list_preset(preset["id"])
        assert packages["count"] == 1
        assert packages["results"][0]["id"] == d1["id"]

        preset = package_factory(
            **{
                config.DEFAULT_PREFIX + "license_id": '["cc-by"]',
                "type": "test_preset",
            }
        )
        packages = h.list_preset(preset["id"])
        assert packages["count"] == 1
        assert packages["results"][0]["id"] == d2["id"]

        preset = package_factory(
            **{
                config.DEFAULT_PREFIX
                + "license_id": '["notspecified", "cc-by"]',
                "type": "test_preset",
            }
        )
        packages = h.list_preset(preset["id"])
        assert packages["count"] == 0


@pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index")
class TestCountPreset:
    def test_basic(self, package_factory):
        d1 = package_factory(license_id="notspecified")
        d2 = package_factory(license_id="cc-by")

        preset = package_factory(
            **{
                config.DEFAULT_PREFIX + "license_id": '["notspecified"]',
                "type": "test_preset",
            }
        )
        assert h.count_preset(preset["id"]) == 1

        preset = package_factory(
            **{
                config.DEFAULT_PREFIX + "license_id": '["cc-by"]',
                "type": "test_preset",
            }
        )
        assert h.count_preset(preset["id"]) == 1

        preset = package_factory(
            **{
                config.DEFAULT_PREFIX
                + "license_id": '["notspecified", "cc-by"]',
                "type": "test_preset",
            }
        )
        assert h.count_preset(preset["id"]) == 0


@pytest.mark.usefixtures("with_plugins", "clean_db")
class TestPayloadFromPreset:
    def test_basic(self, package_factory):
        preset = package_factory(
            **{
                config.DEFAULT_PREFIX + "license_id": '["notspecified"]',
                "type": "test_preset",
            }
        )
        payload = h.payload_from_preset(preset["id"])
        assert payload == {"extras": {}, "fq": 'license_id:"notspecified"'}

    def test_exclude_self(self, package_factory):
        preset = package_factory(
            **{
                config.DEFAULT_PREFIX + "license_id": '["notspecified"]',
                "type": "test_preset",
            }
        )
        payload = h.payload_from_preset(preset["id"], True)
        assert payload == {
            "extras": {},
            "fq": 'license_id:"notspecified" -id:{}'.format(preset["id"]),
        }

    @pytest.mark.ckan_config(config.CONFIG_EXTRAS_FIELD, "notes")
    def test_with_extras(self, package_factory):
        preset = package_factory(
            **{
                config.DEFAULT_PREFIX + "license_id": '["notspecified"]',
                "type": "test_preset",
                "notes": '{"ext_a": "1"}',
            }
        )
        payload = h.payload_from_preset(preset["id"])
        assert payload == {
            "extras": {"ext_a": "1"},
            "fq": 'license_id:"notspecified"',
        }

    def test_multi_facet(self, package_factory):
        preset = package_factory(
            **{
                config.DEFAULT_PREFIX
                + "license_id": '["notspecified", "cc-by"]',
                "type": "test_preset",
            }
        )
        payload = h.payload_from_preset(preset["id"])
        assert payload == {
            "extras": {},
            "fq": 'license_id:"notspecified" license_id:"cc-by"',
        }
