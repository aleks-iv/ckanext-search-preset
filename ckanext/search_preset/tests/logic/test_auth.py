import ckan.plugins.toolkit as tk
import pytest
from ckan.tests.helpers import call_auth


@pytest.mark.usefixtures("with_plugins", "clean_db")
class TestPresetCreate:
    def test_anon_not_allowed(self):
        with pytest.raises(tk.NotAuthorized):
            call_auth("search_preset_preset_create", {"user": ""})

    def test_user_not_allowed(self, user):
        with pytest.raises(tk.NotAuthorized):
            call_auth("search_preset_preset_create", {"user": user["name"]})

    @pytest.mark.parametrize("user__sysadmin", [True])
    def test_sysadmin_allowed(self, user):
        assert call_auth("search_preset_preset_create", {"user": user["name"]})


@pytest.mark.usefixtures("with_plugins", "clean_db")
class TestPresetList:
    def test_non_existing(self):
        with pytest.raises(tk.ObjectNotFound):
            call_auth("search_preset_preset_list", {"user": ""}, id="missing")

    def test_public(self, package):
        assert call_auth(
            "search_preset_preset_list", {"user": ""}, id=package["id"]
        )

    def test_private(self, package_factory, user, organization_factory):
        org = organization_factory(user=user)
        package = package_factory(user=user, private=True, owner_org=org["id"])
        with pytest.raises(tk.NotAuthorized):
            call_auth(
                "search_preset_preset_list", {"user": ""}, id=package["id"]
            )

        assert call_auth(
            "search_preset_preset_list",
            {"user": user["name"]},
            id=package["id"],
        )
