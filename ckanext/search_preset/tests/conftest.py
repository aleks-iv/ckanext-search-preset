import pytest
from ckan.tests import factories
from pytest_factoryboy import register


@register
class UserFactory(factories.User):
    pass


class DatasetFactory(factories.Dataset):
    pass


class OrganizationFactory(factories.Organization):
    pass


register(DatasetFactory, "dataset")
register(OrganizationFactory, "organization")
