import pytest

from pytest_factoryboy import register

from ckan.tests import factories



@register
class UserFactory(factories.User):
    pass


class DatasetFactory(factories.Dataset):
    pass



class OrganizationFactory(factories.Organization):
    pass


register(DatasetFactory, "dataset")
register(OrganizationFactory, "organization")
