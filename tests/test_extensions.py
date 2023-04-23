import pytest

from encepp.extensions import SingleJsonItemStringExporter
from encepp.items import Study


@pytest.fixture(params=[1234, 999, 'Hello'])
def simple_item_json_pair(request):
    study = Study(eu_pas_register_number=f'EUPAS{request.param}')
    return {
        'item': study,
        'json': '{\n"eu_pas_register_number": "' + str(request.param) + '"\n}'
    }


def test_single_json_item_export(simple_item_json_pair):
    exporter = SingleJsonItemStringExporter()
    assert exporter.export_item(
        simple_item_json_pair['item']) == simple_item_json_pair['json']
