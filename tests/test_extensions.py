import json
from pathlib import Path

import pytest

from eupas.extensions import SingleJsonItemStringExporter, ItemHistoryComparer
from eupas.items import EU_PAS_Study


@pytest.fixture(params=[1234, 999, 'Hello'])
def simple_item_json_pair(request):
    study = EU_PAS_Study(eu_pas_register_number=f'EUPAS{request.param}')
    return {
        'item': study,
        'json': '{\n"eu_pas_register_number": "' + str(request.param) + '"\n}'
    }


def test_single_json_item_export(simple_item_json_pair):
    exporter = SingleJsonItemStringExporter()
    assert exporter.export_item(
        simple_item_json_pair['item']) == simple_item_json_pair['json']


@pytest.fixture()
def json_file(tmp_path: Path):
    path = tmp_path / "pytest.json"
    path.write_text("{}")
    return path


@pytest.fixture()
def project_settings(project_settings, json_file: Path):
    project_settings.set('ITEMHISTORYCOMPARER_JSON_INPUT_PATH',
                         str(json_file.absolute()), 100)
    project_settings.set(
        'ITEMHISTORYCOMPARER_JSON_OUTPUT_PATH', str(json_file.parent.absolute()), 100)
    project_settings.set('ITEMHISTORYCOMPARER_ENABLED', True, 100)
    return project_settings


@pytest.fixture(params=[1234])
def history_comparer(json_file: Path, crawler, request):
    study = [{'eu_pas_register_number': f'EUPAS{request.param}',
              'url': 'https://example.com'}]
    with json_file.absolute().open('w', encoding='UTF-8') as f:
        json.dump(study, f)
    return ItemHistoryComparer.from_crawler(crawler)


@pytest.mark.skip("Not implemented")
def test_history_comparer(history_comparer, simple_item, simple_spider):
    history_comparer.item_scraped(simple_item, simple_spider)
