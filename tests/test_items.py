import pytest

from encepp.items import serialize_date
from encepp.items import serialize_encepp_document_url
from encepp.items import serialize_id
from encepp.items import serialize_primary_scope


@pytest.mark.parametrize('id, expected', [
    ('EUPAS123', '123'),
    ('EUPAS31415', '31415')
])
def test_id_serializer_strips_valid_id(id, expected):
    assert serialize_id(id) == expected


@pytest.mark.parametrize('id, expected', [
    ('EUPAS123EUPAS', '123EUPAS'),
    ('hello this is a test', 'hello this is a test')
])
def test_id_serializer_strips_other_strings(id, expected):
    assert serialize_id(id) == expected


@pytest.mark.parametrize('scope, expected', [
    ('Primary scope : Hello this is a scope', 'Hello this is a scope'),
    ('Primary scope : ', '')
])
def test_primary_scope_serializer_strips_valid_scopes(scope, expected):
    assert serialize_primary_scope(scope) == expected


@pytest.mark.parametrize('scope, expected', [
    ('Primary scope : Primary scope : ', 'Primary scope : '),
    ('hello this is a test', 'hello this is a test')
])
def test_primary_scope_serializer_strips_other_strings(scope, expected):
    assert serialize_primary_scope(scope) == expected


@pytest.mark.parametrize('date, expected', [
    ('01/02/2003', '2003-02-01'),
    ('31/05/9999', '9999-05-31'),
])
def test_date_serializer_strips_valid_dates(date, expected):
    assert serialize_date(date).isoformat() == expected


@pytest.mark.parametrize('date', [
    ('01-02-2003'), ('01.12.99'),
    ('31.05.9999'), ('hello there'),
])
def test_date_serializer_fails_other_strings(date):
    with pytest.raises(ValueError):
        serialize_date(date)


@pytest.mark.parametrize('url, expected', [
    ('/12345', 'https://www.encepp.eu/12345'),
    ('/12345;', 'https://www.encepp.eu/12345'),
    ('/12345;jsessionid=1234', 'https://www.encepp.eu/12345'),
    ('htpps://www.example.com', 'htpps://www.example.com'),
    ('htpps://', 'htpps://'),
    ('/;jsessionid=1234', 'pytest_empty'),
    ('/hi/;jsessionid=1234', 'pytest_empty'),
    ('', 'pytest_empty')
])
def test_serialize_encepp_document_url(url, expected):
    assert serialize_encepp_document_url(url, empty_url_name='pytest_empty') == expected
