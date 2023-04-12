# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import item
from datetime import datetime as dt


def serialize_id(x): return x.replace(
    'EUPAS', '', 1) if isinstance(x, str) else x


def serialize_primary_scope(x): return x.replace('Primary scope : ', '', 1)
def serialize_date(x): return dt.strptime(x, '%d/%m/%Y').date()


def serialize_encepp_document_url(x):
    if x[0] != '/':
        return x
    if x.split(";")[0][-1] == '/':
        return 'Empty Url'
    return f'https://www.encepp.eu{x.split(";")[0]}'


class Study(item.Item):
    url = item.Field()
    eu_pas_register_number = item.Field(serializer=serialize_id)
    state = item.Field()
    title = item.Field()
    update_date = item.Field(serializer=serialize_date)
    registration_date = item.Field(serializer=serialize_date)
    acronym = item.Field()
    study_type = item.Field()
    requested_by_regulator = item.Field()
    risk_managment_plan = item.Field()
    regulatory_procedure_number = item.Field()
    centre_name = item.Field()
    centre_location = item.Field()
    centre_name_of_investigator = item.Field()
    centre_organisation = item.Field()
    collaboration_with_research_network = item.Field()
    country_type = item.Field()
    countries = item.Field()
    funding_contract_date_planed = item.Field(serializer=serialize_date)
    funding_contract_date_actual = item.Field(serializer=serialize_date)
    data_collection_date_planed = item.Field(serializer=serialize_date)
    data_collection_date_actual = item.Field(serializer=serialize_date)
    data_analysis_date_planed = item.Field(serializer=serialize_date)
    data_analysis_date_actual = item.Field(serializer=serialize_date)
    iterim_report_date_planed = item.Field(serializer=serialize_date)
    iterim_report_date_actual = item.Field(serializer=serialize_date)
    final_study_date_planed = item.Field(serializer=serialize_date)
    final_study_date_actual = item.Field(serializer=serialize_date)
    funding_companies_names = item.Field()
    funding_companies_percentage = item.Field()
    funding_charities_names = item.Field()
    funding_charities_percentage = item.Field()
    funding_government_body_names = item.Field()
    funding_government_body_percentage = item.Field()
    funding_research_councils_names = item.Field()
    funding_research_councils_percentage = item.Field()
    funding_eu_scheme_names = item.Field()
    funding_eu_scheme_percentage = item.Field()
    funding_other_names = item.Field()
    funding_other_percentage = item.Field()
    substance_atc = item.Field()
    substance_inn = item.Field()
    medical_conditions = item.Field()
    additional_medical_conditions = item.Field()
    age_population = item.Field()
    sex_population = item.Field()
    other_population = item.Field()
    number_of_subjects = item.Field()
    uses_established_data_source = item.Field()
    data_source_types = item.Field()
    data_sources_registered_with_encepp = item.Field()
    data_sources_not_registered_with_encepp = item.Field()
    scopes = item.Field()
    primary_scope = item.Field(serializer=serialize_primary_scope)
    primary_outcomes = item.Field()
    secondary_outcomes = item.Field()
    study_design = item.Field()
    follow_up = item.Field()
    protocol_document_url = item.Field(
        serializer=serialize_encepp_document_url)
    latest_protocol_document_url = item.Field(
        serializer=serialize_encepp_document_url)
    result_document_url = item.Field(serializer=serialize_encepp_document_url)
    latest_result_document_url = item.Field(
        serializer=serialize_encepp_document_url)
    references = item.Field()
    other_documents_url = item.Field(serializer=lambda x: list(map(serialize_encepp_document_url, x)))
