# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import item

from datetime import datetime as dt


def serialize_id(x: str) -> str:
    return x.replace('EUPAS', '', 1)


def serialize_primary_scope(x: str) -> str:
    return x.replace('Primary scope : ', '', 1)


def serialize_date(x: str) -> dt:
    return dt.strptime(x, '%d/%m/%Y').date()


# TODO: Simply drop empty values?
def serialize_encepp_document_url(x: str, empty_url_name: str = 'Empty Url') -> str:
    if not x:
        return empty_url_name
    # Leave absolute paths unchanged (could be external urls)
    if x[0] != '/':
        return x
    # All relative urls ending with / lead do not lead to a new website
    if x.split(";")[0][-1] == '/':
        return empty_url_name
    return f'https://www.encepp.eu{x.split(";")[0]}'


class EMA_RWD_Study(item.Item):
    url = item.Field(required=True)
    eu_pas_register_number = item.Field(
        primary_key=True, required=True, serializer=serialize_id, sql_type=int)
    state = item.Field(required=True)
    title = item.Field(required=True)
    update_date = item.Field(required=True, serializer=serialize_date)
    registration_date = item.Field(required=True, serializer=serialize_date)
    description = item.Field()
    requested_by_regulator = item.Field(required=True)
    risk_management_plan = item.Field()
    regulatory_procedure_number = item.Field()
    # centre_name = item.Field()
    # centre_location = item.Field()
    # centre_name_of_investigator = item.Field()
    # centre_organisation = item.Field()
    # collaboration_with_research_network = item.Field(required=True)
    # country_type = item.Field(required=True)
    countries = item.Field(required=True)
    funding_contract_date_planed = item.Field(serializer=serialize_date)
    funding_contract_date_actual = item.Field(serializer=serialize_date)
    data_collection_date_planed = item.Field(serializer=serialize_date)
    data_collection_date_actual = item.Field(serializer=serialize_date)
    data_analysis_date_planed = item.Field(serializer=serialize_date)
    data_analysis_date_actual = item.Field(serializer=serialize_date)
    iterim_report_date_planed = item.Field(serializer=serialize_date)
    iterim_report_date_actual = item.Field(serializer=serialize_date)
    final_report_date_planed = item.Field(serializer=serialize_date)
    final_report_date_actual = item.Field(serializer=serialize_date)
    # funding_companies_names = item.Field()
    # funding_companies_percentage = item.Field(sql_type=int)
    # funding_charities_names = item.Field()
    # funding_charities_percentage = item.Field(sql_type=int)
    # funding_government_body_names = item.Field()
    # funding_government_body_percentage = item.Field(sql_type=int)
    # funding_research_councils_names = item.Field()
    # funding_research_councils_percentage = item.Field(sql_type=int)
    # funding_eu_scheme_names = item.Field()
    # funding_eu_scheme_percentage = item.Field(sql_type=int)
    # funding_other_names = item.Field()
    # funding_other_percentage = item.Field()  # List of ints
    protocol_document_url = item.Field(
        serializer=serialize_encepp_document_url)
    study_topic = item.Field()
    study_topic_other = item.Field()
    study_type = item.Field(required=True)
    study_type_other = item.Field()
    scopes = item.Field(required=True)
    non_interventional_study_design = item.Field()
    non_interventional_study_design_other = item.Field()
    substance_brand_name = item.Field()
    substance_brand_name_other = item.Field()
    substance_atc = item.Field()
    substance_inn = item.Field()
    medical_conditions = item.Field(required=True)
    additional_medical_conditions = item.Field()
    age_population = item.Field(required=True)
    special_population = item.Field()
    special_population_other = item.Field()
    number_of_subjects = item.Field(required=True, sql_type=int)
    # uses_established_data_source = item.Field(required=True)
    data_sources_registered_with_encepp = item.Field()
    data_sources_not_registered_with_encepp = item.Field()
    data_source_types = item.Field(required=True)
    data_source_types_other = item.Field()
    outcomes = item.Field(required=True)
    result_document_url = item.Field(serializer=serialize_encepp_document_url)
    latest_result_document_url = item.Field(
        serializer=serialize_encepp_document_url)
    references = item.Field(sql_name='document_references')
    other_documents_url = item.Field(
        serializer=lambda x: list(map(serialize_encepp_document_url, x)))


class EU_PAS_Study(item.Item):
    url = item.Field(required=True)
    eu_pas_register_number = item.Field(
        primary_key=True, required=True, serializer=serialize_id, sql_type=int)
    state = item.Field(required=True)
    title = item.Field(required=True)
    update_date = item.Field(required=True, serializer=serialize_date)
    registration_date = item.Field(required=True, serializer=serialize_date)
    acronym = item.Field()
    study_type = item.Field(required=True)
    description = item.Field()
    requested_by_regulator = item.Field(required=True)
    risk_management_plan = item.Field()
    regulatory_procedure_number = item.Field()
    centre_name = item.Field()
    centre_location = item.Field()
    centre_name_of_investigator = item.Field()
    centre_organisation = item.Field()
    collaboration_with_research_network = item.Field(required=True)
    country_type = item.Field(required=True)
    countries = item.Field(required=True)
    funding_contract_date_planed = item.Field(serializer=serialize_date)
    funding_contract_date_actual = item.Field(serializer=serialize_date)
    data_collection_date_planed = item.Field(serializer=serialize_date)
    data_collection_date_actual = item.Field(serializer=serialize_date)
    data_analysis_date_planed = item.Field(serializer=serialize_date)
    data_analysis_date_actual = item.Field(serializer=serialize_date)
    iterim_report_date_planed = item.Field(serializer=serialize_date)
    iterim_report_date_actual = item.Field(serializer=serialize_date)
    final_report_date_planed = item.Field(serializer=serialize_date)
    final_report_date_actual = item.Field(serializer=serialize_date)
    funding_companies_names = item.Field()
    funding_companies_percentage = item.Field(sql_type=int)
    funding_charities_names = item.Field()
    funding_charities_percentage = item.Field(sql_type=int)
    funding_government_body_names = item.Field()
    funding_government_body_percentage = item.Field(sql_type=int)
    funding_research_councils_names = item.Field()
    funding_research_councils_percentage = item.Field(sql_type=int)
    funding_eu_scheme_names = item.Field()
    funding_eu_scheme_percentage = item.Field(sql_type=int)
    funding_other_names = item.Field()
    funding_other_percentage = item.Field()  # List of ints
    substance_atc = item.Field()
    substance_inn = item.Field()
    medical_conditions = item.Field(required=True)
    additional_medical_conditions = item.Field()
    age_population = item.Field(required=True)
    sex_population = item.Field(required=True)
    other_population = item.Field()
    number_of_subjects = item.Field(required=True, sql_type=int)
    uses_established_data_source = item.Field(required=True)
    data_source_types = item.Field(required=True)
    data_sources_registered_with_encepp = item.Field()
    data_sources_not_registered_with_encepp = item.Field()
    scopes = item.Field(required=True)
    primary_scope = item.Field(
        required=True, serializer=serialize_primary_scope)
    primary_outcomes = item.Field(required=True)
    secondary_outcomes = item.Field(required=True)
    study_design = item.Field(required=True)
    follow_up = item.Field(required=True)
    protocol_document_url = item.Field(
        serializer=serialize_encepp_document_url)
    latest_protocol_document_url = item.Field(
        serializer=serialize_encepp_document_url)
    result_document_url = item.Field(serializer=serialize_encepp_document_url)
    latest_result_document_url = item.Field(
        serializer=serialize_encepp_document_url)
    references = item.Field(sql_name='document_references')
    other_documents_url = item.Field(
        serializer=lambda x: list(map(serialize_encepp_document_url, x)))
