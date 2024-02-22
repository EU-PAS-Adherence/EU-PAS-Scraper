The Study Schema is based on the JSON Schema draft 2020-12.

[1] https://catalogues.ema.europa.eu/system/files/2024-01/Study_Questionnaire_Offline.pdf

Notes:

    title:
    maxLength has been set to 600 based on the old value in the EU PAS Register and the new concatenation with the acronym

    description:
    maxLength has been set to 2000 based on the documentad limitation in [1]

    lead_institution_encepp, lead_institution_not_encepp, additional_institutions_encepp, networks_encepp:
    maxLength and items.maxLength have been set to 250 based on the old values in the EU PAS Register
    additional_institutions_encepp also has non unique values in the array

    regulatory_procedure_number:
    maxLength has been set to 400 based on the old value in the EU PAS Register

    non_interventional_study_design_other:
    maxLength has been set to 2000 based on the documentad limitation in [1]

    substance_atc:
    uniqueItems can not be set to true, because duplicate values are possible:
    https://redirect.ema.europa.eu/resource/42316
    https://catalogues.ema.europa.eu/node/2724

    medical_conditions:
    TODO: Check if still true
    uniqueItems can not be set to true, because of the old value in the EU PAS Register

    additional_medical_conditions
    maxLength has been set to 400 based on the old value in the EU PAS Register

    number_of_subjects:
    maximum has been set to 99999999 based on the biggest achievable number with length 8 based on the old value in the EU PAS Register

    outcomes:
    maxLength has been set to 2000 based on the documentad limitation in [1]    

    references:
    uniqueItems can not be set to true, because 12 out of 2535 studies had duplicate fields on last check

    data_sources_registered_with_encepp:
    uniqueItems can not be set to true, because duplicate values are possible
    
    data_source_types:
    items.maxLength has been set to 400 based on the old value in the EU PAS Register

    funding_details, study_topic_other, study_type_other, non_interventional_scopes_other, 
    substance_brand_name_other, additional_medical_conditions, special_population_other, data_source_types_other:
    maxLength has been set to 2000 based on the maxLength of other limited free-text fields

    countries:
    minLength and maxLength are based on the old values from the EU PAS Register (249 countries):
        Afghanistan
        Åland Islands
        Albania
        Algeria
        American Samoa
        Andorra
        Angola
        Anguilla
        Antarctica
        Antigua and Barbuda
        Argentina
        Armenia
        Aruba
        Australia
        Austria
        Azerbaijan
        Bahamas
        Bahrain
        Bangladesh
        Barbados
        Belarus
        Belgium
        Belize
        Benin
        Bermuda
        Bhutan
        Bolivia, Plurinational State of
        Bosnia and Herzegovina
        Botswana
        Bouvet Island
        Brazil
        British Indian Ocean Territory
        Brunei Darussalam
        Bulgaria
        Burkina Faso
        Burundi
        Cambodia
        Cameroon
        Canada
        Cape Verde
        Cayman Islands
        Central African Republic
        Chad
        Chile
        China
        Christmas Island
        Cocos (Keeling) Islands
        Colombia
        Comoros
        Congo
        Congo, The Democratic Republic of the
        Cook Islands
        Costa Rica
        Côte d’Ivoire
        Croatia
        Cuba
        Cyprus
        Czechia
        Denmark
        Djibouti
        Dominica
        Dominican Republic
        Ecuador
        Egypt
        El Salvador
        Equatorial Guinea
        Eritrea
        Estonia
        Ethiopia
        Faeroe Islands
        Falkland Islands (Malvinas)
        Fiji
        Finland
        France
        French Guiana
        French Polynesia
        French Southern Territories
        Gabon
        Gambia
        Georgia
        Germany
        Ghana
        Gibraltar
        Greece
        Greenland
        Grenada
        Guadeloupe
        Guam
        Guatemala
        Guernsey
        Guinea
        Guinea-Bissau
        Guyana
        Haiti
        Heard Island and McDonald Islands
        Holy See (Vatican City State)
        Honduras
        Hong Kong
        Hungary
        Iceland
        India
        Indonesia
        Iran, Islamic Republic of
        Iraq
        Ireland
        Isle of Man
        Israel
        Italy
        Jamaica
        Japan
        Jersey
        Jordan
        Kazakhstan
        Kenya
        Kiribati
        Korea, Democratic People's Republic of
        Korea, Republic of
        Kuwait
        Kyrgyzstan
        Lao People's Democratic Republic
        Latvia
        Lebanon
        Lesotho
        Liberia
        Libyan Arab Jamahiriya
        Liechtenstein
        Lithuania
        Luxembourg
        Macau
        North Macedonia
        Madagascar
        Malawi
        Malaysia
        Maldives
        Mali
        Malta
        Marshall Islands
        Martinique
        Mauritania
        Mauritius
        Mayotte
        Mexico
        Micronesia, Federated States of
        Moldova, Republic of
        Monaco
        Mongolia
        Montenegro
        Montserrat
        Morocco
        Mozambique
        Myanmar
        Namibia
        Nauru
        Nepal
        Netherlands
        New Caledonia
        New Zealand
        Nicaragua
        Niger
        Nigeria
        Niue
        Norfolk Island
        Northern Mariana Islands
        Norway
        Oman
        Pakistan
        Palau
        Palestinian Territory, Occupied
        Panama
        Papua New Guinea
        Paraguay
        Peru
        Philippines
        Pitcairn
        Poland
        Portugal
        Puerto Rico
        Qatar
        Réunion
        Romania
        Russian Federation
        Rwanda
        Saint Helena, Ascension and Tristan da Cunha
        Saint Kitts and Nevis
        Saint Lucia
        Saint Pierre and Miquelon
        Saint Vincent and the Grenadines
        Samoa
        San Marino
        São Tomé and Príncipe
        Saudi Arabia
        Senegal
        Serbia
        Seychelles
        Sierra Leone
        Singapore
        Slovakia
        Slovenia
        Solomon Islands
        Somalia
        South Africa
        South Georgia and the South Sandwich Islands
        Spain
        Sri Lanka
        Sudan
        Suriname
        Svalbard and Jan Mayen
        Eswatini
        Sweden
        Switzerland
        Syria
        Taiwan
        Tajikistan
        Tanzania, United Republic of
        Thailand
        Timor-Leste
        Togo
        Tokelau
        Tonga
        Trinidad and Tobago
        Tunisia
        Turkey
        Turkmenistan
        Turks and Caicos Islands
        Tuvalu
        Uganda
        Ukraine
        United Arab Emirates
        United Kingdom
        United States
        United States Minor Outlying Islands
        Uruguay
        Uzbekistan
        Vanuatu
        Venezuela, Bolivarian Republic of
        Viet Nam
        Virgin Islands, British
        Virgin Islands, U.S.
        Wallis and Futuna
        Western Sahara
        Yemen
        Zambia
        Zimbabwe
        Kosovo
        Saint Barthelemy
        Saint Martin (French Part)
        Curaçao