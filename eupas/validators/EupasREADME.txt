The Study Schema is based on the JSON Schema draft 2020-12.

[1] https://www.encepp.eu/encepp/studyRegistration.htm

Notes:

    title:
    maxLength has been set to 500 based on the maxlength attribute of the element #title in [1]

    acronym:
    maxLength has been set to 50 based on the maxlength attribute of the element #acronym in [1]

    description:
    maxLength has been set to 2000 based on the value attribute of the element [name=countdown] in [1]

    regulatory_procedure_number:
    maxLength has been set to 400 based on the value attribute of the element [name=countdown2] in [1]

    centre_name:
    maxLength has been set to 250 based on the maxlength attribute of the element #studyReg.conductingCentre.centre in [1]

    centre_location:
    maxLength has been set to 250 based on the maxlength attribute of the element #studyReg.conductingCentre.location in [1]

    centre_organisation:
    maxLength has been set to 150 based on the maxlength attribute of the element #organisation.contact.orgAffiliation in [1]

    countries:
    minLength and maxLength are based on the actual values like "Chad" (length: 4) and "Saint Helena, Ascension and Tristan da Cunha" (length: 44)
    Values can be found in [1] using the xpath .//*[@id="studyReg.studyCountries.nationalCountry"]/option/text() or .//*[@id="studyReg.studyCountries.nationalCountry"]/option/@value
    It is possible to check for all possible values (249 countries):
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
    
    funding_companies_names:
    maxLength has been set to 250 based on the maxlength attribute of the element #studyReg.sourcesOfFunding.pharmaceuticalCompanies.name in [1]

    funding_charities_names:
    maxLength has been set to 250 based on the maxlength attribute of the element #studyReg.sourcesOfFunding.charities.name in [1]

    funding_companies_names:
    maxLength has been set to 250 based on the maxlength attribute of the element #studyReg.sourcesOfFunding.pharmaceuticalCompanies.name in [1]

    funding_government_body_names:
    maxLength has been set to 250 based on the maxlength attribute of the element #studyReg.sourcesOfFunding.govtBody.name in [1]

    funding_research_councils_names:
    maxLength has been set to 250 based on the maxlength attribute of the element #studyReg.sourcesOfFunding.researchCouncils.name in [1]

    funding_eu_scheme_names:
    maxLength has been set to 250 based on the maxlength attribute of the element #studyReg.sourcesOfFunding.euFundingScheme.name in [1]

    funding_other_names:
    items.maxLength has been set to 250 based on the maxlength attribute of the element #studyReg.sourcesOfFunding.otherFunding0.name, #studyReg.sourcesOfFunding.otherFunding1.name, #studyReg.sourcesOfFunding.otherFunding2.name, #studyReg.sourcesOfFunding.otherFunding3.name in [1]

    medical_conditions:
    uniqueItems can not be set to true, because 2 out of 2535 studies had duplicate fields on last check

    additional_medical_conditions
    maxLength has been set to 400 based on the value attribute of the element [name=countdown2] on the second page in [1]

    number_of_subjects:
    maximum has been set to 99999999 based on the biggest achievable number with length 8 based on the maxlength attribute of the element #studyReg.numberOfPatients.estTotalNoOfPatients on the second page in [1]

    data_source_types:
    items.maxLength has been set to 400 based on the value attribute of the element [name=countdown1] on the second page in [1]

    primary_scope:
    maxLength has been set to 250 + 16 based on the maxlength attribute of the element #studyReg.studyScope.otherScope on the third page in [1] and the length pf the prefix "Primary scope : "

    primary_outcomes:
    items.maxLength of the array with prefix "Yes" has been set to 400 based on the value attribute of the element [name=countdown1] on the third page in [1]
    
    secondary_outcomes:
    items.maxLength of the array with prefix "Yes" has been set to 400 based on the value attribute of the element [name=countdown2] on the third page in [1]

    study_design:
    maxLength has been set to 250 based on the maxlength attribute of the element #studyReg.studyDesign.otherStudyDesign on the third page in [1]

    references:
    uniqueItems can not be set to true, because 12 out of 2535 studies had duplicate fields on last check    