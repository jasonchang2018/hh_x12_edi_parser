create or replace table
    edwprodhh.edi_837i_parser.hl_billing_providers
as
with filtered_hl as
(
    select      *
    from        edwprodhh.edi_837i_parser.response_flat
    where       hl_index_billing_20         is not null                                          --0 Pre-Filter
                and hl_index_subscriber_22  is null
                and hl_index_patient_23     is null
                and claim_index             is null
)
, provider_hl as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'HL_PREFIX_PROVIDER'
                            when    flattened.index = 2   then      'HL_ID_PROVIDER'
                            when    flattened.index = 3   then      'HL_PARENT_ID_PROVIDER'
                            when    flattened.index = 4   then      'HL_LEVEL_CODE_PROVIDER' --20 BILLING, 22 SUBSCRIBER, 23 PATIENT
                            when    flattened.index = 5   then      'HL_CHILD_CODE_PROVIDER' --1 HAS CHILD NODE, 0 NO CHILD NODE
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^HL.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'HL_PREFIX_PROVIDER',
                        'HL_ID_PROVIDER',
                        'HL_PARENT_ID_PROVIDER',
                        'HL_LEVEL_CODE_PROVIDER',
                        'HL_CHILD_CODE_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    HL_PREFIX_PROVIDER,
                    HL_ID_PROVIDER,
                    HL_PARENT_ID_PROVIDER,
                    HL_LEVEL_CODE_PROVIDER,
                    HL_CHILD_CODE_PROVIDER
                )
)
, provider_prv as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PRV_PREFIX_PROVIDER'
                            when    flattened.index = 2   then      'PROVIDER_CODE_PROVIDER'
                            when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_PROVIDER'
                            when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^PRV.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PRV_PREFIX_PROVIDER',
                        'PROVIDER_CODE_PROVIDER',
                        'REFERENCE_ID_QUALIFIER_PROVIDER',
                        'PROVIDER_TAXONOMY_CODE_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    PRV_PREFIX_PROVIDER,
                    PROVIDER_CODE_PROVIDER,
                    REFERENCE_ID_QUALIFIER_PROVIDER_PRV,
                    PROVIDER_TAXONOMY_CODE_PROVIDER
                )
)
, provider_nm85 as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_PROVIDER'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_PROVIDER'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_PROVIDER'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_PROVIDER'
                            when    flattened.index = 5   then      'FIRST_NAME_PROVIDER'
                            when    flattened.index = 6   then      'MIDDLE_NAME_PROVIDER'
                            when    flattened.index = 7   then      'NAME_PREFIX_PROVIDER'
                            when    flattened.index = 8   then      'NAME_SUFFIX_PROVIDER'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_PROVIDER'
                            when    flattened.index = 10  then      'ID_CODE_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^NM1\\*85.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_PROVIDER',
                        'ENTITY_IDENTIFIER_CODE_PROVIDER',
                        'ENTITY_TYPE_QUALIFIER_PROVIDER',
                        'LAST_NAME_ORG_PROVIDER',
                        'FIRST_NAME_PROVIDER',
                        'MIDDLE_NAME_PROVIDER',
                        'NAME_PREFIX_PROVIDER',
                        'NAME_SUFFIX_PROVIDER',
                        'ID_CODE_QUALIFIER_PROVIDER',
                        'ID_CODE_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    NAME_CODE_PROVIDER,
                    ENTITY_IDENTIFIER_CODE_PROVIDER,
                    ENTITY_TYPE_QUALIFIER_PROVIDER,
                    LAST_NAME_ORG_PROVIDER,
                    FIRST_NAME_PROVIDER,
                    MIDDLE_NAME_PROVIDER,
                    NAME_PREFIX_PROVIDER,
                    NAME_SUFFIX_PROVIDER,
                    ID_CODE_QUALIFIER_PROVIDER,
                    ID_CODE_PROVIDER
                )
)
, provider_n3 as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PROVIDER'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_PROVIDER'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*85'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PROVIDER',
                        'ADDRESS_LINE_1_PROVIDER',
                        'ADDRESS_LINE_2_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    ADDRESS_CODE_PROVIDER_N3,
                    ADDRESS_LINE_1_PROVIDER,
                    ADDRESS_LINE_2_PROVIDER
                )
)
, provider_n4 as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PROVIDER'
                            when    flattened.index = 2   then      'CITY_PROVIDER'
                            when    flattened.index = 3   then      'ST_PROVIDER'
                            when    flattened.index = 4   then      'ZIP_PROVIDER'
                            when    flattened.index = 5   then      'COUNTRY_PROVIDER'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_PROVIDER'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*85'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PROVIDER',
                        'CITY_PROVIDER',
                        'ST_PROVIDER',
                        'ZIP_PROVIDER',
                        'COUNTRY_PROVIDER',
                        'LOCATION_QUALIFIER_PROVIDER',
                        'LOCATION_IDENTIFIER_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    ADDRESS_CODE_PROVIDER_N4,
                    CITY_PROVIDER,
                    ST_PROVIDER,
                    ZIP_PROVIDER,
                    COUNTRY_PROVIDER,
                    LOCATION_QUALIFIER_PROVIDER,
                    LOCATION_IDENTIFIER_PROVIDER
                )
)
, provider_ref as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,
                    case    when    flattened.index = 1   then      'REF_CODE_PROVIDER'
                            when    flattened.index = 2   then      'REFERENCE_ID_QUALIFIER_PROVIDER'
                            when    flattened.index = 3   then      'REFERENCE_ID_PROVIDER'
                            when    flattened.index = 4   then      'DESCRIPTION_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^REF.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*85'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'REF_CODE_PROVIDER',
                        'REFERENCE_ID_QUALIFIER_PROVIDER',
                        'REFERENCE_ID_PROVIDER',
                        'DESCRIPTION_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    REF_CODE_PROVIDER,
                    REFERENCE_ID_QUALIFIER_PROVIDER_REF,
                    REFERENCE_ID_PROVIDER,
                    DESCRIPTION_PROVIDER
                )
)
, provider_per as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,
                    case    when    flattened.index = 1   then      'PROVIDER_CONTACT_PREFIX'
                            when    flattened.index = 2   then      'CONTACT_FUNCTION_CODE_PROVIDER'
                            when    flattened.index = 3   then      'CONTACT_NAME_PROVIDER'
                            when    flattened.index = 4   then      'COMMUNICATION_QUALIFIER_1_PROVIDER'
                            when    flattened.index = 5   then      'COMMUNICATION_NUMBER_1_PROVIDER'
                            when    flattened.index = 6   then      'COMMUNICATION_QUALIFIER_2_PROVIDER'
                            when    flattened.index = 7   then      'COMMUNICATION_NUMBER_2_PROVIDER'
                            when    flattened.index = 8   then      'COMMUNICATION_QUALIFIER_3_PROVIDER'
                            when    flattened.index = 9   then      'COMMUNICATION_NUMBER_3_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^PER.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*85'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PROVIDER_CONTACT_PREFIX',
                        'CONTACT_FUNCTION_CODE_PROVIDER',
                        'CONTACT_NAME_PROVIDER',
                        'COMMUNICATION_QUALIFIER_1_PROVIDER',
                        'COMMUNICATION_NUMBER_1_PROVIDER',
                        'COMMUNICATION_QUALIFIER_2_PROVIDER',
                        'COMMUNICATION_NUMBER_2_PROVIDER',
                        'COMMUNICATION_QUALIFIER_3_PROVIDER',
                        'COMMUNICATION_NUMBER_3_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    PROVIDER_CONTACT_PREFIX,
                    CONTACT_FUNCTION_CODE_PROVIDER,
                    CONTACT_NAME_PROVIDER,
                    COMMUNICATION_QUALIFIER_1_PROVIDER,
                    COMMUNICATION_NUMBER_1_PROVIDER,
                    COMMUNICATION_QUALIFIER_2_PROVIDER,
                    COMMUNICATION_NUMBER_2_PROVIDER,
                    COMMUNICATION_QUALIFIER_3_PROVIDER,
                    COMMUNICATION_NUMBER_3_PROVIDER
                )
)
, provider_payto_nm87 as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_PROVIDER_PAYTO'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_PROVIDER_PAYTO'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_PROVIDER_PAYTO'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_PROVIDER_PAYTO'
                            when    flattened.index = 5   then      'FIRST_NAME_PROVIDER_PAYTO'
                            when    flattened.index = 6   then      'MIDDLE_NAME_PROVIDER_PAYTO'
                            when    flattened.index = 7   then      'NAME_PREFIX_PROVIDER_PAYTO'
                            when    flattened.index = 8   then      'NAME_SUFFIX_PROVIDER_PAYTO'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_PROVIDER_PAYTO'
                            when    flattened.index = 10  then      'ID_CODE_PROVIDER_PAYTO'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^NM1\\*87.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_PROVIDER_PAYTO',
                        'ENTITY_IDENTIFIER_CODE_PROVIDER_PAYTO',
                        'ENTITY_TYPE_QUALIFIER_PROVIDER_PAYTO',
                        'LAST_NAME_ORG_PROVIDER_PAYTO',
                        'FIRST_NAME_PROVIDER_PAYTO',
                        'MIDDLE_NAME_PROVIDER_PAYTO',
                        'NAME_PREFIX_PROVIDER_PAYTO',
                        'NAME_SUFFIX_PROVIDER_PAYTO',
                        'ID_CODE_QUALIFIER_PROVIDER_PAYTO',
                        'ID_CODE_PROVIDER_PAYTO'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    NAME_CODE_PROVIDER_PAYTO,
                    ENTITY_IDENTIFIER_CODE_PROVIDER_PAYTO,
                    ENTITY_TYPE_QUALIFIER_PROVIDER_PAYTO,
                    LAST_NAME_ORG_PROVIDER_PAYTO,
                    FIRST_NAME_PROVIDER_PAYTO,
                    MIDDLE_NAME_PROVIDER_PAYTO,
                    NAME_PREFIX_PROVIDER_PAYTO,
                    NAME_SUFFIX_PROVIDER_PAYTO,
                    ID_CODE_QUALIFIER_PROVIDER_PAYTO,
                    ID_CODE_PROVIDER_PAYTO
                )
)
, provider_payto_n3 as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PROVIDER_PAYTO'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_PROVIDER_PAYTO'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_PROVIDER_PAYTO'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*87'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PROVIDER_PAYTO',
                        'ADDRESS_LINE_1_PROVIDER_PAYTO',
                        'ADDRESS_LINE_2_PROVIDER_PAYTO'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    ADDRESS_CODE_PROVIDER_PAYTO_N3,
                    ADDRESS_LINE_1_PROVIDER_PAYTO,
                    ADDRESS_LINE_2_PROVIDER_PAYTO
                )
)
, provider_payto_n4 as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PROVIDER_PAYTO'
                            when    flattened.index = 2   then      'CITY_PROVIDER_PAYTO'
                            when    flattened.index = 3   then      'ST_PROVIDER_PAYTO'
                            when    flattened.index = 4   then      'ZIP_PROVIDER_PAYTO'
                            when    flattened.index = 5   then      'COUNTRY_PROVIDER_PAYTO'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_PROVIDER_PAYTO'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_PROVIDER_PAYTO'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*87'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PROVIDER_PAYTO',
                        'CITY_PROVIDER_PAYTO',
                        'ST_PROVIDER_PAYTO',
                        'ZIP_PROVIDER_PAYTO',
                        'COUNTRY_PROVIDER_PAYTO',
                        'LOCATION_QUALIFIER_PROVIDER_PAYTO',
                        'LOCATION_IDENTIFIER_PROVIDER_PAYTO'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    ADDRESS_CODE_PROVIDER_PAYTO_N4,
                    CITY_PROVIDER_PAYTO,
                    ST_PROVIDER_PAYTO,
                    ZIP_PROVIDER_PAYTO,
                    COUNTRY_PROVIDER_PAYTO,
                    LOCATION_QUALIFIER_PROVIDER_PAYTO,
                    LOCATION_IDENTIFIER_PROVIDER_PAYTO
                )
)
select      header.response_id,
            header.nth_functional_group,
            header.nth_transaction_set,
            header.index,
            header.hl_index_current,
            header.hl_index_billing_20,
            header.hl_index_subscriber_22,
            header.hl_index_patient_23,
            header.hl_prefix_provider,
            header.hl_id_provider,
            header.hl_parent_id_provider,
            header.hl_level_code_provider,
            header.hl_child_code_provider,
            prv.prv_prefix_provider,
            prv.provider_code_provider,
            prv.reference_id_qualifier_provider_prv,
            prv.provider_taxonomy_code_provider,
            nm85.name_code_provider,
            nm85.entity_identifier_code_provider,
            nm85.entity_type_qualifier_provider,
            nm85.last_name_org_provider,
            nm85.first_name_provider,
            nm85.middle_name_provider,
            nm85.name_prefix_provider,
            nm85.name_suffix_provider,
            nm85.id_code_qualifier_provider,
            nm85.id_code_provider,
            n3.address_code_provider_n3,
            n3.address_line_1_provider,
            n3.address_line_2_provider,
            n4.address_code_provider_n4,
            n4.city_provider,
            n4.st_provider,
            n4.zip_provider,
            n4.country_provider,
            n4.location_qualifier_provider,
            n4.location_identifier_provider,
            ref.ref_code_provider,
            ref.reference_id_qualifier_provider_ref,
            ref.reference_id_provider,
            ref.description_provider,
            per.provider_contact_prefix,
            per.contact_function_code_provider,
            per.contact_name_provider,
            per.communication_qualifier_1_provider,
            per.communication_number_1_provider,
            per.communication_qualifier_2_provider,
            per.communication_number_2_provider,
            per.communication_qualifier_3_provider,
            per.communication_number_3_provider,
            payto_nm87.name_code_provider_payto,
            payto_nm87.entity_identifier_code_provider_payto,
            payto_nm87.entity_type_qualifier_provider_payto,
            payto_nm87.last_name_org_provider_payto,
            payto_nm87.first_name_provider_payto,
            payto_nm87.middle_name_provider_payto,
            payto_nm87.name_prefix_provider_payto,
            payto_nm87.name_suffix_provider_payto,
            payto_nm87.id_code_qualifier_provider_payto,
            payto_nm87.id_code_provider_payto,
            payto_n3.address_code_provider_payto_n3,
            payto_n3.address_line_1_provider_payto,
            payto_n3.address_line_2_provider_payto,
            payto_n4.address_code_provider_payto_n4,
            payto_n4.city_provider_payto,
            payto_n4.st_provider_payto,
            payto_n4.zip_provider_payto,
            payto_n4.country_provider_payto,
            payto_n4.location_qualifier_provider_payto,
            payto_n4.location_identifier_provider_payto

from        provider_hl         as header
            left join
                provider_prv        as prv
                on  header.response_id          = prv.response_id
                and header.nth_functional_group = prv.nth_functional_group
                and header.nth_transaction_set  = prv.nth_transaction_set
                and header.hl_index_billing_20  = prv.hl_index_billing_20
            left join
                provider_nm85       as nm85
                on  header.response_id          = nm85.response_id
                and header.nth_functional_group = nm85.nth_functional_group
                and header.nth_transaction_set  = nm85.nth_transaction_set
                and header.hl_index_billing_20  = nm85.hl_index_billing_20
            left join
                provider_n3         as n3
                on  header.response_id          = n3.response_id
                and header.nth_functional_group = n3.nth_functional_group
                and header.nth_transaction_set  = n3.nth_transaction_set
                and header.hl_index_billing_20  = n3.hl_index_billing_20
            left join
                provider_n4         as n4
                on  header.response_id          = n4.response_id
                and header.nth_functional_group = n4.nth_functional_group
                and header.nth_transaction_set  = n4.nth_transaction_set
                and header.hl_index_billing_20  = n4.hl_index_billing_20
            left join
                provider_ref        as ref
                on  header.response_id          = ref.response_id
                and header.nth_functional_group = ref.nth_functional_group
                and header.nth_transaction_set  = ref.nth_transaction_set
                and header.hl_index_billing_20  = ref.hl_index_billing_20
            left join
                provider_per        as per
                on  header.response_id          = per.response_id
                and header.nth_functional_group = per.nth_functional_group
                and header.nth_transaction_set  = per.nth_transaction_set
                and header.hl_index_billing_20  = per.hl_index_billing_20
            left join
                provider_payto_nm87 as payto_nm87
                on  header.response_id          = payto_nm87.response_id
                and header.nth_functional_group = payto_nm87.nth_functional_group
                and header.nth_transaction_set  = payto_nm87.nth_transaction_set
                and header.hl_index_billing_20  = payto_nm87.hl_index_billing_20
            left join
                provider_payto_n3   as payto_n3
                on  header.response_id          = payto_n3.response_id
                and header.nth_functional_group = payto_n3.nth_functional_group
                and header.nth_transaction_set  = payto_n3.nth_transaction_set
                and header.hl_index_billing_20  = payto_n3.hl_index_billing_20
            left join
                provider_payto_n4   as payto_n4
                on  header.response_id          = payto_n4.response_id
                and header.nth_functional_group = payto_n4.nth_functional_group
                and header.nth_transaction_set  = payto_n4.nth_transaction_set
                and header.hl_index_billing_20  = payto_n4.hl_index_billing_20

order by    1,2,3
;



create or replace task
    edwprodhh.edi_837i_parser.insert_hl_billing_providers
    warehouse = analysis_wh
    after edwprodhh.edi_837i_parser.insert_response_flat
as
insert into
    edwprodhh.edi_837i_parser.hl_billing_providers
(
    RESPONSE_ID,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    INDEX,
    HL_INDEX_CURRENT,
    HL_INDEX_BILLING_20,
    HL_INDEX_SUBSCRIBER_22,
    HL_INDEX_PATIENT_23,
    HL_PREFIX_PROVIDER,
    HL_ID_PROVIDER,
    HL_PARENT_ID_PROVIDER,
    HL_LEVEL_CODE_PROVIDER,
    HL_CHILD_CODE_PROVIDER,
    PRV_PREFIX_PROVIDER,
    PROVIDER_CODE_PROVIDER,
    REFERENCE_ID_QUALIFIER_PROVIDER_PRV,
    PROVIDER_TAXONOMY_CODE_PROVIDER,
    NAME_CODE_PROVIDER,
    ENTITY_IDENTIFIER_CODE_PROVIDER,
    ENTITY_TYPE_QUALIFIER_PROVIDER,
    LAST_NAME_ORG_PROVIDER,
    FIRST_NAME_PROVIDER,
    MIDDLE_NAME_PROVIDER,
    NAME_PREFIX_PROVIDER,
    NAME_SUFFIX_PROVIDER,
    ID_CODE_QUALIFIER_PROVIDER,
    ID_CODE_PROVIDER,
    ADDRESS_CODE_PROVIDER_N3,
    ADDRESS_LINE_1_PROVIDER,
    ADDRESS_LINE_2_PROVIDER,
    ADDRESS_CODE_PROVIDER_N4,
    CITY_PROVIDER,
    ST_PROVIDER,
    ZIP_PROVIDER,
    COUNTRY_PROVIDER,
    LOCATION_QUALIFIER_PROVIDER,
    LOCATION_IDENTIFIER_PROVIDER,
    REF_CODE_PROVIDER,
    REFERENCE_ID_QUALIFIER_PROVIDER_REF,
    REFERENCE_ID_PROVIDER,
    DESCRIPTION_PROVIDER,
    PROVIDER_CONTACT_PREFIX,
    CONTACT_FUNCTION_CODE_PROVIDER,
    CONTACT_NAME_PROVIDER,
    COMMUNICATION_QUALIFIER_1_PROVIDER,
    COMMUNICATION_NUMBER_1_PROVIDER,
    COMMUNICATION_QUALIFIER_2_PROVIDER,
    COMMUNICATION_NUMBER_2_PROVIDER,
    COMMUNICATION_QUALIFIER_3_PROVIDER,
    COMMUNICATION_NUMBER_3_PROVIDER,
    NAME_CODE_PROVIDER_PAYTO,
    ENTITY_IDENTIFIER_CODE_PROVIDER_PAYTO,
    ENTITY_TYPE_QUALIFIER_PROVIDER_PAYTO,
    LAST_NAME_ORG_PROVIDER_PAYTO,
    FIRST_NAME_PROVIDER_PAYTO,
    MIDDLE_NAME_PROVIDER_PAYTO,
    NAME_PREFIX_PROVIDER_PAYTO,
    NAME_SUFFIX_PROVIDER_PAYTO,
    ID_CODE_QUALIFIER_PROVIDER_PAYTO,
    ID_CODE_PROVIDER_PAYTO,
    ADDRESS_CODE_PROVIDER_PAYTO_N3,
    ADDRESS_LINE_1_PROVIDER_PAYTO,
    ADDRESS_LINE_2_PROVIDER_PAYTO,
    ADDRESS_CODE_PROVIDER_PAYTO_N4,
    CITY_PROVIDER_PAYTO,
    ST_PROVIDER_PAYTO,
    ZIP_PROVIDER_PAYTO,
    COUNTRY_PROVIDER_PAYTO,
    LOCATION_QUALIFIER_PROVIDER_PAYTO,
    LOCATION_IDENTIFIER_PROVIDER_PAYTO
)
with filtered_hl as
(
    select      *
    from        edwprodhh.edi_837i_parser.response_flat
    where       hl_index_billing_20         is not null                                          --0 Pre-Filter
                and hl_index_subscriber_22  is null
                and hl_index_patient_23     is null
                and claim_index             is null
                and response_id not in (select response_id from edwprodhh.edi_837i_parser.hl_billing_providers)
)
, provider_hl as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'HL_PREFIX_PROVIDER'
                            when    flattened.index = 2   then      'HL_ID_PROVIDER'
                            when    flattened.index = 3   then      'HL_PARENT_ID_PROVIDER'
                            when    flattened.index = 4   then      'HL_LEVEL_CODE_PROVIDER' --20 BILLING, 22 SUBSCRIBER, 23 PATIENT
                            when    flattened.index = 5   then      'HL_CHILD_CODE_PROVIDER' --1 HAS CHILD NODE, 0 NO CHILD NODE
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^HL.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'HL_PREFIX_PROVIDER',
                        'HL_ID_PROVIDER',
                        'HL_PARENT_ID_PROVIDER',
                        'HL_LEVEL_CODE_PROVIDER',
                        'HL_CHILD_CODE_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    HL_PREFIX_PROVIDER,
                    HL_ID_PROVIDER,
                    HL_PARENT_ID_PROVIDER,
                    HL_LEVEL_CODE_PROVIDER,
                    HL_CHILD_CODE_PROVIDER
                )
)
, provider_prv as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PRV_PREFIX_PROVIDER'
                            when    flattened.index = 2   then      'PROVIDER_CODE_PROVIDER'
                            when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_PROVIDER'
                            when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^PRV.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PRV_PREFIX_PROVIDER',
                        'PROVIDER_CODE_PROVIDER',
                        'REFERENCE_ID_QUALIFIER_PROVIDER',
                        'PROVIDER_TAXONOMY_CODE_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    PRV_PREFIX_PROVIDER,
                    PROVIDER_CODE_PROVIDER,
                    REFERENCE_ID_QUALIFIER_PROVIDER_PRV,
                    PROVIDER_TAXONOMY_CODE_PROVIDER
                )
)
, provider_nm85 as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_PROVIDER'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_PROVIDER'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_PROVIDER'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_PROVIDER'
                            when    flattened.index = 5   then      'FIRST_NAME_PROVIDER'
                            when    flattened.index = 6   then      'MIDDLE_NAME_PROVIDER'
                            when    flattened.index = 7   then      'NAME_PREFIX_PROVIDER'
                            when    flattened.index = 8   then      'NAME_SUFFIX_PROVIDER'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_PROVIDER'
                            when    flattened.index = 10  then      'ID_CODE_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^NM1\\*85.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_PROVIDER',
                        'ENTITY_IDENTIFIER_CODE_PROVIDER',
                        'ENTITY_TYPE_QUALIFIER_PROVIDER',
                        'LAST_NAME_ORG_PROVIDER',
                        'FIRST_NAME_PROVIDER',
                        'MIDDLE_NAME_PROVIDER',
                        'NAME_PREFIX_PROVIDER',
                        'NAME_SUFFIX_PROVIDER',
                        'ID_CODE_QUALIFIER_PROVIDER',
                        'ID_CODE_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    NAME_CODE_PROVIDER,
                    ENTITY_IDENTIFIER_CODE_PROVIDER,
                    ENTITY_TYPE_QUALIFIER_PROVIDER,
                    LAST_NAME_ORG_PROVIDER,
                    FIRST_NAME_PROVIDER,
                    MIDDLE_NAME_PROVIDER,
                    NAME_PREFIX_PROVIDER,
                    NAME_SUFFIX_PROVIDER,
                    ID_CODE_QUALIFIER_PROVIDER,
                    ID_CODE_PROVIDER
                )
)
, provider_n3 as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PROVIDER'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_PROVIDER'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*85'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PROVIDER',
                        'ADDRESS_LINE_1_PROVIDER',
                        'ADDRESS_LINE_2_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    ADDRESS_CODE_PROVIDER_N3,
                    ADDRESS_LINE_1_PROVIDER,
                    ADDRESS_LINE_2_PROVIDER
                )
)
, provider_n4 as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PROVIDER'
                            when    flattened.index = 2   then      'CITY_PROVIDER'
                            when    flattened.index = 3   then      'ST_PROVIDER'
                            when    flattened.index = 4   then      'ZIP_PROVIDER'
                            when    flattened.index = 5   then      'COUNTRY_PROVIDER'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_PROVIDER'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*85'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PROVIDER',
                        'CITY_PROVIDER',
                        'ST_PROVIDER',
                        'ZIP_PROVIDER',
                        'COUNTRY_PROVIDER',
                        'LOCATION_QUALIFIER_PROVIDER',
                        'LOCATION_IDENTIFIER_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    ADDRESS_CODE_PROVIDER_N4,
                    CITY_PROVIDER,
                    ST_PROVIDER,
                    ZIP_PROVIDER,
                    COUNTRY_PROVIDER,
                    LOCATION_QUALIFIER_PROVIDER,
                    LOCATION_IDENTIFIER_PROVIDER
                )
)
, provider_ref as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,
                    case    when    flattened.index = 1   then      'REF_CODE_PROVIDER'
                            when    flattened.index = 2   then      'REFERENCE_ID_QUALIFIER_PROVIDER'
                            when    flattened.index = 3   then      'REFERENCE_ID_PROVIDER'
                            when    flattened.index = 4   then      'DESCRIPTION_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^REF.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*85'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'REF_CODE_PROVIDER',
                        'REFERENCE_ID_QUALIFIER_PROVIDER',
                        'REFERENCE_ID_PROVIDER',
                        'DESCRIPTION_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    REF_CODE_PROVIDER,
                    REFERENCE_ID_QUALIFIER_PROVIDER_REF,
                    REFERENCE_ID_PROVIDER,
                    DESCRIPTION_PROVIDER
                )
)
, provider_per as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,
                    case    when    flattened.index = 1   then      'PROVIDER_CONTACT_PREFIX'
                            when    flattened.index = 2   then      'CONTACT_FUNCTION_CODE_PROVIDER'
                            when    flattened.index = 3   then      'CONTACT_NAME_PROVIDER'
                            when    flattened.index = 4   then      'COMMUNICATION_QUALIFIER_1_PROVIDER'
                            when    flattened.index = 5   then      'COMMUNICATION_NUMBER_1_PROVIDER'
                            when    flattened.index = 6   then      'COMMUNICATION_QUALIFIER_2_PROVIDER'
                            when    flattened.index = 7   then      'COMMUNICATION_NUMBER_2_PROVIDER'
                            when    flattened.index = 8   then      'COMMUNICATION_QUALIFIER_3_PROVIDER'
                            when    flattened.index = 9   then      'COMMUNICATION_NUMBER_3_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^PER.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*85'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PROVIDER_CONTACT_PREFIX',
                        'CONTACT_FUNCTION_CODE_PROVIDER',
                        'CONTACT_NAME_PROVIDER',
                        'COMMUNICATION_QUALIFIER_1_PROVIDER',
                        'COMMUNICATION_NUMBER_1_PROVIDER',
                        'COMMUNICATION_QUALIFIER_2_PROVIDER',
                        'COMMUNICATION_NUMBER_2_PROVIDER',
                        'COMMUNICATION_QUALIFIER_3_PROVIDER',
                        'COMMUNICATION_NUMBER_3_PROVIDER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    PROVIDER_CONTACT_PREFIX,
                    CONTACT_FUNCTION_CODE_PROVIDER,
                    CONTACT_NAME_PROVIDER,
                    COMMUNICATION_QUALIFIER_1_PROVIDER,
                    COMMUNICATION_NUMBER_1_PROVIDER,
                    COMMUNICATION_QUALIFIER_2_PROVIDER,
                    COMMUNICATION_NUMBER_2_PROVIDER,
                    COMMUNICATION_QUALIFIER_3_PROVIDER,
                    COMMUNICATION_NUMBER_3_PROVIDER
                )
)
, provider_payto_nm87 as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_PROVIDER_PAYTO'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_PROVIDER_PAYTO'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_PROVIDER_PAYTO'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_PROVIDER_PAYTO'
                            when    flattened.index = 5   then      'FIRST_NAME_PROVIDER_PAYTO'
                            when    flattened.index = 6   then      'MIDDLE_NAME_PROVIDER_PAYTO'
                            when    flattened.index = 7   then      'NAME_PREFIX_PROVIDER_PAYTO'
                            when    flattened.index = 8   then      'NAME_SUFFIX_PROVIDER_PAYTO'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_PROVIDER_PAYTO'
                            when    flattened.index = 10  then      'ID_CODE_PROVIDER_PAYTO'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^NM1\\*87.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_PROVIDER_PAYTO',
                        'ENTITY_IDENTIFIER_CODE_PROVIDER_PAYTO',
                        'ENTITY_TYPE_QUALIFIER_PROVIDER_PAYTO',
                        'LAST_NAME_ORG_PROVIDER_PAYTO',
                        'FIRST_NAME_PROVIDER_PAYTO',
                        'MIDDLE_NAME_PROVIDER_PAYTO',
                        'NAME_PREFIX_PROVIDER_PAYTO',
                        'NAME_SUFFIX_PROVIDER_PAYTO',
                        'ID_CODE_QUALIFIER_PROVIDER_PAYTO',
                        'ID_CODE_PROVIDER_PAYTO'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    NAME_CODE_PROVIDER_PAYTO,
                    ENTITY_IDENTIFIER_CODE_PROVIDER_PAYTO,
                    ENTITY_TYPE_QUALIFIER_PROVIDER_PAYTO,
                    LAST_NAME_ORG_PROVIDER_PAYTO,
                    FIRST_NAME_PROVIDER_PAYTO,
                    MIDDLE_NAME_PROVIDER_PAYTO,
                    NAME_PREFIX_PROVIDER_PAYTO,
                    NAME_SUFFIX_PROVIDER_PAYTO,
                    ID_CODE_QUALIFIER_PROVIDER_PAYTO,
                    ID_CODE_PROVIDER_PAYTO
                )
)
, provider_payto_n3 as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PROVIDER_PAYTO'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_PROVIDER_PAYTO'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_PROVIDER_PAYTO'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*87'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PROVIDER_PAYTO',
                        'ADDRESS_LINE_1_PROVIDER_PAYTO',
                        'ADDRESS_LINE_2_PROVIDER_PAYTO'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    ADDRESS_CODE_PROVIDER_PAYTO_N3,
                    ADDRESS_LINE_1_PROVIDER_PAYTO,
                    ADDRESS_LINE_2_PROVIDER_PAYTO
                )
)
, provider_payto_n4 as
(
    with long as
    (
        select      filtered_hl.response_id,
                    filtered_hl.nth_functional_group,
                    filtered_hl.nth_transaction_set,
                    filtered_hl.index,
                    filtered_hl.hl_index_current,
                    filtered_hl.hl_index_billing_20,
                    filtered_hl.hl_index_subscriber_22,
                    filtered_hl.hl_index_patient_23,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PROVIDER_PAYTO'
                            when    flattened.index = 2   then      'CITY_PROVIDER_PAYTO'
                            when    flattened.index = 3   then      'ST_PROVIDER_PAYTO'
                            when    flattened.index = 4   then      'ZIP_PROVIDER_PAYTO'
                            when    flattened.index = 5   then      'COUNTRY_PROVIDER_PAYTO'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_PROVIDER_PAYTO'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_PROVIDER_PAYTO'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*87'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PROVIDER_PAYTO',
                        'CITY_PROVIDER_PAYTO',
                        'ST_PROVIDER_PAYTO',
                        'ZIP_PROVIDER_PAYTO',
                        'COUNTRY_PROVIDER_PAYTO',
                        'LOCATION_QUALIFIER_PROVIDER_PAYTO',
                        'LOCATION_IDENTIFIER_PROVIDER_PAYTO'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    ADDRESS_CODE_PROVIDER_PAYTO_N4,
                    CITY_PROVIDER_PAYTO,
                    ST_PROVIDER_PAYTO,
                    ZIP_PROVIDER_PAYTO,
                    COUNTRY_PROVIDER_PAYTO,
                    LOCATION_QUALIFIER_PROVIDER_PAYTO,
                    LOCATION_IDENTIFIER_PROVIDER_PAYTO
                )
)
select      header.response_id,
            header.nth_functional_group,
            header.nth_transaction_set,
            header.index,
            header.hl_index_current,
            header.hl_index_billing_20,
            header.hl_index_subscriber_22,
            header.hl_index_patient_23,
            header.hl_prefix_provider,
            header.hl_id_provider,
            header.hl_parent_id_provider,
            header.hl_level_code_provider,
            header.hl_child_code_provider,
            prv.prv_prefix_provider,
            prv.provider_code_provider,
            prv.reference_id_qualifier_provider_prv,
            prv.provider_taxonomy_code_provider,
            nm85.name_code_provider,
            nm85.entity_identifier_code_provider,
            nm85.entity_type_qualifier_provider,
            nm85.last_name_org_provider,
            nm85.first_name_provider,
            nm85.middle_name_provider,
            nm85.name_prefix_provider,
            nm85.name_suffix_provider,
            nm85.id_code_qualifier_provider,
            nm85.id_code_provider,
            n3.address_code_provider_n3,
            n3.address_line_1_provider,
            n3.address_line_2_provider,
            n4.address_code_provider_n4,
            n4.city_provider,
            n4.st_provider,
            n4.zip_provider,
            n4.country_provider,
            n4.location_qualifier_provider,
            n4.location_identifier_provider,
            ref.ref_code_provider,
            ref.reference_id_qualifier_provider_ref,
            ref.reference_id_provider,
            ref.description_provider,
            per.provider_contact_prefix,
            per.contact_function_code_provider,
            per.contact_name_provider,
            per.communication_qualifier_1_provider,
            per.communication_number_1_provider,
            per.communication_qualifier_2_provider,
            per.communication_number_2_provider,
            per.communication_qualifier_3_provider,
            per.communication_number_3_provider,
            payto_nm87.name_code_provider_payto,
            payto_nm87.entity_identifier_code_provider_payto,
            payto_nm87.entity_type_qualifier_provider_payto,
            payto_nm87.last_name_org_provider_payto,
            payto_nm87.first_name_provider_payto,
            payto_nm87.middle_name_provider_payto,
            payto_nm87.name_prefix_provider_payto,
            payto_nm87.name_suffix_provider_payto,
            payto_nm87.id_code_qualifier_provider_payto,
            payto_nm87.id_code_provider_payto,
            payto_n3.address_code_provider_payto_n3,
            payto_n3.address_line_1_provider_payto,
            payto_n3.address_line_2_provider_payto,
            payto_n4.address_code_provider_payto_n4,
            payto_n4.city_provider_payto,
            payto_n4.st_provider_payto,
            payto_n4.zip_provider_payto,
            payto_n4.country_provider_payto,
            payto_n4.location_qualifier_provider_payto,
            payto_n4.location_identifier_provider_payto

from        provider_hl         as header
            left join
                provider_prv        as prv
                on  header.response_id          = prv.response_id
                and header.nth_functional_group = prv.nth_functional_group
                and header.nth_transaction_set  = prv.nth_transaction_set
                and header.hl_index_billing_20  = prv.hl_index_billing_20
            left join
                provider_nm85       as nm85
                on  header.response_id          = nm85.response_id
                and header.nth_functional_group = nm85.nth_functional_group
                and header.nth_transaction_set  = nm85.nth_transaction_set
                and header.hl_index_billing_20  = nm85.hl_index_billing_20
            left join
                provider_n3         as n3
                on  header.response_id          = n3.response_id
                and header.nth_functional_group = n3.nth_functional_group
                and header.nth_transaction_set  = n3.nth_transaction_set
                and header.hl_index_billing_20  = n3.hl_index_billing_20
            left join
                provider_n4         as n4
                on  header.response_id          = n4.response_id
                and header.nth_functional_group = n4.nth_functional_group
                and header.nth_transaction_set  = n4.nth_transaction_set
                and header.hl_index_billing_20  = n4.hl_index_billing_20
            left join
                provider_ref        as ref
                on  header.response_id          = ref.response_id
                and header.nth_functional_group = ref.nth_functional_group
                and header.nth_transaction_set  = ref.nth_transaction_set
                and header.hl_index_billing_20  = ref.hl_index_billing_20
            left join
                provider_per        as per
                on  header.response_id          = per.response_id
                and header.nth_functional_group = per.nth_functional_group
                and header.nth_transaction_set  = per.nth_transaction_set
                and header.hl_index_billing_20  = per.hl_index_billing_20
            left join
                provider_payto_nm87 as payto_nm87
                on  header.response_id          = payto_nm87.response_id
                and header.nth_functional_group = payto_nm87.nth_functional_group
                and header.nth_transaction_set  = payto_nm87.nth_transaction_set
                and header.hl_index_billing_20  = payto_nm87.hl_index_billing_20
            left join
                provider_payto_n3   as payto_n3
                on  header.response_id          = payto_n3.response_id
                and header.nth_functional_group = payto_n3.nth_functional_group
                and header.nth_transaction_set  = payto_n3.nth_transaction_set
                and header.hl_index_billing_20  = payto_n3.hl_index_billing_20
            left join
                provider_payto_n4   as payto_n4
                on  header.response_id          = payto_n4.response_id
                and header.nth_functional_group = payto_n4.nth_functional_group
                and header.nth_transaction_set  = payto_n4.nth_transaction_set
                and header.hl_index_billing_20  = payto_n4.hl_index_billing_20

order by    1,2,3
;