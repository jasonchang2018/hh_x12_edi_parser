create or replace table
    edwprodhh.edi_837i_parser.hl_subscribers
as
with filtered_hl as
(
    select      *
    from        edwprodhh.edi_837i_parser.response_flat
    where       hl_index_billing_20         is not null                                          --0 Pre-Filter
                and hl_index_subscriber_22  is not null
                and hl_index_patient_23     is null
                and claim_index             is null
)
, subscriber_hl as
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

                    case    when    flattened.index = 1   then      'HL_PREFIX_SUBSCRIBER'
                            when    flattened.index = 2   then      'HL_ID_SUBSCRIBER'
                            when    flattened.index = 3   then      'HL_PARENT_ID_SUBSCRIBER'
                            when    flattened.index = 4   then      'HL_LEVEL_CODE_SUBSCRIBER' --20 BILLING, 22 SUBSCRIBER, 23 PATIENT
                            when    flattened.index = 5   then      'HL_CHILD_CODE_SUBSCRIBER' --1 HAS CHILD NODE, 0 NO CHILD NODE
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
                        'HL_PREFIX_SUBSCRIBER',
                        'HL_ID_SUBSCRIBER',
                        'HL_PARENT_ID_SUBSCRIBER',
                        'HL_LEVEL_CODE_SUBSCRIBER',
                        'HL_CHILD_CODE_SUBSCRIBER'
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
                    HL_PREFIX_SUBSCRIBER,
                    HL_ID_SUBSCRIBER,
                    HL_PARENT_ID_SUBSCRIBER,
                    HL_LEVEL_CODE_SUBSCRIBER,
                    HL_CHILD_CODE_SUBSCRIBER
                )
)
, subscriber_sbr as
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

                    case    when    flattened.index = 1   then      'SBR_PREFIX_SUBSCRIBER'
                            when    flattened.index = 2   then      'PAYOR_RESPONSIBILITY_SEQUENCE_SUBSCRIBER'     --P/S/T PRIMARY/SECONDARY/TERTIARY
                            when    flattened.index = 3   then      'INDIVIDUAL_RELATIONSHIP_CODE_SUBSCRIBER'      --18/01/19/20 SELF/SPOUSE/CHILD/EMPLOYEE
                            when    flattened.index = 4   then      'GROUP_NUMBER_SUBSCRIBER'
                            when    flattened.index = 5   then      'GROUP_NAME_SUBSCRIBER'
                            when    flattened.index = 6   then      'INSURANCE_TYPE_CODE_SUBSCRIBER'               --12/13 PPO/HMO
                            when    flattened.index = 7   then      'COORDINATION_OF_BENEFITS_CODE_SUBSCRIBER'
                            when    flattened.index = 8   then      'EMPLOYMENT_CODE_SUBSCRIBER'                   --F/P FULL/PART TIME
                            when    flattened.index = 9   then      'CLAIM_FILING_INDICATOR_CODE_SUBSCRIBER'       --CI/MB COMMERCIAL/MEDICARE PART B
                            when    flattened.index = 10  then      'PATIENT_SIGNATURE_SOURCE_CODE_SUBSCRIBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^SBR.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SBR_PREFIX_SUBSCRIBER',
                        'PAYOR_RESPONSIBILITY_SEQUENCE_SUBSCRIBER',
                        'INDIVIDUAL_RELATIONSHIP_CODE_SUBSCRIBER',
                        'GROUP_NUMBER_SUBSCRIBER',
                        'GROUP_NAME_SUBSCRIBER',
                        'INSURANCE_TYPE_CODE_SUBSCRIBER',
                        'COORDINATION_OF_BENEFITS_CODE_SUBSCRIBER',
                        'EMPLOYMENT_CODE_SUBSCRIBER',
                        'CLAIM_FILING_INDICATOR_CODE_SUBSCRIBER',
                        'PATIENT_SIGNATURE_SOURCE_CODE_SUBSCRIBER'
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
                    SBR_PREFIX_SUBSCRIBEr,
                    PAYOR_RESPONSIBILITY_SEQUENCE_SUBSCRIBER,
                    INDIVIDUAL_RELATIONSHIP_CODE_SUBSCRIBER,
                    GROUP_NUMBER_SUBSCRIBER,
                    GROUP_NAME_SUBSCRIBER,
                    INSURANCE_TYPE_CODE_SUBSCRIBER,
                    COORDINATION_OF_BENEFITS_CODE_SUBSCRIBER,
                    EMPLOYMENT_CODE_SUBSCRIBER,
                    CLAIM_FILING_INDICATOR_CODE_SUBSCRIBER,
                    PATIENT_SIGNATURE_SOURCE_CODE_SUBSCRIBER
                )
)
, subscriber_nmIL as
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

                    case    when    flattened.index = 1   then      'NAME_CODE_SUBSCRIBER'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_SUBSCRIBER'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_SUBSCRIBER'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_SUBSCRIBER'
                            when    flattened.index = 5   then      'FIRST_NAME_SUBSCRIBER'
                            when    flattened.index = 6   then      'MIDDLE_NAME_SUBSCRIBER'
                            when    flattened.index = 7   then      'NAME_PREFIX_SUBSCRIBER'
                            when    flattened.index = 8   then      'NAME_SUFFIX_SUBSCRIBER'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_SUBSCRIBER'
                            when    flattened.index = 10  then      'ID_CODE_SUBSCRIBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^NM1\\*IL.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_SUBSCRIBER',
                        'ENTITY_IDENTIFIER_CODE_SUBSCRIBER',
                        'ENTITY_TYPE_QUALIFIER_SUBSCRIBER',
                        'LAST_NAME_ORG_SUBSCRIBER',
                        'FIRST_NAME_SUBSCRIBER',
                        'MIDDLE_NAME_SUBSCRIBER',
                        'NAME_PREFIX_SUBSCRIBER',
                        'NAME_SUFFIX_SUBSCRIBER',
                        'ID_CODE_QUALIFIER_SUBSCRIBER',
                        'ID_CODE_SUBSCRIBER'
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
                    NAME_CODE_SUBSCRIBER,
                    ENTITY_IDENTIFIER_CODE_SUBSCRIBER,
                    ENTITY_TYPE_QUALIFIER_SUBSCRIBER,
                    LAST_NAME_ORG_SUBSCRIBER,
                    FIRST_NAME_SUBSCRIBER,
                    MIDDLE_NAME_SUBSCRIBER,
                    NAME_PREFIX_SUBSCRIBER,
                    NAME_SUFFIX_SUBSCRIBER,
                    ID_CODE_QUALIFIER_SUBSCRIBER,
                    ID_CODE_SUBSCRIBER
                )
)
, subscriber_n3 as
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

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_SUBSCRIBER'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_SUBSCRIBER'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_SUBSCRIBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*IL'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_SUBSCRIBER',
                        'ADDRESS_LINE_1_SUBSCRIBER',
                        'ADDRESS_LINE_2_SUBSCRIBER'
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
                    ADDRESS_CODE_SUBSCRIBER_NMIL_N3,
                    ADDRESS_LINE_1_SUBSCRIBER,
                    ADDRESS_LINE_2_SUBSCRIBER
                )
)
, subscriber_n4 as
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

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_SUBSCRIBER'
                            when    flattened.index = 2   then      'CITY_SUBSCRIBER'
                            when    flattened.index = 3   then      'ST_SUBSCRIBER'
                            when    flattened.index = 4   then      'ZIP_SUBSCRIBER'
                            when    flattened.index = 5   then      'COUNTRY_SUBSCRIBER'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_SUBSCRIBER'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_SUBSCRIBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*IL'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_SUBSCRIBER',
                        'CITY_SUBSCRIBER',
                        'ST_SUBSCRIBER',
                        'ZIP_SUBSCRIBER',
                        'COUNTRY_SUBSCRIBER',
                        'LOCATION_QUALIFIER_SUBSCRIBER',
                        'LOCATION_IDENTIFIER_SUBSCRIBER'
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
                    ADDRESS_CODE_SUBSCRIBER_NMIL_N4,
                    CITY_SUBSCRIBER,
                    ST_SUBSCRIBER,
                    ZIP_SUBSCRIBER,
                    COUNTRY_SUBSCRIBER,
                    LOCATION_QUALIFIER_SUBSCRIBER,
                    LOCATION_IDENTIFIER_SUBSCRIBER
                )
)
, subscriber_dmg as
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

                    case    when    flattened.index = 1   then      'DMG_PREFIX_SUBSCRIBER'
                            when    flattened.index = 2   then      'FORMAT_QUALIFIER_SUBSCRIBER'
                            when    flattened.index = 3   then      'DOB_SUBSCRIBER'
                            when    flattened.index = 4   then      'GENDER_CODE_SUBSCRIBER'
                            when    flattened.index = 5   then      'MARITAL_STATUS_SUBSCRIBER'
                            when    flattened.index = 6   then      'ETHNICITY_CODE_SUBSCRIBER'
                            when    flattened.index = 7   then      'CITIZENSHIP_CODE_SUBSCRIBER'
                            when    flattened.index = 8   then      'COUNTRY_CODE_SUBSCRIBER'
                            when    flattened.index = 9   then      'VERIFICATION_CODE_SUBSCRIBER'
                            when    flattened.index = 10  then      'QUANTITY_SUBSCRIBER'
                            when    flattened.index = 11  then      'LIST_QUALIFIER_CODE_SUBSCRIBER'
                            when    flattened.index = 12  then      'INDUSTRY_CODE_SUBSCRIBER'
                            end     as value_header,

                    case    when    value_header = 'DOB_SUBSCRIBER'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^DMG.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*IL'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DMG_PREFIX_SUBSCRIBER',
                        'FORMAT_QUALIFIER_SUBSCRIBER',
                        'DOB_SUBSCRIBER',
                        'GENDER_CODE_SUBSCRIBER',
                        'MARITAL_STATUS_SUBSCRIBER',
                        'ETHNICITY_CODE_SUBSCRIBER',
                        'CITIZENSHIP_CODE_SUBSCRIBER',
                        'COUNTRY_CODE_SUBSCRIBER',
                        'VERIFICATION_CODE_SUBSCRIBER',
                        'QUANTITY_SUBSCRIBER',
                        'LIST_QUALIFIER_CODE_SUBSCRIBER',
                        'INDUSTRY_CODE_SUBSCRIBER'
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
                    DMG_PREFIX_SUBSCRIBER,
                    FORMAT_QUALIFIER_SUBSCRIBER,
                    DOB_SUBSCRIBER,
                    GENDER_CODE_SUBSCRIBER,
                    MARITAL_STATUS_SUBSCRIBER,
                    ETHNICITY_CODE_SUBSCRIBER,
                    CITIZENSHIP_CODE_SUBSCRIBER,
                    COUNTRY_CODE_SUBSCRIBER,
                    VERIFICATION_CODE_SUBSCRIBER,
                    QUANTITY_SUBSCRIBER,
                    LIST_QUALIFIER_CODE_SUBSCRIBER,
                    INDUSTRY_CODE_SUBSCRIBER
                )
)
, subscriber_payor_nmPR as
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

                    case    when    flattened.index = 1   then      'NAME_CODE_SUBSCRIBER_PAYOR'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_SUBSCRIBER_PAYOR'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_SUBSCRIBER_PAYOR'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_SUBSCRIBER_PAYOR'
                            when    flattened.index = 5   then      'FIRST_NAME_SUBSCRIBER_PAYOR'
                            when    flattened.index = 6   then      'MIDDLE_NAME_SUBSCRIBER_PAYOR'
                            when    flattened.index = 7   then      'NAME_PREFIX_SUBSCRIBER_PAYOR'
                            when    flattened.index = 8   then      'NAME_SUFFIX_SUBSCRIBER_PAYOR'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_SUBSCRIBER_PAYOR'
                            when    flattened.index = 10  then      'ID_CODE_SUBSCRIBER_PAYOR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^NM1\\*PR.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_SUBSCRIBER_PAYOR',
                        'ENTITY_IDENTIFIER_CODE_SUBSCRIBER_PAYOR',
                        'ENTITY_TYPE_QUALIFIER_SUBSCRIBER_PAYOR',
                        'LAST_NAME_ORG_SUBSCRIBER_PAYOR',
                        'FIRST_NAME_SUBSCRIBER_PAYOR',
                        'MIDDLE_NAME_SUBSCRIBER_PAYOR',
                        'NAME_PREFIX_SUBSCRIBER_PAYOR',
                        'NAME_SUFFIX_SUBSCRIBER_PAYOR',
                        'ID_CODE_QUALIFIER_SUBSCRIBER_PAYOR',
                        'ID_CODE_SUBSCRIBER_PAYOR'
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
                    NAME_CODE_SUBSCRIBER_PAYOR,
                    ENTITY_IDENTIFIER_CODE_SUBSCRIBER_PAYOR,
                    ENTITY_TYPE_QUALIFIER_SUBSCRIBER_PAYOR,
                    LAST_NAME_ORG_SUBSCRIBER_PAYOR,
                    FIRST_NAME_SUBSCRIBER_PAYOR,
                    MIDDLE_NAME_SUBSCRIBER_PAYOR,
                    NAME_PREFIX_SUBSCRIBER_PAYOR,
                    NAME_SUFFIX_SUBSCRIBER_PAYOR,
                    ID_CODE_QUALIFIER_SUBSCRIBER_PAYOR,
                    ID_CODE_SUBSCRIBER_PAYOR
                )
)
, subscriber_payor_n3 as
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

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_SUBSCRIBER_PAYOR'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_SUBSCRIBER_PAYOR'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_SUBSCRIBER_PAYOR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_SUBSCRIBER_PAYOR',
                        'ADDRESS_LINE_1_SUBSCRIBER_PAYOR',
                        'ADDRESS_LINE_2_SUBSCRIBER_PAYOR'
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
                    ADDRESS_CODE_SUBSCRIBER_PAYOR_NMPR_N3,
                    ADDRESS_LINE_1_SUBSCRIBER_PAYOR,
                    ADDRESS_LINE_2_SUBSCRIBER_PAYOR
                )
)
, subscriber_payor_n4 as
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

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_SUBSCRIBER_PAYOR'
                            when    flattened.index = 2   then      'CITY_SUBSCRIBER_PAYOR'
                            when    flattened.index = 3   then      'ST_SUBSCRIBER_PAYOR'
                            when    flattened.index = 4   then      'ZIP_SUBSCRIBER_PAYOR'
                            when    flattened.index = 5   then      'COUNTRY_SUBSCRIBER_PAYOR'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_SUBSCRIBER_PAYOR'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_SUBSCRIBER_PAYOR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_SUBSCRIBER_PAYOR',
                        'CITY_SUBSCRIBER_PAYOR',
                        'ST_SUBSCRIBER_PAYOR',
                        'ZIP_SUBSCRIBER_PAYOR',
                        'COUNTRY_SUBSCRIBER_PAYOR',
                        'LOCATION_QUALIFIER_SUBSCRIBER_PAYOR',
                        'LOCATION_IDENTIFIER_SUBSCRIBER_PAYOR'
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
                    ADDRESS_CODE_SUBSCRIBER_PAYOR_NMPR_N4,
                    CITY_SUBSCRIBER_PAYOR,
                    ST_SUBSCRIBER_PAYOR,
                    ZIP_SUBSCRIBER_PAYOR,
                    COUNTRY_SUBSCRIBER_PAYOR,
                    LOCATION_QUALIFIER_SUBSCRIBER_PAYOR,
                    LOCATION_IDENTIFIER_SUBSCRIBER_PAYOR
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
            header.hl_prefix_subscriber,
            header.hl_id_subscriber,
            header.hl_parent_id_subscriber,
            header.hl_level_code_subscriber,
            header.hl_child_code_subscriber,
            sbr.sbr_prefix_subscriber,
            sbr.payor_responsibility_sequence_subscriber,
            sbr.individual_relationship_code_subscriber,
            sbr.group_number_subscriber,
            sbr.group_name_subscriber,
            sbr.insurance_type_code_subscriber,
            sbr.coordination_of_benefits_code_subscriber,
            sbr.employment_code_subscriber,
            sbr.claim_filing_indicator_code_subscriber,
            sbr.patient_signature_source_code_subscriber,
            nmIL.name_code_subscriber,
            nmIL.entity_identifier_code_subscriber,
            nmIL.entity_type_qualifier_subscriber,
            nmIL.last_name_org_subscriber,
            nmIL.first_name_subscriber,
            nmIL.middle_name_subscriber,
            nmIL.name_prefix_subscriber,
            nmIL.name_suffix_subscriber,
            nmIL.id_code_qualifier_subscriber,
            nmIL.id_code_subscriber,
            nmIL_n3.address_code_subscriber_nmIL_n3,
            nmIL_n3.address_line_1_subscriber,
            nmIL_n3.address_line_2_subscriber,
            nmIL_n4.address_code_subscriber_nmIL_n4,
            nmIL_n4.city_subscriber,
            nmIL_n4.st_subscriber,
            nmIL_n4.zip_subscriber,
            nmIL_n4.country_subscriber,
            nmIL_n4.location_qualifier_subscriber,
            nmIL_n4.location_identifier_subscriber,
            dmg.dmg_prefix_subscriber,
            dmg.format_qualifier_subscriber,
            dmg.dob_subscriber,
            dmg.gender_code_subscriber,
            dmg.marital_status_subscriber,
            dmg.ethnicity_code_subscriber,
            dmg.citizenship_code_subscriber,
            dmg.country_code_subscriber,
            dmg.verification_code_subscriber,
            dmg.quantity_subscriber,
            dmg.list_qualifier_code_subscriber,
            dmg.industry_code_subscriber,
            nmPR.name_code_subscriber_payor,
            nmPR.entity_identifier_code_subscriber_payor,
            nmPR.entity_type_qualifier_subscriber_payor,
            nmPR.last_name_org_subscriber_payor,
            nmPR.first_name_subscriber_payor,
            nmPR.middle_name_subscriber_payor,
            nmPR.name_prefix_subscriber_payor,
            nmPR.name_suffix_subscriber_payor,
            nmPR.id_code_qualifier_subscriber_payor,
            nmPR.id_code_subscriber_payor,
            nmPR_n3.address_code_subscriber_payor_nmPR_n3,
            nmPR_n3.address_line_1_subscriber_payor,
            nmPR_n3.address_line_2_subscriber_payor,
            nmPR_n4.address_code_subscriber_payor_nmPR_n4,
            nmPR_n4.city_subscriber_payor,
            nmPR_n4.st_subscriber_payor,
            nmPR_n4.zip_subscriber_payor,
            nmPR_n4.country_subscriber_payor,
            nmPR_n4.location_qualifier_subscriber_payor,
            nmPR_n4.location_identifier_subscriber_payor

from        subscriber_hl           as header
            left join
                subscriber_sbr          as sbr
                on  header.response_id              = sbr.response_id
                and header.nth_functional_group     = sbr.nth_functional_group
                and header.nth_transaction_set      = sbr.nth_transaction_set
                and header.hl_index_subscriber_22   = sbr.hl_index_subscriber_22
            left join
                subscriber_nmIL         as nmIL
                on  header.response_id              = nmIL.response_id
                and header.nth_functional_group     = nmIL.nth_functional_group
                and header.nth_transaction_set      = nmIL.nth_transaction_set
                and header.hl_index_subscriber_22   = nmIL.hl_index_subscriber_22
            left join
                subscriber_n3           as nmIL_n3
                on  header.response_id              = nmIL_n3.response_id
                and header.nth_functional_group     = nmIL_n3.nth_functional_group
                and header.nth_transaction_set      = nmIL_n3.nth_transaction_set
                and header.hl_index_subscriber_22   = nmIL_n3.hl_index_subscriber_22
            left join
                subscriber_n4           as nmIL_n4
                on  header.response_id              = nmIL_n4.response_id
                and header.nth_functional_group     = nmIL_n4.nth_functional_group
                and header.nth_transaction_set      = nmIL_n4.nth_transaction_set
                and header.hl_index_subscriber_22   = nmIL_n4.hl_index_subscriber_22
            left join
                subscriber_dmg          as dmg
                on  header.response_id              = dmg.response_id
                and header.nth_functional_group     = dmg.nth_functional_group
                and header.nth_transaction_set      = dmg.nth_transaction_set
                and header.hl_index_subscriber_22   = dmg.hl_index_subscriber_22
            left join
                subscriber_payor_nmPR   as nmPR
                on  header.response_id              = nmPR.response_id
                and header.nth_functional_group     = nmPR.nth_functional_group
                and header.nth_transaction_set      = nmPR.nth_transaction_set
                and header.hl_index_subscriber_22   = nmPR.hl_index_subscriber_22
            left join
                subscriber_payor_n3     as nmPR_n3
                on  header.response_id              = nmPR_n3.response_id
                and header.nth_functional_group     = nmPR_n3.nth_functional_group
                and header.nth_transaction_set      = nmPR_n3.nth_transaction_set
                and header.hl_index_subscriber_22   = nmPR_n3.hl_index_subscriber_22
            left join
                subscriber_payor_n4     as nmPR_n4
                on  header.response_id              = nmPR_n4.response_id
                and header.nth_functional_group     = nmPR_n4.nth_functional_group
                and header.nth_transaction_set      = nmPR_n4.nth_transaction_set
                and header.hl_index_subscriber_22   = nmPR_n4.hl_index_subscriber_22
                
order by    1,2,3
;



create or replace task
    edwprodhh.edi_837i_parser.insert_hl_subscribers
    warehouse = analysis_wh
    after edwprodhh.edi_837i_parser.insert_response_flat
as
insert into
    edwprodhh.edi_837i_parser.hl_subscribers
(
    RESPONSE_ID,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    INDEX,
    HL_INDEX_CURRENT,
    HL_INDEX_BILLING_20,
    HL_INDEX_SUBSCRIBER_22,
    HL_INDEX_PATIENT_23,
    HL_PREFIX_SUBSCRIBER,
    HL_ID_SUBSCRIBER,
    HL_PARENT_ID_SUBSCRIBER,
    HL_LEVEL_CODE_SUBSCRIBER,
    HL_CHILD_CODE_SUBSCRIBER,
    SBR_PREFIX_SUBSCRIBER,
    PAYOR_RESPONSIBILITY_SEQUENCE_SUBSCRIBER,
    INDIVIDUAL_RELATIONSHIP_CODE_SUBSCRIBER,
    GROUP_NUMBER_SUBSCRIBER,
    GROUP_NAME_SUBSCRIBER,
    INSURANCE_TYPE_CODE_SUBSCRIBER,
    COORDINATION_OF_BENEFITS_CODE_SUBSCRIBER,
    EMPLOYMENT_CODE_SUBSCRIBER,
    CLAIM_FILING_INDICATOR_CODE_SUBSCRIBER,
    PATIENT_SIGNATURE_SOURCE_CODE_SUBSCRIBER,
    NAME_CODE_SUBSCRIBER,
    ENTITY_IDENTIFIER_CODE_SUBSCRIBER,
    ENTITY_TYPE_QUALIFIER_SUBSCRIBER,
    LAST_NAME_ORG_SUBSCRIBER,
    FIRST_NAME_SUBSCRIBER,
    MIDDLE_NAME_SUBSCRIBER,
    NAME_PREFIX_SUBSCRIBER,
    NAME_SUFFIX_SUBSCRIBER,
    ID_CODE_QUALIFIER_SUBSCRIBER,
    ID_CODE_SUBSCRIBER,
    ADDRESS_CODE_SUBSCRIBER_NMIL_N3,
    ADDRESS_LINE_1_SUBSCRIBER,
    ADDRESS_LINE_2_SUBSCRIBER,
    ADDRESS_CODE_SUBSCRIBER_NMIL_N4,
    CITY_SUBSCRIBER,
    ST_SUBSCRIBER,
    ZIP_SUBSCRIBER,
    COUNTRY_SUBSCRIBER,
    LOCATION_QUALIFIER_SUBSCRIBER,
    LOCATION_IDENTIFIER_SUBSCRIBER,
    DMG_PREFIX_SUBSCRIBER,
    FORMAT_QUALIFIER_SUBSCRIBER,
    DOB_SUBSCRIBER,
    GENDER_CODE_SUBSCRIBER,
    MARITAL_STATUS_SUBSCRIBER,
    ETHNICITY_CODE_SUBSCRIBER,
    CITIZENSHIP_CODE_SUBSCRIBER,
    COUNTRY_CODE_SUBSCRIBER,
    VERIFICATION_CODE_SUBSCRIBER,
    QUANTITY_SUBSCRIBER,
    LIST_QUALIFIER_CODE_SUBSCRIBER,
    INDUSTRY_CODE_SUBSCRIBER,
    NAME_CODE_SUBSCRIBER_PAYOR,
    ENTITY_IDENTIFIER_CODE_SUBSCRIBER_PAYOR,
    ENTITY_TYPE_QUALIFIER_SUBSCRIBER_PAYOR,
    LAST_NAME_ORG_SUBSCRIBER_PAYOR,
    FIRST_NAME_SUBSCRIBER_PAYOR,
    MIDDLE_NAME_SUBSCRIBER_PAYOR,
    NAME_PREFIX_SUBSCRIBER_PAYOR,
    NAME_SUFFIX_SUBSCRIBER_PAYOR,
    ID_CODE_QUALIFIER_SUBSCRIBER_PAYOR,
    ID_CODE_SUBSCRIBER_PAYOR,
    ADDRESS_CODE_SUBSCRIBER_PAYOR_NMPR_N3,
    ADDRESS_LINE_1_SUBSCRIBER_PAYOR,
    ADDRESS_LINE_2_SUBSCRIBER_PAYOR,
    ADDRESS_CODE_SUBSCRIBER_PAYOR_NMPR_N4,
    CITY_SUBSCRIBER_PAYOR,
    ST_SUBSCRIBER_PAYOR,
    ZIP_SUBSCRIBER_PAYOR,
    COUNTRY_SUBSCRIBER_PAYOR,
    LOCATION_QUALIFIER_SUBSCRIBER_PAYOR,
    LOCATION_IDENTIFIER_SUBSCRIBER_PAYOR
)
with filtered_hl as
(
    select      *
    from        edwprodhh.edi_837i_parser.response_flat
    where       hl_index_billing_20         is not null                                          --0 Pre-Filter
                and hl_index_subscriber_22  is not null
                and hl_index_patient_23     is null
                and claim_index             is null
                and response_id not in (select response_id from edwprodhh.edi_837i_parser.hl_subscribers)
)
, subscriber_hl as
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

                    case    when    flattened.index = 1   then      'HL_PREFIX_SUBSCRIBER'
                            when    flattened.index = 2   then      'HL_ID_SUBSCRIBER'
                            when    flattened.index = 3   then      'HL_PARENT_ID_SUBSCRIBER'
                            when    flattened.index = 4   then      'HL_LEVEL_CODE_SUBSCRIBER' --20 BILLING, 22 SUBSCRIBER, 23 PATIENT
                            when    flattened.index = 5   then      'HL_CHILD_CODE_SUBSCRIBER' --1 HAS CHILD NODE, 0 NO CHILD NODE
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
                        'HL_PREFIX_SUBSCRIBER',
                        'HL_ID_SUBSCRIBER',
                        'HL_PARENT_ID_SUBSCRIBER',
                        'HL_LEVEL_CODE_SUBSCRIBER',
                        'HL_CHILD_CODE_SUBSCRIBER'
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
                    HL_PREFIX_SUBSCRIBER,
                    HL_ID_SUBSCRIBER,
                    HL_PARENT_ID_SUBSCRIBER,
                    HL_LEVEL_CODE_SUBSCRIBER,
                    HL_CHILD_CODE_SUBSCRIBER
                )
)
, subscriber_sbr as
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

                    case    when    flattened.index = 1   then      'SBR_PREFIX_SUBSCRIBER'
                            when    flattened.index = 2   then      'PAYOR_RESPONSIBILITY_SEQUENCE_SUBSCRIBER'     --P/S/T PRIMARY/SECONDARY/TERTIARY
                            when    flattened.index = 3   then      'INDIVIDUAL_RELATIONSHIP_CODE_SUBSCRIBER'      --18/01/19/20 SELF/SPOUSE/CHILD/EMPLOYEE
                            when    flattened.index = 4   then      'GROUP_NUMBER_SUBSCRIBER'
                            when    flattened.index = 5   then      'GROUP_NAME_SUBSCRIBER'
                            when    flattened.index = 6   then      'INSURANCE_TYPE_CODE_SUBSCRIBER'               --12/13 PPO/HMO
                            when    flattened.index = 7   then      'COORDINATION_OF_BENEFITS_CODE_SUBSCRIBER'
                            when    flattened.index = 8   then      'EMPLOYMENT_CODE_SUBSCRIBER'                   --F/P FULL/PART TIME
                            when    flattened.index = 9   then      'CLAIM_FILING_INDICATOR_CODE_SUBSCRIBER'       --CI/MB COMMERCIAL/MEDICARE PART B
                            when    flattened.index = 10  then      'PATIENT_SIGNATURE_SOURCE_CODE_SUBSCRIBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^SBR.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SBR_PREFIX_SUBSCRIBER',
                        'PAYOR_RESPONSIBILITY_SEQUENCE_SUBSCRIBER',
                        'INDIVIDUAL_RELATIONSHIP_CODE_SUBSCRIBER',
                        'GROUP_NUMBER_SUBSCRIBER',
                        'GROUP_NAME_SUBSCRIBER',
                        'INSURANCE_TYPE_CODE_SUBSCRIBER',
                        'COORDINATION_OF_BENEFITS_CODE_SUBSCRIBER',
                        'EMPLOYMENT_CODE_SUBSCRIBER',
                        'CLAIM_FILING_INDICATOR_CODE_SUBSCRIBER',
                        'PATIENT_SIGNATURE_SOURCE_CODE_SUBSCRIBER'
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
                    SBR_PREFIX_SUBSCRIBEr,
                    PAYOR_RESPONSIBILITY_SEQUENCE_SUBSCRIBER,
                    INDIVIDUAL_RELATIONSHIP_CODE_SUBSCRIBER,
                    GROUP_NUMBER_SUBSCRIBER,
                    GROUP_NAME_SUBSCRIBER,
                    INSURANCE_TYPE_CODE_SUBSCRIBER,
                    COORDINATION_OF_BENEFITS_CODE_SUBSCRIBER,
                    EMPLOYMENT_CODE_SUBSCRIBER,
                    CLAIM_FILING_INDICATOR_CODE_SUBSCRIBER,
                    PATIENT_SIGNATURE_SOURCE_CODE_SUBSCRIBER
                )
)
, subscriber_nmIL as
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

                    case    when    flattened.index = 1   then      'NAME_CODE_SUBSCRIBER'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_SUBSCRIBER'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_SUBSCRIBER'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_SUBSCRIBER'
                            when    flattened.index = 5   then      'FIRST_NAME_SUBSCRIBER'
                            when    flattened.index = 6   then      'MIDDLE_NAME_SUBSCRIBER'
                            when    flattened.index = 7   then      'NAME_PREFIX_SUBSCRIBER'
                            when    flattened.index = 8   then      'NAME_SUFFIX_SUBSCRIBER'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_SUBSCRIBER'
                            when    flattened.index = 10  then      'ID_CODE_SUBSCRIBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^NM1\\*IL.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_SUBSCRIBER',
                        'ENTITY_IDENTIFIER_CODE_SUBSCRIBER',
                        'ENTITY_TYPE_QUALIFIER_SUBSCRIBER',
                        'LAST_NAME_ORG_SUBSCRIBER',
                        'FIRST_NAME_SUBSCRIBER',
                        'MIDDLE_NAME_SUBSCRIBER',
                        'NAME_PREFIX_SUBSCRIBER',
                        'NAME_SUFFIX_SUBSCRIBER',
                        'ID_CODE_QUALIFIER_SUBSCRIBER',
                        'ID_CODE_SUBSCRIBER'
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
                    NAME_CODE_SUBSCRIBER,
                    ENTITY_IDENTIFIER_CODE_SUBSCRIBER,
                    ENTITY_TYPE_QUALIFIER_SUBSCRIBER,
                    LAST_NAME_ORG_SUBSCRIBER,
                    FIRST_NAME_SUBSCRIBER,
                    MIDDLE_NAME_SUBSCRIBER,
                    NAME_PREFIX_SUBSCRIBER,
                    NAME_SUFFIX_SUBSCRIBER,
                    ID_CODE_QUALIFIER_SUBSCRIBER,
                    ID_CODE_SUBSCRIBER
                )
)
, subscriber_n3 as
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

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_SUBSCRIBER'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_SUBSCRIBER'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_SUBSCRIBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*IL'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_SUBSCRIBER',
                        'ADDRESS_LINE_1_SUBSCRIBER',
                        'ADDRESS_LINE_2_SUBSCRIBER'
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
                    ADDRESS_CODE_SUBSCRIBER_NMIL_N3,
                    ADDRESS_LINE_1_SUBSCRIBER,
                    ADDRESS_LINE_2_SUBSCRIBER
                )
)
, subscriber_n4 as
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

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_SUBSCRIBER'
                            when    flattened.index = 2   then      'CITY_SUBSCRIBER'
                            when    flattened.index = 3   then      'ST_SUBSCRIBER'
                            when    flattened.index = 4   then      'ZIP_SUBSCRIBER'
                            when    flattened.index = 5   then      'COUNTRY_SUBSCRIBER'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_SUBSCRIBER'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_SUBSCRIBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*IL'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_SUBSCRIBER',
                        'CITY_SUBSCRIBER',
                        'ST_SUBSCRIBER',
                        'ZIP_SUBSCRIBER',
                        'COUNTRY_SUBSCRIBER',
                        'LOCATION_QUALIFIER_SUBSCRIBER',
                        'LOCATION_IDENTIFIER_SUBSCRIBER'
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
                    ADDRESS_CODE_SUBSCRIBER_NMIL_N4,
                    CITY_SUBSCRIBER,
                    ST_SUBSCRIBER,
                    ZIP_SUBSCRIBER,
                    COUNTRY_SUBSCRIBER,
                    LOCATION_QUALIFIER_SUBSCRIBER,
                    LOCATION_IDENTIFIER_SUBSCRIBER
                )
)
, subscriber_dmg as
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

                    case    when    flattened.index = 1   then      'DMG_PREFIX_SUBSCRIBER'
                            when    flattened.index = 2   then      'FORMAT_QUALIFIER_SUBSCRIBER'
                            when    flattened.index = 3   then      'DOB_SUBSCRIBER'
                            when    flattened.index = 4   then      'GENDER_CODE_SUBSCRIBER'
                            when    flattened.index = 5   then      'MARITAL_STATUS_SUBSCRIBER'
                            when    flattened.index = 6   then      'ETHNICITY_CODE_SUBSCRIBER'
                            when    flattened.index = 7   then      'CITIZENSHIP_CODE_SUBSCRIBER'
                            when    flattened.index = 8   then      'COUNTRY_CODE_SUBSCRIBER'
                            when    flattened.index = 9   then      'VERIFICATION_CODE_SUBSCRIBER'
                            when    flattened.index = 10  then      'QUANTITY_SUBSCRIBER'
                            when    flattened.index = 11  then      'LIST_QUALIFIER_CODE_SUBSCRIBER'
                            when    flattened.index = 12  then      'INDUSTRY_CODE_SUBSCRIBER'
                            end     as value_header,

                    case    when    value_header = 'DOB_SUBSCRIBER'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^DMG.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*IL'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DMG_PREFIX_SUBSCRIBER',
                        'FORMAT_QUALIFIER_SUBSCRIBER',
                        'DOB_SUBSCRIBER',
                        'GENDER_CODE_SUBSCRIBER',
                        'MARITAL_STATUS_SUBSCRIBER',
                        'ETHNICITY_CODE_SUBSCRIBER',
                        'CITIZENSHIP_CODE_SUBSCRIBER',
                        'COUNTRY_CODE_SUBSCRIBER',
                        'VERIFICATION_CODE_SUBSCRIBER',
                        'QUANTITY_SUBSCRIBER',
                        'LIST_QUALIFIER_CODE_SUBSCRIBER',
                        'INDUSTRY_CODE_SUBSCRIBER'
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
                    DMG_PREFIX_SUBSCRIBER,
                    FORMAT_QUALIFIER_SUBSCRIBER,
                    DOB_SUBSCRIBER,
                    GENDER_CODE_SUBSCRIBER,
                    MARITAL_STATUS_SUBSCRIBER,
                    ETHNICITY_CODE_SUBSCRIBER,
                    CITIZENSHIP_CODE_SUBSCRIBER,
                    COUNTRY_CODE_SUBSCRIBER,
                    VERIFICATION_CODE_SUBSCRIBER,
                    QUANTITY_SUBSCRIBER,
                    LIST_QUALIFIER_CODE_SUBSCRIBER,
                    INDUSTRY_CODE_SUBSCRIBER
                )
)
, subscriber_payor_nmPR as
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

                    case    when    flattened.index = 1   then      'NAME_CODE_SUBSCRIBER_PAYOR'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_SUBSCRIBER_PAYOR'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_SUBSCRIBER_PAYOR'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_SUBSCRIBER_PAYOR'
                            when    flattened.index = 5   then      'FIRST_NAME_SUBSCRIBER_PAYOR'
                            when    flattened.index = 6   then      'MIDDLE_NAME_SUBSCRIBER_PAYOR'
                            when    flattened.index = 7   then      'NAME_PREFIX_SUBSCRIBER_PAYOR'
                            when    flattened.index = 8   then      'NAME_SUFFIX_SUBSCRIBER_PAYOR'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_SUBSCRIBER_PAYOR'
                            when    flattened.index = 10  then      'ID_CODE_SUBSCRIBER_PAYOR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^NM1\\*PR.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_SUBSCRIBER_PAYOR',
                        'ENTITY_IDENTIFIER_CODE_SUBSCRIBER_PAYOR',
                        'ENTITY_TYPE_QUALIFIER_SUBSCRIBER_PAYOR',
                        'LAST_NAME_ORG_SUBSCRIBER_PAYOR',
                        'FIRST_NAME_SUBSCRIBER_PAYOR',
                        'MIDDLE_NAME_SUBSCRIBER_PAYOR',
                        'NAME_PREFIX_SUBSCRIBER_PAYOR',
                        'NAME_SUFFIX_SUBSCRIBER_PAYOR',
                        'ID_CODE_QUALIFIER_SUBSCRIBER_PAYOR',
                        'ID_CODE_SUBSCRIBER_PAYOR'
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
                    NAME_CODE_SUBSCRIBER_PAYOR,
                    ENTITY_IDENTIFIER_CODE_SUBSCRIBER_PAYOR,
                    ENTITY_TYPE_QUALIFIER_SUBSCRIBER_PAYOR,
                    LAST_NAME_ORG_SUBSCRIBER_PAYOR,
                    FIRST_NAME_SUBSCRIBER_PAYOR,
                    MIDDLE_NAME_SUBSCRIBER_PAYOR,
                    NAME_PREFIX_SUBSCRIBER_PAYOR,
                    NAME_SUFFIX_SUBSCRIBER_PAYOR,
                    ID_CODE_QUALIFIER_SUBSCRIBER_PAYOR,
                    ID_CODE_SUBSCRIBER_PAYOR
                )
)
, subscriber_payor_n3 as
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

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_SUBSCRIBER_PAYOR'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_SUBSCRIBER_PAYOR'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_SUBSCRIBER_PAYOR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_SUBSCRIBER_PAYOR',
                        'ADDRESS_LINE_1_SUBSCRIBER_PAYOR',
                        'ADDRESS_LINE_2_SUBSCRIBER_PAYOR'
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
                    ADDRESS_CODE_SUBSCRIBER_PAYOR_NMPR_N3,
                    ADDRESS_LINE_1_SUBSCRIBER_PAYOR,
                    ADDRESS_LINE_2_SUBSCRIBER_PAYOR
                )
)
, subscriber_payor_n4 as
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

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_SUBSCRIBER_PAYOR'
                            when    flattened.index = 2   then      'CITY_SUBSCRIBER_PAYOR'
                            when    flattened.index = 3   then      'ST_SUBSCRIBER_PAYOR'
                            when    flattened.index = 4   then      'ZIP_SUBSCRIBER_PAYOR'
                            when    flattened.index = 5   then      'COUNTRY_SUBSCRIBER_PAYOR'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_SUBSCRIBER_PAYOR'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_SUBSCRIBER_PAYOR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_SUBSCRIBER_PAYOR',
                        'CITY_SUBSCRIBER_PAYOR',
                        'ST_SUBSCRIBER_PAYOR',
                        'ZIP_SUBSCRIBER_PAYOR',
                        'COUNTRY_SUBSCRIBER_PAYOR',
                        'LOCATION_QUALIFIER_SUBSCRIBER_PAYOR',
                        'LOCATION_IDENTIFIER_SUBSCRIBER_PAYOR'
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
                    ADDRESS_CODE_SUBSCRIBER_PAYOR_NMPR_N4,
                    CITY_SUBSCRIBER_PAYOR,
                    ST_SUBSCRIBER_PAYOR,
                    ZIP_SUBSCRIBER_PAYOR,
                    COUNTRY_SUBSCRIBER_PAYOR,
                    LOCATION_QUALIFIER_SUBSCRIBER_PAYOR,
                    LOCATION_IDENTIFIER_SUBSCRIBER_PAYOR
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
            header.hl_prefix_subscriber,
            header.hl_id_subscriber,
            header.hl_parent_id_subscriber,
            header.hl_level_code_subscriber,
            header.hl_child_code_subscriber,
            sbr.sbr_prefix_subscriber,
            sbr.payor_responsibility_sequence_subscriber,
            sbr.individual_relationship_code_subscriber,
            sbr.group_number_subscriber,
            sbr.group_name_subscriber,
            sbr.insurance_type_code_subscriber,
            sbr.coordination_of_benefits_code_subscriber,
            sbr.employment_code_subscriber,
            sbr.claim_filing_indicator_code_subscriber,
            sbr.patient_signature_source_code_subscriber,
            nmIL.name_code_subscriber,
            nmIL.entity_identifier_code_subscriber,
            nmIL.entity_type_qualifier_subscriber,
            nmIL.last_name_org_subscriber,
            nmIL.first_name_subscriber,
            nmIL.middle_name_subscriber,
            nmIL.name_prefix_subscriber,
            nmIL.name_suffix_subscriber,
            nmIL.id_code_qualifier_subscriber,
            nmIL.id_code_subscriber,
            nmIL_n3.address_code_subscriber_nmIL_n3,
            nmIL_n3.address_line_1_subscriber,
            nmIL_n3.address_line_2_subscriber,
            nmIL_n4.address_code_subscriber_nmIL_n4,
            nmIL_n4.city_subscriber,
            nmIL_n4.st_subscriber,
            nmIL_n4.zip_subscriber,
            nmIL_n4.country_subscriber,
            nmIL_n4.location_qualifier_subscriber,
            nmIL_n4.location_identifier_subscriber,
            dmg.dmg_prefix_subscriber,
            dmg.format_qualifier_subscriber,
            dmg.dob_subscriber,
            dmg.gender_code_subscriber,
            dmg.marital_status_subscriber,
            dmg.ethnicity_code_subscriber,
            dmg.citizenship_code_subscriber,
            dmg.country_code_subscriber,
            dmg.verification_code_subscriber,
            dmg.quantity_subscriber,
            dmg.list_qualifier_code_subscriber,
            dmg.industry_code_subscriber,
            nmPR.name_code_subscriber_payor,
            nmPR.entity_identifier_code_subscriber_payor,
            nmPR.entity_type_qualifier_subscriber_payor,
            nmPR.last_name_org_subscriber_payor,
            nmPR.first_name_subscriber_payor,
            nmPR.middle_name_subscriber_payor,
            nmPR.name_prefix_subscriber_payor,
            nmPR.name_suffix_subscriber_payor,
            nmPR.id_code_qualifier_subscriber_payor,
            nmPR.id_code_subscriber_payor,
            nmPR_n3.address_code_subscriber_payor_nmPR_n3,
            nmPR_n3.address_line_1_subscriber_payor,
            nmPR_n3.address_line_2_subscriber_payor,
            nmPR_n4.address_code_subscriber_payor_nmPR_n4,
            nmPR_n4.city_subscriber_payor,
            nmPR_n4.st_subscriber_payor,
            nmPR_n4.zip_subscriber_payor,
            nmPR_n4.country_subscriber_payor,
            nmPR_n4.location_qualifier_subscriber_payor,
            nmPR_n4.location_identifier_subscriber_payor

from        subscriber_hl           as header
            left join
                subscriber_sbr          as sbr
                on  header.response_id              = sbr.response_id
                and header.nth_functional_group     = sbr.nth_functional_group
                and header.nth_transaction_set      = sbr.nth_transaction_set
                and header.hl_index_subscriber_22   = sbr.hl_index_subscriber_22
            left join
                subscriber_nmIL         as nmIL
                on  header.response_id              = nmIL.response_id
                and header.nth_functional_group     = nmIL.nth_functional_group
                and header.nth_transaction_set      = nmIL.nth_transaction_set
                and header.hl_index_subscriber_22   = nmIL.hl_index_subscriber_22
            left join
                subscriber_n3           as nmIL_n3
                on  header.response_id              = nmIL_n3.response_id
                and header.nth_functional_group     = nmIL_n3.nth_functional_group
                and header.nth_transaction_set      = nmIL_n3.nth_transaction_set
                and header.hl_index_subscriber_22   = nmIL_n3.hl_index_subscriber_22
            left join
                subscriber_n4           as nmIL_n4
                on  header.response_id              = nmIL_n4.response_id
                and header.nth_functional_group     = nmIL_n4.nth_functional_group
                and header.nth_transaction_set      = nmIL_n4.nth_transaction_set
                and header.hl_index_subscriber_22   = nmIL_n4.hl_index_subscriber_22
            left join
                subscriber_dmg          as dmg
                on  header.response_id              = dmg.response_id
                and header.nth_functional_group     = dmg.nth_functional_group
                and header.nth_transaction_set      = dmg.nth_transaction_set
                and header.hl_index_subscriber_22   = dmg.hl_index_subscriber_22
            left join
                subscriber_payor_nmPR   as nmPR
                on  header.response_id              = nmPR.response_id
                and header.nth_functional_group     = nmPR.nth_functional_group
                and header.nth_transaction_set      = nmPR.nth_transaction_set
                and header.hl_index_subscriber_22   = nmPR.hl_index_subscriber_22
            left join
                subscriber_payor_n3     as nmPR_n3
                on  header.response_id              = nmPR_n3.response_id
                and header.nth_functional_group     = nmPR_n3.nth_functional_group
                and header.nth_transaction_set      = nmPR_n3.nth_transaction_set
                and header.hl_index_subscriber_22   = nmPR_n3.hl_index_subscriber_22
            left join
                subscriber_payor_n4     as nmPR_n4
                on  header.response_id              = nmPR_n4.response_id
                and header.nth_functional_group     = nmPR_n4.nth_functional_group
                and header.nth_transaction_set      = nmPR_n4.nth_transaction_set
                and header.hl_index_subscriber_22   = nmPR_n4.hl_index_subscriber_22
                
order by    1,2,3
;