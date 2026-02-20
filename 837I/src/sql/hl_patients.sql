create or replace table
    edwprodhh.edi_837i_parser.hl_patients
as
with filtered_hl as
(
    select      *
    from        edwprodhh.edi_837i_parser.response_flat as filtered
    where       hl_index_billing_20         is not null                                          --0 Pre-Filter
                and hl_index_subscriber_22  is not null
                and hl_index_patient_23     is not null
                and claim_index             is null
)
, patient_hl as
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

                    case    when    flattened.index = 1   then      'HL_PREFIX_PATIENT'
                            when    flattened.index = 2   then      'HL_ID_PATIENT'
                            when    flattened.index = 3   then      'HL_PARENT_ID_PATIENT'
                            when    flattened.index = 4   then      'HL_LEVEL_CODE_PATIENT' --20 BILLING, 22 SUBSCRIBER, 23 PATIENT
                            when    flattened.index = 5   then      'HL_CHILD_CODE_PATIENT' --1 HAS CHILD NODE, 0 NO CHILD NODE
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
                        'HL_PREFIX_PATIENT',
                        'HL_ID_PATIENT',
                        'HL_PARENT_ID_PATIENT',
                        'HL_LEVEL_CODE_PATIENT',
                        'HL_CHILD_CODE_PATIENT'
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
                    HL_PREFIX_PATIENT,
                    HL_ID_PATIENT,
                    HL_PARENT_ID_PATIENT,
                    HL_LEVEL_CODE_PATIENT,
                    HL_CHILD_CODE_PATIENT
                )
)
, patient_pat as
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

                    case    when    flattened.index = 1   then      'PAT_PREFIX_PATIENT'
                            when    flattened.index = 2   then      'RELATIONSHIP_CODE_PATIENT'     --18/01/19/20 SELF/SPOUSE/CHILD/EMPLOYEE
                            when    flattened.index = 3   then      'LOCATION_CODE_PATIENT'
                            when    flattened.index = 4   then      'EMPLOYMENT_STATUS_PATIENT'
                            when    flattened.index = 5   then      'STUDENT_STATUS_PATIENT'
                            when    flattened.index = 6   then      'DATE_OF_DEATH_PATIENT'
                            when    flattened.index = 7   then      'FORMAT_QUALIFIER_PATIENT'
                            when    flattened.index = 8   then      'MEASUREMENT_UNIT_CODE_PATIENT'
                            when    flattened.index = 9   then      'WEIGHT_PATIENT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^PAT.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PAT_PREFIX_PATIENT',
                        'RELATIONSHIP_CODE_PATIENT',
                        'LOCATION_CODE_PATIENT',
                        'EMPLOYMENT_STATUS_PATIENT',
                        'STUDENT_STATUS_PATIENT',
                        'DATE_OF_DEATH_PATIENT',
                        'FORMAT_QUALIFIER_PATIENT',
                        'MEASUREMENT_UNIT_CODE_PATIENT',
                        'WEIGHT_PATIENT'
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
                    PAT_PREFIX_PATIENT,
                    RELATIONSHIP_CODE_PATIENT,
                    LOCATION_CODE_PATIENT,
                    EMPLOYMENT_STATUS_PATIENT,
                    STUDENT_STATUS_PATIENT,
                    DATE_OF_DEATH_PATIENT,
                    FORMAT_QUALIFIER_PATIENT_PAT,
                    MEASUREMENT_UNIT_CODE_PATIENT,
                    WEIGHT_PATIENT
                )
)
, patient_nmQC as
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

                    case    when    flattened.index = 1   then      'NAME_CODE_PATIENT'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_PATIENT'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_PATIENT'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_PATIENT'
                            when    flattened.index = 5   then      'FIRST_NAME_PATIENT'
                            when    flattened.index = 6   then      'MIDDLE_NAME_PATIENT'
                            when    flattened.index = 7   then      'NAME_PREFIX_PATIENT'
                            when    flattened.index = 8   then      'NAME_SUFFIX_PATIENT'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_PATIENT'
                            when    flattened.index = 10  then      'ID_CODE_PATIENT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^NM1\\*QC.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_PATIENT',
                        'ENTITY_IDENTIFIER_CODE_PATIENT',
                        'ENTITY_TYPE_QUALIFIER_PATIENT',
                        'LAST_NAME_ORG_PATIENT',
                        'FIRST_NAME_PATIENT',
                        'MIDDLE_NAME_PATIENT',
                        'NAME_PREFIX_PATIENT',
                        'NAME_SUFFIX_PATIENT',
                        'ID_CODE_QUALIFIER_PATIENT',
                        'ID_CODE_PATIENT'
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
                    NAME_CODE_PATIENT,
                    ENTITY_IDENTIFIER_CODE_PATIENT,
                    ENTITY_TYPE_QUALIFIER_PATIENT,
                    LAST_NAME_ORG_PATIENT,
                    FIRST_NAME_PATIENT,
                    MIDDLE_NAME_PATIENT,
                    NAME_PREFIX_PATIENT,
                    NAME_SUFFIX_PATIENT,
                    ID_CODE_QUALIFIER_PATIENT,
                    ID_CODE_PATIENT
                )
)
, patient_n3 as
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

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PATIENT'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_PATIENT'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_PATIENT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*QC'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PATIENT',
                        'ADDRESS_LINE_1_PATIENT',
                        'ADDRESS_LINE_2_PATIENT'
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
                    ADDRESS_CODE_PATIENT_N3,
                    ADDRESS_LINE_1_PATIENT,
                    ADDRESS_LINE_2_PATIENT
                )
)
, patient_n4 as
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

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PATIENT'
                            when    flattened.index = 2   then      'CITY_PATIENT'
                            when    flattened.index = 3   then      'ST_PATIENT'
                            when    flattened.index = 4   then      'ZIP_PATIENT'
                            when    flattened.index = 5   then      'COUNTRY_PATIENT'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_PATIENT'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_PATIENT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*QC'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PATIENT',
                        'CITY_PATIENT',
                        'ST_PATIENT',
                        'ZIP_PATIENT',
                        'COUNTRY_PATIENT',
                        'LOCATION_QUALIFIER_PATIENT',
                        'LOCATION_IDENTIFIER_PATIENT'
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
                    ADDRESS_CODE_PATIENT_N4,
                    CITY_PATIENT,
                    ST_PATIENT,
                    ZIP_PATIENT,
                    COUNTRY_PATIENT,
                    LOCATION_QUALIFIER_PATIENT,
                    LOCATION_IDENTIFIER_PATIENT
                )
)
, patient_dmg as
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

                    case    when    flattened.index = 1   then      'DMG_PREFIX_PATIENT'
                            when    flattened.index = 2   then      'FORMAT_QUALIFIER_PATIENT'
                            when    flattened.index = 3   then      'DOB_PATIENT'
                            when    flattened.index = 4   then      'GENDER_CODE_PATIENT'
                            when    flattened.index = 5   then      'MARITAL_STATUS_PATIENT'
                            when    flattened.index = 6   then      'ETHNICITY_CODE_PATIENT'
                            when    flattened.index = 7   then      'CITIZENSHIP_CODE_PATIENT'
                            when    flattened.index = 8   then      'COUNTRY_CODE_PATIENT'
                            when    flattened.index = 9   then      'VERIFICATION_CODE_PATIENT'
                            when    flattened.index = 10  then      'QUANTITY_PATIENT'
                            when    flattened.index = 11  then      'LIST_QUALIFIER_CODE_PATIENT'
                            when    flattened.index = 12  then      'INDUSTRY_CODE_PATIENT'
                            end     as value_header,

                    case    when    value_header = 'DOB_PATIENT'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^DMG.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*QC'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DMG_PREFIX_PATIENT',
                        'FORMAT_QUALIFIER_PATIENT',
                        'DOB_PATIENT',
                        'GENDER_CODE_PATIENT',
                        'MARITAL_STATUS_PATIENT',
                        'ETHNICITY_CODE_PATIENT',
                        'CITIZENSHIP_CODE_PATIENT',
                        'COUNTRY_CODE_PATIENT',
                        'VERIFICATION_CODE_PATIENT',
                        'QUANTITY_PATIENT',
                        'LIST_QUALIFIER_CODE_PATIENT',
                        'INDUSTRY_CODE_PATIENT'
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
                    DMG_PREFIX_PATIENT,
                    FORMAT_QUALIFIER_PATIENT_DMG,
                    DOB_PATIENT,
                    GENDER_CODE_PATIENT,
                    MARITAL_STATUS_PATIENT,
                    ETHNICITY_CODE_PATIENT,
                    CITIZENSHIP_CODE_PATIENT,
                    COUNTRY_CODE_PATIENT,
                    VERIFICATION_CODE_PATIENT,
                    QUANTITY_PATIENT,
                    LIST_QUALIFIER_CODE_PATIENT,
                    INDUSTRY_CODE_PATIENT
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
            header.hl_prefix_patient,
            header.hl_id_patient,
            header.hl_parent_id_patient,
            header.hl_level_code_patient,
            header.hl_child_code_patient,
            pat.pat_prefix_patient,
            pat.relationship_code_patient,
            pat.location_code_patient,
            pat.employment_status_patient,
            pat.student_status_patient,
            pat.date_of_death_patient,
            pat.format_qualifier_patient_pat,
            pat.measurement_unit_code_patient,
            pat.weight_patient,
            nmQC.name_code_patient,
            nmQC.entity_identifier_code_patient,
            nmQC.entity_type_qualifier_patient,
            nmQC.last_name_org_patient,
            nmQC.first_name_patient,
            nmQC.middle_name_patient,
            nmQC.name_prefix_patient,
            nmQC.name_suffix_patient,
            nmQC.id_code_qualifier_patient,
            nmQC.id_code_patient,
            n3.address_code_patient_n3,
            n3.address_line_1_patient,
            n3.address_line_2_patient,
            n4.address_code_patient_n4,
            n4.city_patient,
            n4.st_patient,
            n4.zip_patient,
            n4.country_patient,
            n4.location_qualifier_patient,
            n4.location_identifier_patient,
            dmg.dmg_prefix_patient,
            dmg.format_qualifier_patient_dmg,
            dmg.dob_patient,
            dmg.gender_code_patient,
            dmg.marital_status_patient,
            dmg.ethnicity_code_patient,
            dmg.citizenship_code_patient,
            dmg.country_code_patient,
            dmg.verification_code_patient,
            dmg.quantity_patient,
            dmg.list_qualifier_code_patient,
            dmg.industry_code_patient

from        patient_hl      as header
            left join
                patient_pat     as pat
                on  header.response_id          = pat.response_id
                and header.nth_functional_group = pat.nth_functional_group
                and header.nth_transaction_set  = pat.nth_transaction_set
                and header.hl_index_patient_23  = pat.hl_index_patient_23
            left join
                patient_nmQC    as nmQC
                on  header.response_id          = nmQC.response_id
                and header.nth_functional_group = nmQC.nth_functional_group
                and header.nth_transaction_set  = nmQC.nth_transaction_set
                and header.hl_index_patient_23  = nmQC.hl_index_patient_23
            left join
                patient_n3      as n3
                on  header.response_id          = n3.response_id
                and header.nth_functional_group = n3.nth_functional_group
                and header.nth_transaction_set  = n3.nth_transaction_set
                and header.hl_index_patient_23  = n3.hl_index_patient_23
            left join
                patient_n4      as n4
                on  header.response_id          = n4.response_id
                and header.nth_functional_group = n4.nth_functional_group
                and header.nth_transaction_set  = n4.nth_transaction_set
                and header.hl_index_patient_23  = n4.hl_index_patient_23
            left join
                patient_dmg     as dmg
                on  header.response_id          = dmg.response_id
                and header.nth_functional_group = dmg.nth_functional_group
                and header.nth_transaction_set  = dmg.nth_transaction_set
                and header.hl_index_patient_23  = dmg.hl_index_patient_23
                
order by    1,2,3
;



create or replace task
    edwprodhh.edi_837i_parser.insert_hl_patients
    warehouse = analysis_wh
    after edwprodhh.edi_837i_parser.insert_response_flat
as
insert into
    edwprodhh.edi_837i_parser.hl_patients
(
    RESPONSE_ID,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    INDEX,
    HL_INDEX_CURRENT,
    HL_INDEX_BILLING_20,
    HL_INDEX_SUBSCRIBER_22,
    HL_INDEX_PATIENT_23,
    HL_PREFIX_PATIENT,
    HL_ID_PATIENT,
    HL_PARENT_ID_PATIENT,
    HL_LEVEL_CODE_PATIENT,
    HL_CHILD_CODE_PATIENT,
    PAT_PREFIX_PATIENT,
    RELATIONSHIP_CODE_PATIENT,
    LOCATION_CODE_PATIENT,
    EMPLOYMENT_STATUS_PATIENT,
    STUDENT_STATUS_PATIENT,
    DATE_OF_DEATH_PATIENT,
    FORMAT_QUALIFIER_PATIENT_PAT,
    MEASUREMENT_UNIT_CODE_PATIENT,
    WEIGHT_PATIENT,
    NAME_CODE_PATIENT,
    ENTITY_IDENTIFIER_CODE_PATIENT,
    ENTITY_TYPE_QUALIFIER_PATIENT,
    LAST_NAME_ORG_PATIENT,
    FIRST_NAME_PATIENT,
    MIDDLE_NAME_PATIENT,
    NAME_PREFIX_PATIENT,
    NAME_SUFFIX_PATIENT,
    ID_CODE_QUALIFIER_PATIENT,
    ID_CODE_PATIENT,
    ADDRESS_CODE_PATIENT_N3,
    ADDRESS_LINE_1_PATIENT,
    ADDRESS_LINE_2_PATIENT,
    ADDRESS_CODE_PATIENT_N4,
    CITY_PATIENT,
    ST_PATIENT,
    ZIP_PATIENT,
    COUNTRY_PATIENT,
    LOCATION_QUALIFIER_PATIENT,
    LOCATION_IDENTIFIER_PATIENT,
    DMG_PREFIX_PATIENT,
    FORMAT_QUALIFIER_PATIENT_DMG,
    DOB_PATIENT,
    GENDER_CODE_PATIENT,
    MARITAL_STATUS_PATIENT,
    ETHNICITY_CODE_PATIENT,
    CITIZENSHIP_CODE_PATIENT,
    COUNTRY_CODE_PATIENT,
    VERIFICATION_CODE_PATIENT,
    QUANTITY_PATIENT,
    LIST_QUALIFIER_CODE_PATIENT,
    INDUSTRY_CODE_PATIENT
)
with filtered_hl as
(
    select      *
    from        edwprodhh.edi_837i_parser.response_flat as filtered
    where       hl_index_billing_20         is not null                                          --0 Pre-Filter
                and hl_index_subscriber_22  is not null
                and hl_index_patient_23     is not null
                and claim_index             is null
                and response_id not in (select response_id from edwprodhh.edi_837i_parser.hl_patients)
)
, patient_hl as
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

                    case    when    flattened.index = 1   then      'HL_PREFIX_PATIENT'
                            when    flattened.index = 2   then      'HL_ID_PATIENT'
                            when    flattened.index = 3   then      'HL_PARENT_ID_PATIENT'
                            when    flattened.index = 4   then      'HL_LEVEL_CODE_PATIENT' --20 BILLING, 22 SUBSCRIBER, 23 PATIENT
                            when    flattened.index = 5   then      'HL_CHILD_CODE_PATIENT' --1 HAS CHILD NODE, 0 NO CHILD NODE
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
                        'HL_PREFIX_PATIENT',
                        'HL_ID_PATIENT',
                        'HL_PARENT_ID_PATIENT',
                        'HL_LEVEL_CODE_PATIENT',
                        'HL_CHILD_CODE_PATIENT'
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
                    HL_PREFIX_PATIENT,
                    HL_ID_PATIENT,
                    HL_PARENT_ID_PATIENT,
                    HL_LEVEL_CODE_PATIENT,
                    HL_CHILD_CODE_PATIENT
                )
)
, patient_pat as
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

                    case    when    flattened.index = 1   then      'PAT_PREFIX_PATIENT'
                            when    flattened.index = 2   then      'RELATIONSHIP_CODE_PATIENT'     --18/01/19/20 SELF/SPOUSE/CHILD/EMPLOYEE
                            when    flattened.index = 3   then      'LOCATION_CODE_PATIENT'
                            when    flattened.index = 4   then      'EMPLOYMENT_STATUS_PATIENT'
                            when    flattened.index = 5   then      'STUDENT_STATUS_PATIENT'
                            when    flattened.index = 6   then      'DATE_OF_DEATH_PATIENT'
                            when    flattened.index = 7   then      'FORMAT_QUALIFIER_PATIENT'
                            when    flattened.index = 8   then      'MEASUREMENT_UNIT_CODE_PATIENT'
                            when    flattened.index = 9   then      'WEIGHT_PATIENT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^PAT.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PAT_PREFIX_PATIENT',
                        'RELATIONSHIP_CODE_PATIENT',
                        'LOCATION_CODE_PATIENT',
                        'EMPLOYMENT_STATUS_PATIENT',
                        'STUDENT_STATUS_PATIENT',
                        'DATE_OF_DEATH_PATIENT',
                        'FORMAT_QUALIFIER_PATIENT',
                        'MEASUREMENT_UNIT_CODE_PATIENT',
                        'WEIGHT_PATIENT'
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
                    PAT_PREFIX_PATIENT,
                    RELATIONSHIP_CODE_PATIENT,
                    LOCATION_CODE_PATIENT,
                    EMPLOYMENT_STATUS_PATIENT,
                    STUDENT_STATUS_PATIENT,
                    DATE_OF_DEATH_PATIENT,
                    FORMAT_QUALIFIER_PATIENT_PAT,
                    MEASUREMENT_UNIT_CODE_PATIENT,
                    WEIGHT_PATIENT
                )
)
, patient_nmQC as
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

                    case    when    flattened.index = 1   then      'NAME_CODE_PATIENT'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_PATIENT'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_PATIENT'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_PATIENT'
                            when    flattened.index = 5   then      'FIRST_NAME_PATIENT'
                            when    flattened.index = 6   then      'MIDDLE_NAME_PATIENT'
                            when    flattened.index = 7   then      'NAME_PREFIX_PATIENT'
                            when    flattened.index = 8   then      'NAME_SUFFIX_PATIENT'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_PATIENT'
                            when    flattened.index = 10  then      'ID_CODE_PATIENT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^NM1\\*QC.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_PATIENT',
                        'ENTITY_IDENTIFIER_CODE_PATIENT',
                        'ENTITY_TYPE_QUALIFIER_PATIENT',
                        'LAST_NAME_ORG_PATIENT',
                        'FIRST_NAME_PATIENT',
                        'MIDDLE_NAME_PATIENT',
                        'NAME_PREFIX_PATIENT',
                        'NAME_SUFFIX_PATIENT',
                        'ID_CODE_QUALIFIER_PATIENT',
                        'ID_CODE_PATIENT'
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
                    NAME_CODE_PATIENT,
                    ENTITY_IDENTIFIER_CODE_PATIENT,
                    ENTITY_TYPE_QUALIFIER_PATIENT,
                    LAST_NAME_ORG_PATIENT,
                    FIRST_NAME_PATIENT,
                    MIDDLE_NAME_PATIENT,
                    NAME_PREFIX_PATIENT,
                    NAME_SUFFIX_PATIENT,
                    ID_CODE_QUALIFIER_PATIENT,
                    ID_CODE_PATIENT
                )
)
, patient_n3 as
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

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PATIENT'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_PATIENT'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_PATIENT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*QC'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PATIENT',
                        'ADDRESS_LINE_1_PATIENT',
                        'ADDRESS_LINE_2_PATIENT'
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
                    ADDRESS_CODE_PATIENT_N3,
                    ADDRESS_LINE_1_PATIENT,
                    ADDRESS_LINE_2_PATIENT
                )
)
, patient_n4 as
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

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PATIENT'
                            when    flattened.index = 2   then      'CITY_PATIENT'
                            when    flattened.index = 3   then      'ST_PATIENT'
                            when    flattened.index = 4   then      'ZIP_PATIENT'
                            when    flattened.index = 5   then      'COUNTRY_PATIENT'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_PATIENT'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_PATIENT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*QC'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PATIENT',
                        'CITY_PATIENT',
                        'ST_PATIENT',
                        'ZIP_PATIENT',
                        'COUNTRY_PATIENT',
                        'LOCATION_QUALIFIER_PATIENT',
                        'LOCATION_IDENTIFIER_PATIENT'
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
                    ADDRESS_CODE_PATIENT_N4,
                    CITY_PATIENT,
                    ST_PATIENT,
                    ZIP_PATIENT,
                    COUNTRY_PATIENT,
                    LOCATION_QUALIFIER_PATIENT,
                    LOCATION_IDENTIFIER_PATIENT
                )
)
, patient_dmg as
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

                    case    when    flattened.index = 1   then      'DMG_PREFIX_PATIENT'
                            when    flattened.index = 2   then      'FORMAT_QUALIFIER_PATIENT'
                            when    flattened.index = 3   then      'DOB_PATIENT'
                            when    flattened.index = 4   then      'GENDER_CODE_PATIENT'
                            when    flattened.index = 5   then      'MARITAL_STATUS_PATIENT'
                            when    flattened.index = 6   then      'ETHNICITY_CODE_PATIENT'
                            when    flattened.index = 7   then      'CITIZENSHIP_CODE_PATIENT'
                            when    flattened.index = 8   then      'COUNTRY_CODE_PATIENT'
                            when    flattened.index = 9   then      'VERIFICATION_CODE_PATIENT'
                            when    flattened.index = 10  then      'QUANTITY_PATIENT'
                            when    flattened.index = 11  then      'LIST_QUALIFIER_CODE_PATIENT'
                            when    flattened.index = 12  then      'INDUSTRY_CODE_PATIENT'
                            end     as value_header,

                    case    when    value_header = 'DOB_PATIENT'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_hl,
                    lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_hl.line_element_837, '^DMG.*')                          --1 Filter
                    and filtered_hl.lag_name_indicator = 'NM1*QC'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DMG_PREFIX_PATIENT',
                        'FORMAT_QUALIFIER_PATIENT',
                        'DOB_PATIENT',
                        'GENDER_CODE_PATIENT',
                        'MARITAL_STATUS_PATIENT',
                        'ETHNICITY_CODE_PATIENT',
                        'CITIZENSHIP_CODE_PATIENT',
                        'COUNTRY_CODE_PATIENT',
                        'VERIFICATION_CODE_PATIENT',
                        'QUANTITY_PATIENT',
                        'LIST_QUALIFIER_CODE_PATIENT',
                        'INDUSTRY_CODE_PATIENT'
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
                    DMG_PREFIX_PATIENT,
                    FORMAT_QUALIFIER_PATIENT_DMG,
                    DOB_PATIENT,
                    GENDER_CODE_PATIENT,
                    MARITAL_STATUS_PATIENT,
                    ETHNICITY_CODE_PATIENT,
                    CITIZENSHIP_CODE_PATIENT,
                    COUNTRY_CODE_PATIENT,
                    VERIFICATION_CODE_PATIENT,
                    QUANTITY_PATIENT,
                    LIST_QUALIFIER_CODE_PATIENT,
                    INDUSTRY_CODE_PATIENT
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
            header.hl_prefix_patient,
            header.hl_id_patient,
            header.hl_parent_id_patient,
            header.hl_level_code_patient,
            header.hl_child_code_patient,
            pat.pat_prefix_patient,
            pat.relationship_code_patient,
            pat.location_code_patient,
            pat.employment_status_patient,
            pat.student_status_patient,
            pat.date_of_death_patient,
            pat.format_qualifier_patient_pat,
            pat.measurement_unit_code_patient,
            pat.weight_patient,
            nmQC.name_code_patient,
            nmQC.entity_identifier_code_patient,
            nmQC.entity_type_qualifier_patient,
            nmQC.last_name_org_patient,
            nmQC.first_name_patient,
            nmQC.middle_name_patient,
            nmQC.name_prefix_patient,
            nmQC.name_suffix_patient,
            nmQC.id_code_qualifier_patient,
            nmQC.id_code_patient,
            n3.address_code_patient_n3,
            n3.address_line_1_patient,
            n3.address_line_2_patient,
            n4.address_code_patient_n4,
            n4.city_patient,
            n4.st_patient,
            n4.zip_patient,
            n4.country_patient,
            n4.location_qualifier_patient,
            n4.location_identifier_patient,
            dmg.dmg_prefix_patient,
            dmg.format_qualifier_patient_dmg,
            dmg.dob_patient,
            dmg.gender_code_patient,
            dmg.marital_status_patient,
            dmg.ethnicity_code_patient,
            dmg.citizenship_code_patient,
            dmg.country_code_patient,
            dmg.verification_code_patient,
            dmg.quantity_patient,
            dmg.list_qualifier_code_patient,
            dmg.industry_code_patient

from        patient_hl      as header
            left join
                patient_pat     as pat
                on  header.response_id          = pat.response_id
                and header.nth_functional_group = pat.nth_functional_group
                and header.nth_transaction_set  = pat.nth_transaction_set
                and header.hl_index_patient_23  = pat.hl_index_patient_23
            left join
                patient_nmQC    as nmQC
                on  header.response_id          = nmQC.response_id
                and header.nth_functional_group = nmQC.nth_functional_group
                and header.nth_transaction_set  = nmQC.nth_transaction_set
                and header.hl_index_patient_23  = nmQC.hl_index_patient_23
            left join
                patient_n3      as n3
                on  header.response_id          = n3.response_id
                and header.nth_functional_group = n3.nth_functional_group
                and header.nth_transaction_set  = n3.nth_transaction_set
                and header.hl_index_patient_23  = n3.hl_index_patient_23
            left join
                patient_n4      as n4
                on  header.response_id          = n4.response_id
                and header.nth_functional_group = n4.nth_functional_group
                and header.nth_transaction_set  = n4.nth_transaction_set
                and header.hl_index_patient_23  = n4.hl_index_patient_23
            left join
                patient_dmg     as dmg
                on  header.response_id          = dmg.response_id
                and header.nth_functional_group = dmg.nth_functional_group
                and header.nth_transaction_set  = dmg.nth_transaction_set
                and header.hl_index_patient_23  = dmg.hl_index_patient_23
                
order by    1,2,3
;