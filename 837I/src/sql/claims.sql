create or replace table
    edwprodhh.edi_837i_parser.claims
as
with filtered_clm as
(
    select      *
    from        edwprodhh.edi_837i_parser.response_flat
    where       claim_index is not null --0 Pre-Filter
)
, header_clm as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CLM_PREFIX'
                            when    flattened.index = 2   then      'CLAIM_ID'
                            when    flattened.index = 3   then      'TOTAL_CLAIM_CHARGE'
                            when    flattened.index = 4   then      'CLAIM_PAYMENT_AMOUNT'
                            when    flattened.index = 5   then      'PATIENT_RESPONSIBILITY_AMOUNT'
                            when    flattened.index = 6   then      'COMPOSITE_FACILITY_CODE'           --11/13/21 Office/Hospital/Inpatient : ...
                            when    flattened.index = 7   then      'PROVIDER_SIGNATURE'
                            when    flattened.index = 8   then      'PARTICIPATION_CODE'
                            when    flattened.index = 9   then      'BENEFITS_ASSIGNMENT_INDICATOR'
                            when    flattened.index = 10  then      'RELEASE_OF_INFO_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^CLM.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'CLM_PREFIX',
                        'CLAIM_ID',
                        'TOTAL_CLAIM_CHARGE',
                        'CLAIM_PAYMENT_AMOUNT',
                        'PATIENT_RESPONSIBILITY_AMOUNT',
                        'COMPOSITE_FACILITY_CODE',
                        'PROVIDER_SIGNATURE',
                        'PARTICIPATION_CODE',
                        'BENEFITS_ASSIGNMENT_INDICATOR',
                        'RELEASE_OF_INFO_CODE'
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
                    CLAIM_INDEX,
                    CLM_PREFIX,
                    CLAIM_ID,
                    TOTAL_CLAIM_CHARGE,
                    CLAIM_PAYMENT_AMOUNT,
                    PATIENT_RESPONSIBILITY_AMOUNT,
                    COMPOSITE_FACILITY_CODE,
                    PROVIDER_SIGNATURE,
                    PARTICIPATION_CODE,
                    BENEFITS_ASSIGNMENT_INDICATOR,
                    RELEASE_OF_INFO_CODE
                )
)
, claim_dtp_434 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_CLAIM'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_CLAIM'
                            when    flattened.index = 3   then      'DATE_FORMAT_CLAIM'
                            when    flattened.index = 4   then      'DATE_RANGE_CLAIM'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*434.*')                          --1 Filter
    )
    select      *,
                case    when    date_format_claim = 'RD8'
                        and     regexp_like(date_range_claim, '^\\d{8}\\-\\d{8}$')
                        then    to_date(left(date_range_claim,  8), 'YYYYMMDD')
                        else    NULL
                        end     as start_date_claim,

                case    when    date_format_claim = 'RD8'
                        and     regexp_like(date_range_claim, '^\\d{8}\\-\\d{8}$')
                        then    to_date(right(date_range_claim, 8), 'YYYYMMDD')
                        else    NULL
                        end     as end_date_claim
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_CLAIM',
                        'DATE_QUALIFIER_CLAIM',
                        'DATE_FORMAT_CLAIM',
                        'DATE_RANGE_CLAIM'
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
                    CLAIM_INDEX,
                    DTP_PREFIX_CLAIM,
                    DATE_QUALIFIER_CLAIM,
                    DATE_FORMAT_CLAIM,
                    DATE_RANGE_CLAIM
                )
)
, claim_dtp_435 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_CLAIM_ADMIT'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_CLAIM_ADMIT'
                            when    flattened.index = 3   then      'DATE_FORMAT_CLAIM_ADMIT'
                            when    flattened.index = 4   then      'DATETIME_CLAIM_ADMIT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*435.*')                          --1 Filter
    )
    select      *,
                case    when    date_format_claim_admit = 'DT'
                        and     regexp_like(datetime_claim_admit, '^\\d{12}$')
                        then    to_date(left(datetime_claim_admit,  8), 'YYYYMMDD')
                        else    NULL
                        end     as admit_date_claim,

                case    when    date_format_claim_admit = 'DT'
                        and     regexp_like(datetime_claim_admit, '^\\d{12}$')
                        then    left(right(datetime_claim_admit, 4), 2)
                        else    NULL
                        end     as admit_hour_claim
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_CLAIM_ADMIT',
                        'DATE_QUALIFIER_CLAIM_ADMIT',
                        'DATE_FORMAT_CLAIM_ADMIT',
                        'DATETIME_CLAIM_ADMIT'
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
                    CLAIM_INDEX,
                    DTP_PREFIX_CLAIM_ADMIT,
                    DATE_QUALIFIER_CLAIM_ADMIT,
                    DATE_FORMAT_CLAIM_ADMIT,
                    DATETIME_CLAIM_ADMIT
                )
)
, claim_dtp_096 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_CLAIM_TIME'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_CLAIM_TIME'
                            when    flattened.index = 3   then      'DATE_FORMAT_CLAIM_TIME'
                            when    flattened.index = 4   then      'TIME_CLAIM_TIME'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*096.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_CLAIM_TIME',
                        'DATE_QUALIFIER_CLAIM_TIME',
                        'DATE_FORMAT_CLAIM_TIME',
                        'TIME_CLAIM_TIME'
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
                    CLAIM_INDEX,
                    DTP_PREFIX_CLAIM_TIME,
                    DATE_QUALIFIER_CLAIM_TIME,
                    DATE_FORMAT_CLAIM_TIME,
                    TIME_CLAIM_TIME
                )
)
, claim_cl1 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CL1_PREFIX'                
                            when    flattened.index = 2   then      'ADMISSION_TYPE_CODE'       --1/2/3/4/5 Emergency/Urgent/Elective/Newbord/Trauma
                            when    flattened.index = 3   then      'ADMISSION_SOURCE_CODE'     --1/2/3/7/9 Phys Referral/Clinic Referral/HMO Referral/ER/Unavailable
                            when    flattened.index = 4   then      'PATIENT_STATUS_CODE'       --1/2/7/20/30 Discharge home/Discharge short term hospital/Left/Expired/Still a patient
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^CL1.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'CL1_PREFIX',
                        'ADMISSION_TYPE_CODE',
                        'ADMISSION_SOURCE_CODE',
                        'PATIENT_STATUS_CODE'
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
                    CLAIM_INDEX,
                    CL1_PREFIX,
                    ADMISSION_TYPE_CODE,
                    ADMISSION_SOURCE_CODE,
                    PATIENT_STATUS_CODE
                )
)
, claim_ref as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_PREFIX_CLAIM'
                            when    flattened.index = 2   then      'REFERENCE_ID_CODE_CLAIM'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'REFERENCE_ID_CLAIM'
                            when    flattened.index = 4   then      'DESCRIPTION_CLAIM'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^REF.*')                            --1 Filter
                    and filtered_clm.claim_index is not null
                    and filtered_clm.lx_index is null
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'REF_PREFIX_CLAIM',
                            'REFERENCE_ID_CODE_CLAIM',
                            'REFERENCE_ID_CLAIM',
                            'DESCRIPTION_CLAIM'
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
                        CLAIM_INDEX,
                        REF_PREFIX_CLAIM,
                        REFERENCE_ID_CODE_CLAIM,
                        REFERENCE_ID_CLAIM,
                        DESCRIPTION_CLAIM
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                claim_index,
                array_agg(
                    object_construct_keep_null(
                        'claim_ref_code',           reference_id_code_claim::varchar,
                        'claim_ref_value',          reference_id_claim::varchar,
                        'claim_ref_description',    description_claim::varchar
                    )
                )   as clm_ref_array
    from        pivoted
    group by    1,2,3,4
    order by    1,2,3,4
)
, claim_hi as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'HI_PREFIX'
                            when    flattened.index = 2   then      'HI_VAL01'
                            when    flattened.index = 3   then      'HI_VAL02'
                            when    flattened.index = 4   then      'HI_VAL03'
                            when    flattened.index = 5   then      'HI_VAL04'
                            when    flattened.index = 6   then      'HI_VAL05'
                            when    flattened.index = 7   then      'HI_VAL06'
                            when    flattened.index = 8   then      'HI_VAL07'
                            when    flattened.index = 9   then      'HI_VAL08'
                            when    flattened.index = 10  then      'HI_VAL09'
                            when    flattened.index = 11  then      'HI_VAL10'
                            when    flattened.index = 12  then      'HI_VAL11'
                            when    flattened.index = 13  then      'HI_VAL12'
                            when    flattened.index = 14  then      'HI_VAL13'
                            when    flattened.index = 15  then      'HI_VAL14'
                            when    flattened.index = 16  then      'HI_VAL15'
                            when    flattened.index = 17  then      'HI_VAL16'
                            when    flattened.index = 18  then      'HI_VAL17'
                            when    flattened.index = 19  then      'HI_VAL18'
                            when    flattened.index = 20 then       'HI_VAL19'
                            when    flattened.index = 21  then      'HI_VAL20'
                            when    flattened.index = 22  then      'HI_VAL21'
                            when    flattened.index = 23  then      'HI_VAL22'
                            when    flattened.index = 24  then      'HI_VAL23'
                            when    flattened.index = 25  then      'HI_VAL24'
                            when    flattened.index = 26  then      'HI_VAL25'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^HI.*')                            --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'HI_PREFIX',
                            'HI_VAL01',
                            'HI_VAL02',
                            'HI_VAL03',
                            'HI_VAL04',
                            'HI_VAL05',
                            'HI_VAL06',
                            'HI_VAL07',
                            'HI_VAL08',
                            'HI_VAL09',
                            'HI_VAL10',
                            'HI_VAL11',
                            'HI_VAL12',
                            'HI_VAL13',
                            'HI_VAL14',
                            'HI_VAL15',
                            'HI_VAL16',
                            'HI_VAL17',
                            'HI_VAL18',
                            'HI_VAL19',
                            'HI_VAL20',
                            'HI_VAL21',
                            'HI_VAL22',
                            'HI_VAL23',
                            'HI_VAL24',
                            'HI_VAL25'
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
                        CLAIM_INDEX,
                        HI_PREFIX,
                        HI_VAL01,
                        HI_VAL02,
                        HI_VAL03,
                        HI_VAL04,
                        HI_VAL05,
                        HI_VAL06,
                        HI_VAL07,
                        HI_VAL08,
                        HI_VAL09,
                        HI_VAL10,
                        HI_VAL11,
                        HI_VAL12,
                        HI_VAL13,
                        HI_VAL14,
                        HI_VAL15,
                        HI_VAL16,
                        HI_VAL17,
                        HI_VAL18,
                        HI_VAL19,
                        HI_VAL20,
                        HI_VAL21,
                        HI_VAL22,
                        HI_VAL23,
                        HI_VAL24,
                        HI_VAL25
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                claim_index,
                array_agg(unpvt.metric_value) as clm_hi_array
    from        pivoted
                unpivot (
                    metric_value for metric_name in (
                        HI_VAL01,
                        HI_VAL02,
                        HI_VAL03,
                        HI_VAL04,
                        HI_VAL05,
                        HI_VAL06,
                        HI_VAL07,
                        HI_VAL08,
                        HI_VAL09,
                        HI_VAL10,
                        HI_VAL11,
                        HI_VAL12,
                        HI_VAL13,
                        HI_VAL14,
                        HI_VAL15,
                        HI_VAL16,
                        HI_VAL17,
                        HI_VAL18,
                        HI_VAL19,
                        HI_VAL20,
                        HI_VAL21,
                        HI_VAL22,
                        HI_VAL23,
                        HI_VAL24,
                        HI_VAL25
                    )
                )   as unpvt
    group by    1,2,3,4
    order by    1,2,3,4
)
, claim_nm71 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_ATTENDING'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_ATTENDING'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_ATTENDING'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_ATTENDING'
                            when    flattened.index = 5   then      'FIRST_NAME_ATTENDING'
                            when    flattened.index = 6   then      'MIDDLE_NAME_ATTENDING'
                            when    flattened.index = 7   then      'NAME_PREFIX_ATTENDING'
                            when    flattened.index = 8   then      'NAME_SUFFIX_ATTENDING'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_ATTENDING'
                            when    flattened.index = 10  then      'ID_CODE_ATTENDING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*71.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_ATTENDING',
                        'ENTITY_IDENTIFIER_CODE_ATTENDING',
                        'ENTITY_TYPE_QUALIFIER_ATTENDING',
                        'LAST_NAME_ORG_ATTENDING',
                        'FIRST_NAME_ATTENDING',
                        'MIDDLE_NAME_ATTENDING',
                        'NAME_PREFIX_ATTENDING',
                        'NAME_SUFFIX_ATTENDING',
                        'ID_CODE_QUALIFIER_ATTENDING',
                        'ID_CODE_ATTENDING'
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
                    CLAIM_INDEX,
                    NAME_CODE_ATTENDING,
                    ENTITY_IDENTIFIER_CODE_ATTENDING,
                    ENTITY_TYPE_QUALIFIER_ATTENDING,
                    LAST_NAME_ORG_ATTENDING,
                    FIRST_NAME_ATTENDING,
                    MIDDLE_NAME_ATTENDING,
                    NAME_PREFIX_ATTENDING,
                    NAME_SUFFIX_ATTENDING,
                    ID_CODE_QUALIFIER_ATTENDING,
                    ID_CODE_ATTENDING
                )
)
, claim_nm71_prv as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PRV_PREFIX_ATTENDING'
                            when    flattened.index = 2   then      'PROVIDER_CODE_ATTENDING'
                            when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_ATTENDING'
                            when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_ATTENDING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^PRV.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*71'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PRV_PREFIX_ATTENDING',
                        'PROVIDER_CODE_ATTENDING',
                        'REFERENCE_ID_QUALIFIER_ATTENDING',
                        'PROVIDER_TAXONOMY_CODE_ATTENDING'
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
                    CLAIM_INDEX,
                    PRV_PREFIX_ATTENDING,
                    PROVIDER_CODE_ATTENDING,
                    REFERENCE_ID_QUALIFIER_ATTENDING,
                    PROVIDER_TAXONOMY_CODE_ATTENDING
                )
)
, claim_nm72 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_OPERATING'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_OPERATING'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_OPERATING'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_OPERATING'
                            when    flattened.index = 5   then      'FIRST_NAME_OPERATING'
                            when    flattened.index = 6   then      'MIDDLE_NAME_OPERATING'
                            when    flattened.index = 7   then      'NAME_PREFIX_OPERATING'
                            when    flattened.index = 8   then      'NAME_SUFFIX_OPERATING'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_OPERATING'
                            when    flattened.index = 10  then      'ID_CODE_OPERATING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*72.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_OPERATING',
                        'ENTITY_IDENTIFIER_CODE_OPERATING',
                        'ENTITY_TYPE_QUALIFIER_OPERATING',
                        'LAST_NAME_ORG_OPERATING',
                        'FIRST_NAME_OPERATING',
                        'MIDDLE_NAME_OPERATING',
                        'NAME_PREFIX_OPERATING',
                        'NAME_SUFFIX_OPERATING',
                        'ID_CODE_QUALIFIER_OPERATING',
                        'ID_CODE_OPERATING'
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
                    CLAIM_INDEX,
                    NAME_CODE_OPERATING,
                    ENTITY_IDENTIFIER_CODE_OPERATING,
                    ENTITY_TYPE_QUALIFIER_OPERATING,
                    LAST_NAME_ORG_OPERATING,
                    FIRST_NAME_OPERATING,
                    MIDDLE_NAME_OPERATING,
                    NAME_PREFIX_OPERATING,
                    NAME_SUFFIX_OPERATING,
                    ID_CODE_QUALIFIER_OPERATING,
                    ID_CODE_OPERATING
                )
)
, claim_nm72_prv as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PRV_PREFIX_OPERATING'
                            when    flattened.index = 2   then      'PROVIDER_CODE_OPERATING'
                            when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_OPERATING'
                            when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_OPERATING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^PRV.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*72'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PRV_PREFIX_OPERATING',
                        'PROVIDER_CODE_OPERATING',
                        'REFERENCE_ID_QUALIFIER_OPERATING',
                        'PROVIDER_TAXONOMY_CODE_OPERATING'
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
                    CLAIM_INDEX,
                    PRV_PREFIX_OPERATING,
                    PROVIDER_CODE_OPERATING,
                    REFERENCE_ID_QUALIFIER_OPERATING,
                    PROVIDER_TAXONOMY_CODE_OPERATING
                )
)

, clm_ref_flattened as
(
    select      claims.response_id,
                claims.nth_functional_group,
                claims.nth_transaction_set,
                claims.claim_index,
                flattened.value['claim_ref_code']           ::varchar as claim_ref_code,
                flattened.value['claim_ref_description']    ::varchar as claim_ref_description,
                flattened.value['claim_ref_value']          ::varchar as claim_ref_value
                
    from        claim_ref as claims,
                lateral flatten(input => clm_ref_array) as flattened
)
, clm_ref_ea as
(
    select      *
    from        clm_ref_flattened
    where       claim_ref_code = 'EA'
    --Ensure uniqueness
    qualify     row_number() over (partition by response_id, nth_functional_group, nth_transaction_set, claim_index order by claim_ref_value asc) = 1
)
, clm_ref_g1 as
(
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                claim_index,
                array_agg(claim_ref_value) as claim_ref_value_array
    from        clm_ref_flattened
    where       claim_ref_code = 'G1'
    group by    1,2,3,4
)

select      header.response_id,
            header.nth_functional_group,
            header.nth_transaction_set,
            header.index,
            header.hl_index_current,
            header.hl_index_billing_20,
            header.hl_index_subscriber_22,
            header.hl_index_patient_23,
            header.claim_index,
            header.clm_prefix,
            header.claim_id,
            header.total_claim_charge,
            header.claim_payment_amount,
            header.patient_responsibility_amount,
            header.composite_facility_code,
            header.provider_signature,
            header.participation_code,
            header.benefits_assignment_indicator,
            header.release_of_info_code,
            dtp_434.dtp_prefix_claim,
            dtp_434.date_qualifier_claim,
            dtp_434.date_format_claim,
            dtp_434.date_range_claim,
            dtp_434.start_date_claim,
            dtp_434.end_date_claim,
            dtp_435.dtp_prefix_claim_admit,
            dtp_435.date_qualifier_claim_admit,
            dtp_435.date_format_claim_admit,
            dtp_435.datetime_claim_admit,
            dtp_435.admit_date_claim,
            dtp_435.admit_hour_claim,
            dtp_096.dtp_prefix_claim_time,
            dtp_096.date_qualifier_claim_time,
            dtp_096.date_format_claim_time,
            dtp_096.time_claim_time,
            cl1.cl1_prefix,
            cl1.admission_type_code,
            cl1.admission_source_code,
            cl1.patient_status_code,
            nm71.name_code_attending,
            nm71.entity_identifier_code_attending,
            nm71.entity_type_qualifier_attending,
            nm71.last_name_org_attending,
            nm71.first_name_attending,
            nm71.middle_name_attending,
            nm71.name_prefix_attending,
            nm71.name_suffix_attending,
            nm71.id_code_qualifier_attending,
            nm71.id_code_attending,
            nm71_prv.prv_prefix_attending,
            nm71_prv.provider_code_attending,
            nm71_prv.reference_id_qualifier_attending,
            nm71_prv.provider_taxonomy_code_attending,
            nm72.name_code_operating,
            nm72.entity_identifier_code_operating,
            nm72.entity_type_qualifier_operating,
            nm72.last_name_org_operating,
            nm72.first_name_operating,
            nm72.middle_name_operating,
            nm72.name_prefix_operating,
            nm72.name_suffix_operating,
            nm72.id_code_qualifier_operating,
            nm72.id_code_operating,
            nm72_prv.prv_prefix_operating,
            nm72_prv.provider_code_operating,
            nm72_prv.reference_id_qualifier_operating,
            nm72_prv.provider_taxonomy_code_operating,

            ref.clm_ref_array,
            hi.clm_hi_array,

            clm_ref_ea.claim_ref_value          as clm_ref_medical_record_num,
            clm_ref_g1.claim_ref_value_array    as clm_ref_treatment_auth_codes_array

from        header_clm      as header
            left join
                claim_dtp_434   as dtp_434
                on  header.response_id          = dtp_434.response_id
                and header.nth_functional_group = dtp_434.nth_functional_group
                and header.nth_transaction_set  = dtp_434.nth_transaction_set
                and header.claim_index          = dtp_434.claim_index
            left join
                claim_dtp_435   as dtp_435
                on  header.response_id          = dtp_435.response_id
                and header.nth_functional_group = dtp_435.nth_functional_group
                and header.nth_transaction_set  = dtp_435.nth_transaction_set
                and header.claim_index          = dtp_435.claim_index
            left join
                claim_dtp_096   as dtp_096
                on  header.response_id          = dtp_096.response_id
                and header.nth_functional_group = dtp_096.nth_functional_group
                and header.nth_transaction_set  = dtp_096.nth_transaction_set
                and header.claim_index          = dtp_096.claim_index
            left join
                claim_cl1       as cl1
                on  header.response_id          = cl1.response_id
                and header.nth_functional_group = cl1.nth_functional_group
                and header.nth_transaction_set  = cl1.nth_transaction_set
                and header.claim_index          = cl1.claim_index
            left join
                claim_nm71      as nm71
                on  header.response_id          = nm71.response_id
                and header.nth_functional_group = nm71.nth_functional_group
                and header.nth_transaction_set  = nm71.nth_transaction_set
                and header.claim_index          = nm71.claim_index
            left join
                claim_nm71_prv  as nm71_prv
                on  header.response_id          = nm71_prv.response_id
                and header.nth_functional_group = nm71_prv.nth_functional_group
                and header.nth_transaction_set  = nm71_prv.nth_transaction_set
                and header.claim_index          = nm71_prv.claim_index
            left join
                claim_nm72      as nm72
                on  header.response_id          = nm72.response_id
                and header.nth_functional_group = nm72.nth_functional_group
                and header.nth_transaction_set  = nm72.nth_transaction_set
                and header.claim_index          = nm72.claim_index
            left join
                claim_nm72_prv  as nm72_prv
                on  header.response_id          = nm72_prv.response_id
                and header.nth_functional_group = nm72_prv.nth_functional_group
                and header.nth_transaction_set  = nm72_prv.nth_transaction_set
                and header.claim_index          = nm72_prv.claim_index
                
            left join
                claim_ref       as ref
                on  header.response_id          = ref.response_id
                and header.nth_functional_group = ref.nth_functional_group
                and header.nth_transaction_set  = ref.nth_transaction_set
                and header.claim_index          = ref.claim_index
            left join
                claim_hi        as hi
                on  header.response_id          = hi.response_id
                and header.nth_functional_group = hi.nth_functional_group
                and header.nth_transaction_set  = hi.nth_transaction_set
                and header.claim_index          = hi.claim_index

            left join
                clm_ref_ea
                on  header.response_id          = clm_ref_ea.response_id
                and header.nth_functional_group = clm_ref_ea.nth_functional_group
                and header.nth_transaction_set  = clm_ref_ea.nth_transaction_set
                and header.claim_index          = clm_ref_ea.claim_index
            left join
                clm_ref_g1
                on  header.response_id          = clm_ref_g1.response_id
                and header.nth_functional_group = clm_ref_g1.nth_functional_group
                and header.nth_transaction_set  = clm_ref_g1.nth_transaction_set
                and header.claim_index          = clm_ref_g1.claim_index                

order by    1,2,3
;



create or replace task
    edwprodhh.edi_837i_parser.insert_claims
    warehouse = analysis_wh
    after edwprodhh.edi_837i_parser.insert_response_flat
as
insert into
    edwprodhh.edi_837i_parser.claims
(
    RESPONSE_ID,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    INDEX,
    HL_INDEX_CURRENT,
    HL_INDEX_BILLING_20,
    HL_INDEX_SUBSCRIBER_22,
    HL_INDEX_PATIENT_23,
    CLAIM_INDEX,
    CLM_PREFIX,
    CLAIM_ID,
    TOTAL_CLAIM_CHARGE,
    CLAIM_PAYMENT_AMOUNT,
    PATIENT_RESPONSIBILITY_AMOUNT,
    COMPOSITE_FACILITY_CODE,
    PROVIDER_SIGNATURE,
    PARTICIPATION_CODE,
    BENEFITS_ASSIGNMENT_INDICATOR,
    RELEASE_OF_INFO_CODE,
    DTP_PREFIX_CLAIM,
    DATE_QUALIFIER_CLAIM,
    DATE_FORMAT_CLAIM,
    DATE_RANGE_CLAIM,
    START_DATE_CLAIM,
    END_DATE_CLAIM,
    DTP_PREFIX_CLAIM_ADMIT,
    DATE_QUALIFIER_CLAIM_ADMIT,
    DATE_FORMAT_CLAIM_ADMIT,
    DATETIME_CLAIM_ADMIT,
    ADMIT_DATE_CLAIM,
    ADMIT_HOUR_CLAIM,
    DTP_PREFIX_CLAIM_TIME,
    DATE_QUALIFIER_CLAIM_TIME,
    DATE_FORMAT_CLAIM_TIME,
    TIME_CLAIM_TIME,
    CL1_PREFIX,
    ADMISSION_TYPE_CODE,
    ADMISSION_SOURCE_CODE,
    PATIENT_STATUS_CODE,
    NAME_CODE_ATTENDING,
    ENTITY_IDENTIFIER_CODE_ATTENDING,
    ENTITY_TYPE_QUALIFIER_ATTENDING,
    LAST_NAME_ORG_ATTENDING,
    FIRST_NAME_ATTENDING,
    MIDDLE_NAME_ATTENDING,
    NAME_PREFIX_ATTENDING,
    NAME_SUFFIX_ATTENDING,
    ID_CODE_QUALIFIER_ATTENDING,
    ID_CODE_ATTENDING,
    PRV_PREFIX_ATTENDING,
    PROVIDER_CODE_ATTENDING,
    REFERENCE_ID_QUALIFIER_ATTENDING,
    PROVIDER_TAXONOMY_CODE_ATTENDING,
    NAME_CODE_OPERATING,
    ENTITY_IDENTIFIER_CODE_OPERATING,
    ENTITY_TYPE_QUALIFIER_OPERATING,
    LAST_NAME_ORG_OPERATING,
    FIRST_NAME_OPERATING,
    MIDDLE_NAME_OPERATING,
    NAME_PREFIX_OPERATING,
    NAME_SUFFIX_OPERATING,
    ID_CODE_QUALIFIER_OPERATING,
    ID_CODE_OPERATING,
    PRV_PREFIX_OPERATING,
    PROVIDER_CODE_OPERATING,
    REFERENCE_ID_QUALIFIER_OPERATING,
    PROVIDER_TAXONOMY_CODE_OPERATING,
    CLM_REF_ARRAY,
    CLM_HI_ARRAY,
    CLM_REF_MEDICAL_RECORD_NUM,
    CLM_REF_TREATMENT_AUTH_CODES_ARRAY
)
with filtered_clm as
(
    select      *
    from        edwprodhh.edi_837i_parser.response_flat
    where       claim_index is not null --0 Pre-Filter
                and response_id not in (select response_id from edwprodhh.edi_837i_parser.claims)
)
, header_clm as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CLM_PREFIX'
                            when    flattened.index = 2   then      'CLAIM_ID'
                            when    flattened.index = 3   then      'TOTAL_CLAIM_CHARGE'
                            when    flattened.index = 4   then      'CLAIM_PAYMENT_AMOUNT'
                            when    flattened.index = 5   then      'PATIENT_RESPONSIBILITY_AMOUNT'
                            when    flattened.index = 6   then      'COMPOSITE_FACILITY_CODE'           --11/13/21 Office/Hospital/Inpatient : ...
                            when    flattened.index = 7   then      'PROVIDER_SIGNATURE'
                            when    flattened.index = 8   then      'PARTICIPATION_CODE'
                            when    flattened.index = 9   then      'BENEFITS_ASSIGNMENT_INDICATOR'
                            when    flattened.index = 10  then      'RELEASE_OF_INFO_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^CLM.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'CLM_PREFIX',
                        'CLAIM_ID',
                        'TOTAL_CLAIM_CHARGE',
                        'CLAIM_PAYMENT_AMOUNT',
                        'PATIENT_RESPONSIBILITY_AMOUNT',
                        'COMPOSITE_FACILITY_CODE',
                        'PROVIDER_SIGNATURE',
                        'PARTICIPATION_CODE',
                        'BENEFITS_ASSIGNMENT_INDICATOR',
                        'RELEASE_OF_INFO_CODE'
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
                    CLAIM_INDEX,
                    CLM_PREFIX,
                    CLAIM_ID,
                    TOTAL_CLAIM_CHARGE,
                    CLAIM_PAYMENT_AMOUNT,
                    PATIENT_RESPONSIBILITY_AMOUNT,
                    COMPOSITE_FACILITY_CODE,
                    PROVIDER_SIGNATURE,
                    PARTICIPATION_CODE,
                    BENEFITS_ASSIGNMENT_INDICATOR,
                    RELEASE_OF_INFO_CODE
                )
)
, claim_dtp_434 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_CLAIM'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_CLAIM'
                            when    flattened.index = 3   then      'DATE_FORMAT_CLAIM'
                            when    flattened.index = 4   then      'DATE_RANGE_CLAIM'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*434.*')                          --1 Filter
    )
    select      *,
                case    when    date_format_claim = 'RD8'
                        and     regexp_like(date_range_claim, '^\\d{8}\\-\\d{8}$')
                        then    to_date(left(date_range_claim,  8), 'YYYYMMDD')
                        else    NULL
                        end     as start_date_claim,

                case    when    date_format_claim = 'RD8'
                        and     regexp_like(date_range_claim, '^\\d{8}\\-\\d{8}$')
                        then    to_date(right(date_range_claim, 8), 'YYYYMMDD')
                        else    NULL
                        end     as end_date_claim
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_CLAIM',
                        'DATE_QUALIFIER_CLAIM',
                        'DATE_FORMAT_CLAIM',
                        'DATE_RANGE_CLAIM'
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
                    CLAIM_INDEX,
                    DTP_PREFIX_CLAIM,
                    DATE_QUALIFIER_CLAIM,
                    DATE_FORMAT_CLAIM,
                    DATE_RANGE_CLAIM
                )
)
, claim_dtp_435 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_CLAIM_ADMIT'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_CLAIM_ADMIT'
                            when    flattened.index = 3   then      'DATE_FORMAT_CLAIM_ADMIT'
                            when    flattened.index = 4   then      'DATETIME_CLAIM_ADMIT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*435.*')                          --1 Filter
    )
    select      *,
                case    when    date_format_claim_admit = 'DT'
                        and     regexp_like(datetime_claim_admit, '^\\d{12}$')
                        then    to_date(left(datetime_claim_admit,  8), 'YYYYMMDD')
                        else    NULL
                        end     as admit_date_claim,

                case    when    date_format_claim_admit = 'DT'
                        and     regexp_like(datetime_claim_admit, '^\\d{12}$')
                        then    left(right(datetime_claim_admit, 4), 2)
                        else    NULL
                        end     as admit_hour_claim
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_CLAIM_ADMIT',
                        'DATE_QUALIFIER_CLAIM_ADMIT',
                        'DATE_FORMAT_CLAIM_ADMIT',
                        'DATETIME_CLAIM_ADMIT'
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
                    CLAIM_INDEX,
                    DTP_PREFIX_CLAIM_ADMIT,
                    DATE_QUALIFIER_CLAIM_ADMIT,
                    DATE_FORMAT_CLAIM_ADMIT,
                    DATETIME_CLAIM_ADMIT
                )
)
, claim_dtp_096 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_CLAIM_TIME'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_CLAIM_TIME'
                            when    flattened.index = 3   then      'DATE_FORMAT_CLAIM_TIME'
                            when    flattened.index = 4   then      'TIME_CLAIM_TIME'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*096.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_CLAIM_TIME',
                        'DATE_QUALIFIER_CLAIM_TIME',
                        'DATE_FORMAT_CLAIM_TIME',
                        'TIME_CLAIM_TIME'
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
                    CLAIM_INDEX,
                    DTP_PREFIX_CLAIM_TIME,
                    DATE_QUALIFIER_CLAIM_TIME,
                    DATE_FORMAT_CLAIM_TIME,
                    TIME_CLAIM_TIME
                )
)
, claim_cl1 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CL1_PREFIX'                
                            when    flattened.index = 2   then      'ADMISSION_TYPE_CODE'       --1/2/3/4/5 Emergency/Urgent/Elective/Newbord/Trauma
                            when    flattened.index = 3   then      'ADMISSION_SOURCE_CODE'     --1/2/3/7/9 Phys Referral/Clinic Referral/HMO Referral/ER/Unavailable
                            when    flattened.index = 4   then      'PATIENT_STATUS_CODE'       --1/2/7/20/30 Discharge home/Discharge short term hospital/Left/Expired/Still a patient
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^CL1.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'CL1_PREFIX',
                        'ADMISSION_TYPE_CODE',
                        'ADMISSION_SOURCE_CODE',
                        'PATIENT_STATUS_CODE'
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
                    CLAIM_INDEX,
                    CL1_PREFIX,
                    ADMISSION_TYPE_CODE,
                    ADMISSION_SOURCE_CODE,
                    PATIENT_STATUS_CODE
                )
)
, claim_ref as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_PREFIX_CLAIM'
                            when    flattened.index = 2   then      'REFERENCE_ID_CODE_CLAIM'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'REFERENCE_ID_CLAIM'
                            when    flattened.index = 4   then      'DESCRIPTION_CLAIM'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^REF.*')                            --1 Filter
                    and filtered_clm.claim_index is not null
                    and filtered_clm.lx_index is null
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'REF_PREFIX_CLAIM',
                            'REFERENCE_ID_CODE_CLAIM',
                            'REFERENCE_ID_CLAIM',
                            'DESCRIPTION_CLAIM'
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
                        CLAIM_INDEX,
                        REF_PREFIX_CLAIM,
                        REFERENCE_ID_CODE_CLAIM,
                        REFERENCE_ID_CLAIM,
                        DESCRIPTION_CLAIM
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                claim_index,
                array_agg(
                    object_construct_keep_null(
                        'claim_ref_code',           reference_id_code_claim::varchar,
                        'claim_ref_value',          reference_id_claim::varchar,
                        'claim_ref_description',    description_claim::varchar
                    )
                )   as clm_ref_array
    from        pivoted
    group by    1,2,3,4
    order by    1,2,3,4
)
, claim_hi as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'HI_PREFIX'
                            when    flattened.index = 2   then      'HI_VAL01'
                            when    flattened.index = 3   then      'HI_VAL02'
                            when    flattened.index = 4   then      'HI_VAL03'
                            when    flattened.index = 5   then      'HI_VAL04'
                            when    flattened.index = 6   then      'HI_VAL05'
                            when    flattened.index = 7   then      'HI_VAL06'
                            when    flattened.index = 8   then      'HI_VAL07'
                            when    flattened.index = 9   then      'HI_VAL08'
                            when    flattened.index = 10  then      'HI_VAL09'
                            when    flattened.index = 11  then      'HI_VAL10'
                            when    flattened.index = 12  then      'HI_VAL11'
                            when    flattened.index = 13  then      'HI_VAL12'
                            when    flattened.index = 14  then      'HI_VAL13'
                            when    flattened.index = 15  then      'HI_VAL14'
                            when    flattened.index = 16  then      'HI_VAL15'
                            when    flattened.index = 17  then      'HI_VAL16'
                            when    flattened.index = 18  then      'HI_VAL17'
                            when    flattened.index = 19  then      'HI_VAL18'
                            when    flattened.index = 20 then       'HI_VAL19'
                            when    flattened.index = 21  then      'HI_VAL20'
                            when    flattened.index = 22  then      'HI_VAL21'
                            when    flattened.index = 23  then      'HI_VAL22'
                            when    flattened.index = 24  then      'HI_VAL23'
                            when    flattened.index = 25  then      'HI_VAL24'
                            when    flattened.index = 26  then      'HI_VAL25'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^HI.*')                            --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'HI_PREFIX',
                            'HI_VAL01',
                            'HI_VAL02',
                            'HI_VAL03',
                            'HI_VAL04',
                            'HI_VAL05',
                            'HI_VAL06',
                            'HI_VAL07',
                            'HI_VAL08',
                            'HI_VAL09',
                            'HI_VAL10',
                            'HI_VAL11',
                            'HI_VAL12',
                            'HI_VAL13',
                            'HI_VAL14',
                            'HI_VAL15',
                            'HI_VAL16',
                            'HI_VAL17',
                            'HI_VAL18',
                            'HI_VAL19',
                            'HI_VAL20',
                            'HI_VAL21',
                            'HI_VAL22',
                            'HI_VAL23',
                            'HI_VAL24',
                            'HI_VAL25'
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
                        CLAIM_INDEX,
                        HI_PREFIX,
                        HI_VAL01,
                        HI_VAL02,
                        HI_VAL03,
                        HI_VAL04,
                        HI_VAL05,
                        HI_VAL06,
                        HI_VAL07,
                        HI_VAL08,
                        HI_VAL09,
                        HI_VAL10,
                        HI_VAL11,
                        HI_VAL12,
                        HI_VAL13,
                        HI_VAL14,
                        HI_VAL15,
                        HI_VAL16,
                        HI_VAL17,
                        HI_VAL18,
                        HI_VAL19,
                        HI_VAL20,
                        HI_VAL21,
                        HI_VAL22,
                        HI_VAL23,
                        HI_VAL24,
                        HI_VAL25
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                claim_index,
                array_agg(unpvt.metric_value) as clm_hi_array
    from        pivoted
                unpivot (
                    metric_value for metric_name in (
                        HI_VAL01,
                        HI_VAL02,
                        HI_VAL03,
                        HI_VAL04,
                        HI_VAL05,
                        HI_VAL06,
                        HI_VAL07,
                        HI_VAL08,
                        HI_VAL09,
                        HI_VAL10,
                        HI_VAL11,
                        HI_VAL12,
                        HI_VAL13,
                        HI_VAL14,
                        HI_VAL15,
                        HI_VAL16,
                        HI_VAL17,
                        HI_VAL18,
                        HI_VAL19,
                        HI_VAL20,
                        HI_VAL21,
                        HI_VAL22,
                        HI_VAL23,
                        HI_VAL24,
                        HI_VAL25
                    )
                )   as unpvt
    group by    1,2,3,4
    order by    1,2,3,4
)
, claim_nm71 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_ATTENDING'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_ATTENDING'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_ATTENDING'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_ATTENDING'
                            when    flattened.index = 5   then      'FIRST_NAME_ATTENDING'
                            when    flattened.index = 6   then      'MIDDLE_NAME_ATTENDING'
                            when    flattened.index = 7   then      'NAME_PREFIX_ATTENDING'
                            when    flattened.index = 8   then      'NAME_SUFFIX_ATTENDING'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_ATTENDING'
                            when    flattened.index = 10  then      'ID_CODE_ATTENDING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*71.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_ATTENDING',
                        'ENTITY_IDENTIFIER_CODE_ATTENDING',
                        'ENTITY_TYPE_QUALIFIER_ATTENDING',
                        'LAST_NAME_ORG_ATTENDING',
                        'FIRST_NAME_ATTENDING',
                        'MIDDLE_NAME_ATTENDING',
                        'NAME_PREFIX_ATTENDING',
                        'NAME_SUFFIX_ATTENDING',
                        'ID_CODE_QUALIFIER_ATTENDING',
                        'ID_CODE_ATTENDING'
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
                    CLAIM_INDEX,
                    NAME_CODE_ATTENDING,
                    ENTITY_IDENTIFIER_CODE_ATTENDING,
                    ENTITY_TYPE_QUALIFIER_ATTENDING,
                    LAST_NAME_ORG_ATTENDING,
                    FIRST_NAME_ATTENDING,
                    MIDDLE_NAME_ATTENDING,
                    NAME_PREFIX_ATTENDING,
                    NAME_SUFFIX_ATTENDING,
                    ID_CODE_QUALIFIER_ATTENDING,
                    ID_CODE_ATTENDING
                )
)
, claim_nm71_prv as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PRV_PREFIX_ATTENDING'
                            when    flattened.index = 2   then      'PROVIDER_CODE_ATTENDING'
                            when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_ATTENDING'
                            when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_ATTENDING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^PRV.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*71'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PRV_PREFIX_ATTENDING',
                        'PROVIDER_CODE_ATTENDING',
                        'REFERENCE_ID_QUALIFIER_ATTENDING',
                        'PROVIDER_TAXONOMY_CODE_ATTENDING'
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
                    CLAIM_INDEX,
                    PRV_PREFIX_ATTENDING,
                    PROVIDER_CODE_ATTENDING,
                    REFERENCE_ID_QUALIFIER_ATTENDING,
                    PROVIDER_TAXONOMY_CODE_ATTENDING
                )
)
, claim_nm72 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_OPERATING'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_OPERATING'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_OPERATING'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_OPERATING'
                            when    flattened.index = 5   then      'FIRST_NAME_OPERATING'
                            when    flattened.index = 6   then      'MIDDLE_NAME_OPERATING'
                            when    flattened.index = 7   then      'NAME_PREFIX_OPERATING'
                            when    flattened.index = 8   then      'NAME_SUFFIX_OPERATING'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_OPERATING'
                            when    flattened.index = 10  then      'ID_CODE_OPERATING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*72.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_OPERATING',
                        'ENTITY_IDENTIFIER_CODE_OPERATING',
                        'ENTITY_TYPE_QUALIFIER_OPERATING',
                        'LAST_NAME_ORG_OPERATING',
                        'FIRST_NAME_OPERATING',
                        'MIDDLE_NAME_OPERATING',
                        'NAME_PREFIX_OPERATING',
                        'NAME_SUFFIX_OPERATING',
                        'ID_CODE_QUALIFIER_OPERATING',
                        'ID_CODE_OPERATING'
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
                    CLAIM_INDEX,
                    NAME_CODE_OPERATING,
                    ENTITY_IDENTIFIER_CODE_OPERATING,
                    ENTITY_TYPE_QUALIFIER_OPERATING,
                    LAST_NAME_ORG_OPERATING,
                    FIRST_NAME_OPERATING,
                    MIDDLE_NAME_OPERATING,
                    NAME_PREFIX_OPERATING,
                    NAME_SUFFIX_OPERATING,
                    ID_CODE_QUALIFIER_OPERATING,
                    ID_CODE_OPERATING
                )
)
, claim_nm72_prv as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_functional_group,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PRV_PREFIX_OPERATING'
                            when    flattened.index = 2   then      'PROVIDER_CODE_OPERATING'
                            when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_OPERATING'
                            when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_OPERATING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^PRV.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*72'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PRV_PREFIX_OPERATING',
                        'PROVIDER_CODE_OPERATING',
                        'REFERENCE_ID_QUALIFIER_OPERATING',
                        'PROVIDER_TAXONOMY_CODE_OPERATING'
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
                    CLAIM_INDEX,
                    PRV_PREFIX_OPERATING,
                    PROVIDER_CODE_OPERATING,
                    REFERENCE_ID_QUALIFIER_OPERATING,
                    PROVIDER_TAXONOMY_CODE_OPERATING
                )
)

, clm_ref_flattened as
(
    select      claims.response_id,
                claims.nth_functional_group,
                claims.nth_transaction_set,
                claims.claim_index,
                flattened.value['claim_ref_code']           ::varchar as claim_ref_code,
                flattened.value['claim_ref_description']    ::varchar as claim_ref_description,
                flattened.value['claim_ref_value']          ::varchar as claim_ref_value
                
    from        claim_ref as claims,
                lateral flatten(input => clm_ref_array) as flattened
)
, clm_ref_ea as
(
    select      *
    from        clm_ref_flattened
    where       claim_ref_code = 'EA'
    --Ensure uniqueness
    qualify     row_number() over (partition by response_id, nth_functional_group, nth_transaction_set, claim_index order by claim_ref_value asc) = 1
)
, clm_ref_g1 as
(
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                claim_index,
                array_agg(claim_ref_value) as claim_ref_value_array
    from        clm_ref_flattened
    where       claim_ref_code = 'G1'
    group by    1,2,3,4
)

select      header.response_id,
            header.nth_functional_group,
            header.nth_transaction_set,
            header.index,
            header.hl_index_current,
            header.hl_index_billing_20,
            header.hl_index_subscriber_22,
            header.hl_index_patient_23,
            header.claim_index,
            header.clm_prefix,
            header.claim_id,
            header.total_claim_charge,
            header.claim_payment_amount,
            header.patient_responsibility_amount,
            header.composite_facility_code,
            header.provider_signature,
            header.participation_code,
            header.benefits_assignment_indicator,
            header.release_of_info_code,
            dtp_434.dtp_prefix_claim,
            dtp_434.date_qualifier_claim,
            dtp_434.date_format_claim,
            dtp_434.date_range_claim,
            dtp_434.start_date_claim,
            dtp_434.end_date_claim,
            dtp_435.dtp_prefix_claim_admit,
            dtp_435.date_qualifier_claim_admit,
            dtp_435.date_format_claim_admit,
            dtp_435.datetime_claim_admit,
            dtp_435.admit_date_claim,
            dtp_435.admit_hour_claim,
            dtp_096.dtp_prefix_claim_time,
            dtp_096.date_qualifier_claim_time,
            dtp_096.date_format_claim_time,
            dtp_096.time_claim_time,
            cl1.cl1_prefix,
            cl1.admission_type_code,
            cl1.admission_source_code,
            cl1.patient_status_code,
            nm71.name_code_attending,
            nm71.entity_identifier_code_attending,
            nm71.entity_type_qualifier_attending,
            nm71.last_name_org_attending,
            nm71.first_name_attending,
            nm71.middle_name_attending,
            nm71.name_prefix_attending,
            nm71.name_suffix_attending,
            nm71.id_code_qualifier_attending,
            nm71.id_code_attending,
            nm71_prv.prv_prefix_attending,
            nm71_prv.provider_code_attending,
            nm71_prv.reference_id_qualifier_attending,
            nm71_prv.provider_taxonomy_code_attending,
            nm72.name_code_operating,
            nm72.entity_identifier_code_operating,
            nm72.entity_type_qualifier_operating,
            nm72.last_name_org_operating,
            nm72.first_name_operating,
            nm72.middle_name_operating,
            nm72.name_prefix_operating,
            nm72.name_suffix_operating,
            nm72.id_code_qualifier_operating,
            nm72.id_code_operating,
            nm72_prv.prv_prefix_operating,
            nm72_prv.provider_code_operating,
            nm72_prv.reference_id_qualifier_operating,
            nm72_prv.provider_taxonomy_code_operating,

            ref.clm_ref_array,
            hi.clm_hi_array,

            clm_ref_ea.claim_ref_value          as clm_ref_medical_record_num,
            clm_ref_g1.claim_ref_value_array    as clm_ref_treatment_auth_codes_array

from        header_clm      as header
            left join
                claim_dtp_434   as dtp_434
                on  header.response_id          = dtp_434.response_id
                and header.nth_functional_group = dtp_434.nth_functional_group
                and header.nth_transaction_set  = dtp_434.nth_transaction_set
                and header.claim_index          = dtp_434.claim_index
            left join
                claim_dtp_435   as dtp_435
                on  header.response_id          = dtp_435.response_id
                and header.nth_functional_group = dtp_435.nth_functional_group
                and header.nth_transaction_set  = dtp_435.nth_transaction_set
                and header.claim_index          = dtp_435.claim_index
            left join
                claim_dtp_096   as dtp_096
                on  header.response_id          = dtp_096.response_id
                and header.nth_functional_group = dtp_096.nth_functional_group
                and header.nth_transaction_set  = dtp_096.nth_transaction_set
                and header.claim_index          = dtp_096.claim_index
            left join
                claim_cl1       as cl1
                on  header.response_id          = cl1.response_id
                and header.nth_functional_group = cl1.nth_functional_group
                and header.nth_transaction_set  = cl1.nth_transaction_set
                and header.claim_index          = cl1.claim_index
            left join
                claim_nm71      as nm71
                on  header.response_id          = nm71.response_id
                and header.nth_functional_group = nm71.nth_functional_group
                and header.nth_transaction_set  = nm71.nth_transaction_set
                and header.claim_index          = nm71.claim_index
            left join
                claim_nm71_prv  as nm71_prv
                on  header.response_id          = nm71_prv.response_id
                and header.nth_functional_group = nm71_prv.nth_functional_group
                and header.nth_transaction_set  = nm71_prv.nth_transaction_set
                and header.claim_index          = nm71_prv.claim_index
            left join
                claim_nm72      as nm72
                on  header.response_id          = nm72.response_id
                and header.nth_functional_group = nm72.nth_functional_group
                and header.nth_transaction_set  = nm72.nth_transaction_set
                and header.claim_index          = nm72.claim_index
            left join
                claim_nm72_prv  as nm72_prv
                on  header.response_id          = nm72_prv.response_id
                and header.nth_functional_group = nm72_prv.nth_functional_group
                and header.nth_transaction_set  = nm72_prv.nth_transaction_set
                and header.claim_index          = nm72_prv.claim_index
                
            left join
                claim_ref       as ref
                on  header.response_id          = ref.response_id
                and header.nth_functional_group = ref.nth_functional_group
                and header.nth_transaction_set  = ref.nth_transaction_set
                and header.claim_index          = ref.claim_index
            left join
                claim_hi        as hi
                on  header.response_id          = hi.response_id
                and header.nth_functional_group = hi.nth_functional_group
                and header.nth_transaction_set  = hi.nth_transaction_set
                and header.claim_index          = hi.claim_index

            left join
                clm_ref_ea
                on  header.response_id          = clm_ref_ea.response_id
                and header.nth_functional_group = clm_ref_ea.nth_functional_group
                and header.nth_transaction_set  = clm_ref_ea.nth_transaction_set
                and header.claim_index          = clm_ref_ea.claim_index
            left join
                clm_ref_g1
                on  header.response_id          = clm_ref_g1.response_id
                and header.nth_functional_group = clm_ref_g1.nth_functional_group
                and header.nth_transaction_set  = clm_ref_g1.nth_transaction_set
                and header.claim_index          = clm_ref_g1.claim_index                

order by    1,2,3
;