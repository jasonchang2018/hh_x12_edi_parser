create or replace table
    edwprodhh.edi_837i_parser.claim_additional_subscribers
as
with filtered_clm_sbr as
(
    select      *
    from        edwprodhh.edi_837i_parser.response_flat
    where       claim_index is not null --0 Pre-Filter
                and other_sbr_index is not null
                and lx_index is null
)
, claim_sbr_header as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'SBR_PREFIX_OTHERSBR'
                            when    flattened.index = 2   then      'PAYOR_RESPONSIBILITY_SEQUENCE_OTHERSBR'
                            when    flattened.index = 3   then      'INDIVIDUAL_RELATIONSHIP_CODE_OTHERSBR'
                            when    flattened.index = 4   then      'GROUP_NUMBER_OTHERSBR'
                            when    flattened.index = 5   then      'GROUP_NAME_OTHERSBR'
                            when    flattened.index = 6   then      'INSURANCE_TYPE_CODE_OTHERSBR'
                            when    flattened.index = 7   then      'COORDINATION_OF_BENEFITS_CODE_OTHERSBR'
                            when    flattened.index = 8   then      'EMPLOYMENT_CODE_OTHERSBR'
                            when    flattened.index = 9   then      'CLAIM_FILING_INDICATOR_CODE_OTHERSBR'
                            when    flattened.index = 10  then      'PATIENT_SIGNATURE_SOURCE_CODE_OTHERSBR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^SBR.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SBR_PREFIX_OTHERSBR',
                        'PAYOR_RESPONSIBILITY_SEQUENCE_OTHERSBR',
                        'INDIVIDUAL_RELATIONSHIP_CODE_OTHERSBR',
                        'GROUP_NUMBER_OTHERSBR',
                        'GROUP_NAME_OTHERSBR',
                        'INSURANCE_TYPE_CODE_OTHERSBR',
                        'COORDINATION_OF_BENEFITS_CODE_OTHERSBR',
                        'EMPLOYMENT_CODE_OTHERSBR',
                        'CLAIM_FILING_INDICATOR_CODE_OTHERSBR',
                        'PATIENT_SIGNATURE_SOURCE_CODE_OTHERSBR'
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
                    OTHER_SBR_INDEX,
                    SBR_PREFIX_OTHERSBR,
                    PAYOR_RESPONSIBILITY_SEQUENCE_OTHERSBR,
                    INDIVIDUAL_RELATIONSHIP_CODE_OTHERSBR,
                    GROUP_NUMBER_OTHERSBR,
                    GROUP_NAME_OTHERSBR,
                    INSURANCE_TYPE_CODE_OTHERSBR,
                    COORDINATION_OF_BENEFITS_CODE_OTHERSBR,
                    EMPLOYMENT_CODE_OTHERSBR,
                    CLAIM_FILING_INDICATOR_CODE_OTHERSBR,
                    PATIENT_SIGNATURE_SOURCE_CODE_OTHERSBR
                )
)
, claim_sbr_cas as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then      'PREFIX_CAS'
                            when    flattened.index = 2     then      'CLM_ADJ_GROUP_CODE'
                            when    flattened.index = 3     then      'ADJ_REASON_CODE_1'
                            when    flattened.index = 4     then      'ADJ_AMOUNT_1'
                            when    flattened.index = 5     then      'ADJ_QUANTITY_1'
                            when    flattened.index = 6     then      'ADJ_REASON_CODE_2'
                            when    flattened.index = 7     then      'ADJ_AMOUNT_2'
                            when    flattened.index = 8     then      'ADJ_QUANTITY_2'
                            when    flattened.index = 9     then      'ADJ_REASON_CODE_3'
                            when    flattened.index = 10    then      'ADJ_AMOUNT_3'
                            when    flattened.index = 11    then      'ADJ_QUANTITY_3'
                            when    flattened.index = 12    then      'ADJ_REASON_CODE_4'
                            when    flattened.index = 13    then      'ADJ_AMOUNT_4'
                            when    flattened.index = 14    then      'ADJ_QUANTITY_4'
                            when    flattened.index = 15    then      'ADJ_REASON_CODE_5'
                            when    flattened.index = 16    then      'ADJ_AMOUNT_5'
                            when    flattened.index = 17    then      'ADJ_QUANTITY_5'
                            when    flattened.index = 18    then      'ADJ_REASON_CODE_6'
                            when    flattened.index = 19    then      'ADJ_AMOUNT_6'
                            when    flattened.index = 20    then      'ADJ_QUANTITY_6'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^CAS.*')                         --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'PREFIX_CAS',
                            'CLM_ADJ_GROUP_CODE',
                            'ADJ_REASON_CODE_1',
                            'ADJ_AMOUNT_1',
                            'ADJ_QUANTITY_1',
                            'ADJ_REASON_CODE_2',
                            'ADJ_AMOUNT_2',
                            'ADJ_QUANTITY_2',
                            'ADJ_REASON_CODE_3',
                            'ADJ_AMOUNT_3',
                            'ADJ_QUANTITY_3',
                            'ADJ_REASON_CODE_4',
                            'ADJ_AMOUNT_4',
                            'ADJ_QUANTITY_4',
                            'ADJ_REASON_CODE_5',
                            'ADJ_AMOUNT_5',
                            'ADJ_QUANTITY_5',
                            'ADJ_REASON_CODE_6',
                            'ADJ_AMOUNT_6',
                            'ADJ_QUANTITY_6'
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
                        OTHER_SBR_INDEX,
                        PREFIX_CAS,
                        CLM_ADJ_GROUP_CODE,
                        ADJ_REASON_CODE_1,
                        ADJ_AMOUNT_1,
                        ADJ_QUANTITY_1,
                        ADJ_REASON_CODE_2,
                        ADJ_AMOUNT_2,
                        ADJ_QUANTITY_2,
                        ADJ_REASON_CODE_3,
                        ADJ_AMOUNT_3,
                        ADJ_QUANTITY_3,
                        ADJ_REASON_CODE_4,
                        ADJ_AMOUNT_4,
                        ADJ_QUANTITY_4,
                        ADJ_REASON_CODE_5,
                        ADJ_AMOUNT_5,
                        ADJ_QUANTITY_5,
                        ADJ_REASON_CODE_6,
                        ADJ_AMOUNT_6,
                        ADJ_QUANTITY_6
                    )
    )
    , unpivoted as
    (
        select      response_id,
                    nth_functional_group,
                    nth_transaction_set,
                    index,
                    hl_index_current,
                    hl_index_billing_20,
                    hl_index_subscriber_22,
                    hl_index_patient_23,
                    claim_index,
                    other_sbr_index,
                    clm_adj_group_code,
                    regexp_substr(unpvt.metric_name, '\\d+$') as nth_element,
                    regexp_replace(unpvt.metric_name, '_\\d+$', '') as metric_name,
                    metric_value
        from        pivoted
                    unpivot include nulls (
                        metric_value for metric_name in (
                            ADJ_REASON_CODE_1,
                            ADJ_AMOUNT_1,
                            ADJ_QUANTITY_1,
                            ADJ_REASON_CODE_2,
                            ADJ_AMOUNT_2,
                            ADJ_QUANTITY_2,
                            ADJ_REASON_CODE_3,
                            ADJ_AMOUNT_3,
                            ADJ_QUANTITY_3,
                            ADJ_REASON_CODE_4,
                            ADJ_AMOUNT_4,
                            ADJ_QUANTITY_4,
                            ADJ_REASON_CODE_5,
                            ADJ_AMOUNT_5,
                            ADJ_QUANTITY_5,
                            ADJ_REASON_CODE_6,
                            ADJ_AMOUNT_6,
                            ADJ_QUANTITY_6
                        )
                    )   as unpvt
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                claim_index,
                other_sbr_index,
                array_agg(
                    object_construct_keep_null(
                        'adj_group_code',   clm_adj_group_code,
                        'adj_detail',       object_construct_keep_null(
                                                'adj_reason_code',  adj_reason_code::varchar,
                                                'adj_amount',       adj_amount::number(18,2),
                                                'adj_quantity',     adj_quantity::number(18,2)
                                            )
                    )
                )   as cas_adj_array
    from        unpivoted
                pivot (
                    max(metric_value) for metric_name in (
                        'ADJ_REASON_CODE',
                        'ADJ_AMOUNT',
                        'ADJ_QUANTITY'
                    )
                )   as pvt (
                    response_id,
                    nth_functional_group,
                    nth_transaction_set,
                    index,
                    hl_index_current,
                    hl_index_billing_20,
                    hl_index_subscriber_22,
                    hl_index_patient_23,
                    claim_index,
                    other_sbr_index,
                    clm_adj_group_code,
                    nth_element,
                    adj_reason_code,
                    adj_amount,
                    adj_quantity
                )
    where       not (
                    adj_reason_code is null
                    and adj_amount is null
                    and adj_quantity is null
                )
    group by    1,2,3,4,5
    order by    1,2,3,4,5
)
, claim_sbr_amt as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then      'PREFIX_AMT'
                            when    flattened.index = 2     then      'AMT_QUALIFIER_CODE'
                            when    flattened.index = 3     then      'MONETARY_AMOUNT'
                            when    flattened.index = 4     then      'CREDIT_DEBIT_FLAG'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^AMT.*')                         --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'PREFIX_AMT',
                            'AMT_QUALIFIER_CODE',
                            'MONETARY_AMOUNT',
                            'CREDIT_DEBIT_FLAG'
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
                        OTHER_SBR_INDEX,
                        PREFIX_AMT,
                        AMT_QUALIFIER_CODE,
                        MONETARY_AMOUNT,
                        CREDIT_DEBIT_FLAG
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                claim_index,
                other_sbr_index,
                array_agg(
                    object_construct_keep_null(
                        'amt_qualifier_code',   amt_qualifier_code::varchar,
                        'monetary_amount',      monetary_amount::number(18,2),
                        'credit_debit_flag',    credit_debit_flag::varchar
                    )
                )   as amt_adj_array
    from        pivoted
    group by    1,2,3,4,5
    order by    1,2,3,4,5
)
, claim_sbr_oi as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PREFIX_OTHERSBR'
                            when    flattened.index = 2   then      'EMPTY1_OTHERSBR'
                            when    flattened.index = 3   then      'EMPTY2_OTHERSBR'
                            when    flattened.index = 4   then      'BENEFITS_ASSIGNMENT_OTHERSBR'
                            when    flattened.index = 5   then      'PATIENT_SIGNATURE_SOURCE_OTHERSBR'
                            when    flattened.index = 6   then      'EMPTY5_OTHERSBR'
                            when    flattened.index = 7   then      'RELEASE_OF_INFO_OTHERSBR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^OI.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PREFIX_OTHERSBR',
                        'EMPTY1_OTHERSBR',
                        'EMPTY2_OTHERSBR',
                        'BENEFITS_ASSIGNMENT_OTHERSBR',
                        'PATIENT_SIGNATURE_SOURCE_OTHERSBR',
                        'EMPTY5_OTHERSBR',
                        'RELEASE_OF_INFO_OTHERSBR'
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
                    OTHER_SBR_INDEX,
                    PREFIX_OTHERSBR,
                    EMPTY1_OTHERSBR,
                    EMPTY2_OTHERSBR,
                    BENEFITS_ASSIGNMENT_OTHERSBR,
                    PATIENT_SIGNATURE_SOURCE_OTHERSBR,
                    EMPTY5_OTHERSBR,
                    RELEASE_OF_INFO_OTHERSBR
                )
)
, claim_sbr_nmIL as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_OTHERSBR'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_OTHERSBR'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_OTHERSBR'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_OTHERSBR'
                            when    flattened.index = 5   then      'FIRST_NAME_OTHERSBR'
                            when    flattened.index = 6   then      'MIDDLE_NAME_OTHERSBR'
                            when    flattened.index = 7   then      'NAME_PREFIX_OTHERSBR'
                            when    flattened.index = 8   then      'NAME_SUFFIX_OTHERSBR'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_OTHERSBR'
                            when    flattened.index = 10  then      'ID_CODE_OTHERSBR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^NM1\\*IL.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_OTHERSBR',
                        'ENTITY_IDENTIFIER_CODE_OTHERSBR',
                        'ENTITY_TYPE_QUALIFIER_OTHERSBR',
                        'LAST_NAME_ORG_OTHERSBR',
                        'FIRST_NAME_OTHERSBR',
                        'MIDDLE_NAME_OTHERSBR',
                        'NAME_PREFIX_OTHERSBR',
                        'NAME_SUFFIX_OTHERSBR',
                        'ID_CODE_QUALIFIER_OTHERSBR',
                        'ID_CODE_OTHERSBR'
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
                    OTHER_SBR_INDEX,
                    NAME_CODE_OTHERSBR,
                    ENTITY_IDENTIFIER_CODE_OTHERSBR,
                    ENTITY_TYPE_QUALIFIER_OTHERSBR,
                    LAST_NAME_ORG_OTHERSBR,
                    FIRST_NAME_OTHERSBR,
                    MIDDLE_NAME_OTHERSBR,
                    NAME_PREFIX_OTHERSBR,
                    NAME_SUFFIX_OTHERSBR,
                    ID_CODE_QUALIFIER_OTHERSBR,
                    ID_CODE_OTHERSBR_NMIL
                )
)
, claim_sbr_nmIL_n3 as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_OTHERSBR'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_OTHERSBR'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_OTHERSBR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_clm_sbr.lag_name_indicator = 'NM1*IL'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_OTHERSBR',
                        'ADDRESS_LINE_1_OTHERSBR',
                        'ADDRESS_LINE_2_OTHERSBR'
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
                    OTHER_SBR_INDEX,
                    ADDRESS_CODE_OTHERSBR_NMIL_N3,
                    ADDRESS_LINE_1_OTHERSBR,
                    ADDRESS_LINE_2_OTHERSBR
                )
)
, claim_sbr_nmIL_n4 as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_OTHERSBR'
                            when    flattened.index = 2   then      'CITY_OTHERSBR'
                            when    flattened.index = 3   then      'ST_OTHERSBR'
                            when    flattened.index = 4   then      'ZIP_OTHERSBR'
                            when    flattened.index = 5   then      'COUNTRY_OTHERSBR'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_OTHERSBR'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_OTHERSBR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_clm_sbr.lag_name_indicator = 'NM1*IL'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_OTHERSBR',
                        'CITY_OTHERSBR',
                        'ST_OTHERSBR',
                        'ZIP_OTHERSBR',
                        'COUNTRY_OTHERSBR',
                        'LOCATION_QUALIFIER_OTHERSBR',
                        'LOCATION_IDENTIFIER_OTHERSBR'
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
                    OTHER_SBR_INDEX,
                    ADDRESS_CODE_OTHERSBR_NMIL_N4,
                    CITY_OTHERSBR,
                    ST_OTHERSBR,
                    ZIP_OTHERSBR,
                    COUNTRY_OTHERSBR,
                    LOCATION_QUALIFIER_OTHERSBR,
                    LOCATION_IDENTIFIER_OTHERSBR
                )
)
, claim_sbr_nmPR as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_OTHERPYR'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_OTHERPYR'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_OTHERPYR'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_OTHERPYR'
                            when    flattened.index = 5   then      'FIRST_NAME_OTHERPYR'
                            when    flattened.index = 6   then      'MIDDLE_NAME_OTHERPYR'
                            when    flattened.index = 7   then      'NAME_PREFIX_OTHERPYR'
                            when    flattened.index = 8   then      'NAME_SUFFIX_OTHERPYR'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_OTHERPYR'
                            when    flattened.index = 10  then      'ID_CODE_OTHERSBR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^NM1\\*PR.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_OTHERPYR',
                        'ENTITY_IDENTIFIER_CODE_OTHERPYR',
                        'ENTITY_TYPE_QUALIFIER_OTHERPYR',
                        'LAST_NAME_ORG_OTHERPYR',
                        'FIRST_NAME_OTHERPYR',
                        'MIDDLE_NAME_OTHERPYR',
                        'NAME_PREFIX_OTHERPYR',
                        'NAME_SUFFIX_OTHERPYR',
                        'ID_CODE_QUALIFIER_OTHERPYR',
                        'ID_CODE_OTHERSBR'
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
                    OTHER_SBR_INDEX,
                    NAME_CODE_OTHERPYR,
                    ENTITY_IDENTIFIER_CODE_OTHERPYR,
                    ENTITY_TYPE_QUALIFIER_OTHERPYR,
                    LAST_NAME_ORG_OTHERPYR,
                    FIRST_NAME_OTHERPYR,
                    MIDDLE_NAME_OTHERPYR,
                    NAME_PREFIX_OTHERPYR,
                    NAME_SUFFIX_OTHERPYR,
                    ID_CODE_QUALIFIER_OTHERPYR,
                    ID_CODE_OTHERSBR_NMPR
                )
)
, claim_sbr_nmPR_n3 as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_OTHERPYR'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_OTHERPYR'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_OTHERPYR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_clm_sbr.lag_name_indicator = 'NM1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_OTHERPYR',
                        'ADDRESS_LINE_1_OTHERPYR',
                        'ADDRESS_LINE_2_OTHERPYR'
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
                    OTHER_SBR_INDEX,
                    ADDRESS_CODE_OTHERPYR_NMPR_N3,
                    ADDRESS_LINE_1_OTHERPYR,
                    ADDRESS_LINE_2_OTHERPYR
                )
)
, claim_sbr_nmPR_n4 as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_OTHERPYR'
                            when    flattened.index = 2   then      'CITY_OTHERPYR'
                            when    flattened.index = 3   then      'ST_OTHERPYR'
                            when    flattened.index = 4   then      'ZIP_OTHERPYR'
                            when    flattened.index = 5   then      'COUNTRY_OTHERPYR'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_OTHERPYR'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_OTHERPYR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_clm_sbr.lag_name_indicator = 'NM1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_OTHERPYR',
                        'CITY_OTHERPYR',
                        'ST_OTHERPYR',
                        'ZIP_OTHERPYR',
                        'COUNTRY_OTHERPYR',
                        'LOCATION_QUALIFIER_OTHERPYR',
                        'LOCATION_IDENTIFIER_OTHERPYR'
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
                    OTHER_SBR_INDEX,
                    ADDRESS_CODE_OTHERPYR_NMPR_N4,
                    CITY_OTHERPYR,
                    ST_OTHERPYR,
                    ZIP_OTHERPYR,
                    COUNTRY_OTHERPYR,
                    LOCATION_QUALIFIER_OTHERPYR,
                    LOCATION_IDENTIFIER_OTHERPYR
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
            header.claim_index,
            header.other_sbr_index,
            header.sbr_prefix_othersbr,
            header.payor_responsibility_sequence_othersbr,
            header.individual_relationship_code_othersbr,
            header.group_number_othersbr,
            header.group_name_othersbr,
            header.insurance_type_code_othersbr,
            header.coordination_of_benefits_code_othersbr,
            header.employment_code_othersbr,
            header.claim_filing_indicator_code_othersbr,
            header.patient_signature_source_code_othersbr,
            oi.prefix_othersbr,
            oi.empty1_othersbr,
            oi.empty2_othersbr,
            oi.benefits_assignment_othersbr,
            oi.patient_signature_source_othersbr,
            oi.empty5_othersbr,
            oi.release_of_info_othersbr,
            nmIL.name_code_othersbr,
            nmIL.entity_identifier_code_othersbr,
            nmIL.entity_type_qualifier_othersbr,
            nmIL.last_name_org_othersbr,
            nmIL.first_name_othersbr,
            nmIL.middle_name_othersbr,
            nmIL.name_prefix_othersbr,
            nmIL.name_suffix_othersbr,
            nmIL.id_code_qualifier_othersbr,
            nmIL.id_code_othersbr_nmIL,
            nmIL_n3.address_code_othersbr_nmIL_n3,
            nmIL_n3.address_line_1_othersbr,
            nmIL_n3.address_line_2_othersbr,
            nmIL_n4.address_code_othersbr_nmIL_n4,
            nmIL_n4.city_othersbr,
            nmIL_n4.st_othersbr,
            nmIL_n4.zip_othersbr,
            nmIL_n4.country_othersbr,
            nmIL_n4.location_qualifier_othersbr,
            nmIL_n4.location_identifier_othersbr,
            nmPR.name_code_otherpyr,
            nmPR.entity_identifier_code_otherpyr,
            nmPR.entity_type_qualifier_otherpyr,
            nmPR.last_name_org_otherpyr,
            nmPR.first_name_otherpyr,
            nmPR.middle_name_otherpyr,
            nmPR.name_prefix_otherpyr,
            nmPR.name_suffix_otherpyr,
            nmPR.id_code_qualifier_otherpyr,
            nmPR.id_code_othersbr_nmPR,
            nmPR_n3.address_code_otherpyr_nmPR_n3,
            nmPR_n3.address_line_1_otherpyr,
            nmPR_n3.address_line_2_otherpyr,
            nmPR_n4.address_code_otherpyr_nmPR_n4,
            nmPR_n4.city_otherpyr,
            nmPR_n4.st_otherpyr,
            nmPR_n4.zip_otherpyr,
            nmPR_n4.country_otherpyr,
            nmPR_n4.location_qualifier_otherpyr,
            nmPR_n4.location_identifier_otherpyr,

            cas.cas_adj_array,
            amt.amt_adj_array

from        claim_sbr_header    as header
            left join
                claim_sbr_oi        as oi
                on  header.response_id          = oi.response_id
                and header.nth_functional_group = oi.nth_functional_group
                and header.nth_transaction_set  = oi.nth_transaction_set
                and header.claim_index          = oi.claim_index
                and header.other_sbr_index      = oi.other_sbr_index
            left join
                claim_sbr_nmIL      as nmIL
                on  header.response_id          = nmIL.response_id
                and header.nth_functional_group = nmIL.nth_functional_group
                and header.nth_transaction_set  = nmIL.nth_transaction_set
                and header.claim_index          = nmIL.claim_index
                and header.other_sbr_index      = nmIL.other_sbr_index
            left join
                claim_sbr_nmIL_n3   as nmIL_n3
                on  header.response_id          = nmIL_n3.response_id
                and header.nth_functional_group = nmIL_n3.nth_functional_group
                and header.nth_transaction_set  = nmIL_n3.nth_transaction_set
                and header.claim_index          = nmIL_n3.claim_index
                and header.other_sbr_index      = nmIL_n3.other_sbr_index
            left join
                claim_sbr_nmIL_n4   as nmIL_n4
                on  header.response_id          = nmIL_n4.response_id
                and header.nth_functional_group = nmIL_n4.nth_functional_group
                and header.nth_transaction_set  = nmIL_n4.nth_transaction_set
                and header.claim_index          = nmIL_n4.claim_index
                and header.other_sbr_index      = nmIL_n4.other_sbr_index
            left join
                claim_sbr_nmPR      as nmPR
                on  header.response_id          = nmPR.response_id
                and header.nth_functional_group = nmPR.nth_functional_group
                and header.nth_transaction_set  = nmPR.nth_transaction_set
                and header.claim_index          = nmPR.claim_index
                and header.other_sbr_index      = nmPR.other_sbr_index
            left join
                claim_sbr_nmPR_n3   as nmPR_n3
                on  header.response_id          = nmPR_n3.response_id
                and header.nth_functional_group = nmPR_n3.nth_functional_group
                and header.nth_transaction_set  = nmPR_n3.nth_transaction_set
                and header.claim_index          = nmPR_n3.claim_index
                and header.other_sbr_index      = nmPR_n3.other_sbr_index
            left join
                claim_sbr_nmPR_n4   as nmPR_n4
                on  header.response_id          = nmPR_n4.response_id
                and header.nth_functional_group = nmPR_n4.nth_functional_group
                and header.nth_transaction_set  = nmPR_n4.nth_transaction_set
                and header.claim_index          = nmPR_n4.claim_index
                and header.other_sbr_index      = nmPR_n4.other_sbr_index
                
            left join
                claim_sbr_cas       as cas
                on  header.response_id          = cas.response_id
                and header.nth_functional_group = cas.nth_functional_group
                and header.nth_transaction_set  = cas.nth_transaction_set
                and header.claim_index          = cas.claim_index
                and header.other_sbr_index      = cas.other_sbr_index
            left join
                claim_sbr_amt       as amt
                on  header.response_id          = amt.response_id
                and header.nth_functional_group = amt.nth_functional_group
                and header.nth_transaction_set  = amt.nth_transaction_set
                and header.claim_index          = amt.claim_index
                and header.other_sbr_index      = amt.other_sbr_index
                
order by    1,2,3
;



create or replace task
    edwprodhh.edi_837i_parser.insert_claim_additional_subscribers
    warehouse = analysis_wh
    after edwprodhh.edi_837i_parser.insert_response_flat
as
insert into
    edwprodhh.edi_837i_parser.claim_additional_subscribers
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
    OTHER_SBR_INDEX,
    SBR_PREFIX_OTHERSBR,
    PAYOR_RESPONSIBILITY_SEQUENCE_OTHERSBR,
    INDIVIDUAL_RELATIONSHIP_CODE_OTHERSBR,
    GROUP_NUMBER_OTHERSBR,
    GROUP_NAME_OTHERSBR,
    INSURANCE_TYPE_CODE_OTHERSBR,
    COORDINATION_OF_BENEFITS_CODE_OTHERSBR,
    EMPLOYMENT_CODE_OTHERSBR,
    CLAIM_FILING_INDICATOR_CODE_OTHERSBR,
    PATIENT_SIGNATURE_SOURCE_CODE_OTHERSBR,
    PREFIX_OTHERSBR,
    EMPTY1_OTHERSBR,
    EMPTY2_OTHERSBR,
    BENEFITS_ASSIGNMENT_OTHERSBR,
    PATIENT_SIGNATURE_SOURCE_OTHERSBR,
    EMPTY5_OTHERSBR,
    RELEASE_OF_INFO_OTHERSBR,
    NAME_CODE_OTHERSBR,
    ENTITY_IDENTIFIER_CODE_OTHERSBR,
    ENTITY_TYPE_QUALIFIER_OTHERSBR,
    LAST_NAME_ORG_OTHERSBR,
    FIRST_NAME_OTHERSBR,
    MIDDLE_NAME_OTHERSBR,
    NAME_PREFIX_OTHERSBR,
    NAME_SUFFIX_OTHERSBR,
    ID_CODE_QUALIFIER_OTHERSBR,
    ID_CODE_OTHERSBR_NMIL,
    ADDRESS_CODE_OTHERSBR_NMIL_N3,
    ADDRESS_LINE_1_OTHERSBR,
    ADDRESS_LINE_2_OTHERSBR,
    ADDRESS_CODE_OTHERSBR_NMIL_N4,
    CITY_OTHERSBR,
    ST_OTHERSBR,
    ZIP_OTHERSBR,
    COUNTRY_OTHERSBR,
    LOCATION_QUALIFIER_OTHERSBR,
    LOCATION_IDENTIFIER_OTHERSBR,
    NAME_CODE_OTHERPYR,
    ENTITY_IDENTIFIER_CODE_OTHERPYR,
    ENTITY_TYPE_QUALIFIER_OTHERPYR,
    LAST_NAME_ORG_OTHERPYR,
    FIRST_NAME_OTHERPYR,
    MIDDLE_NAME_OTHERPYR,
    NAME_PREFIX_OTHERPYR,
    NAME_SUFFIX_OTHERPYR,
    ID_CODE_QUALIFIER_OTHERPYR,
    ID_CODE_OTHERSBR_NMPR,
    ADDRESS_CODE_OTHERPYR_NMPR_N3,
    ADDRESS_LINE_1_OTHERPYR,
    ADDRESS_LINE_2_OTHERPYR,
    ADDRESS_CODE_OTHERPYR_NMPR_N4,
    CITY_OTHERPYR,
    ST_OTHERPYR,
    ZIP_OTHERPYR,
    COUNTRY_OTHERPYR,
    LOCATION_QUALIFIER_OTHERPYR,
    LOCATION_IDENTIFIER_OTHERPYR,
    CAS_ADJ_ARRAY,
    AMT_ADJ_ARRAY
)
with filtered_clm_sbr as
(
    select      *
    from        edwprodhh.edi_837i_parser.response_flat
    where       claim_index is not null --0 Pre-Filter
                and other_sbr_index is not null
                and lx_index is null
                and response_id not in (select response_id from edwprodhh.edi_837i_parser.claim_additional_subscribers)
)
, claim_sbr_header as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'SBR_PREFIX_OTHERSBR'
                            when    flattened.index = 2   then      'PAYOR_RESPONSIBILITY_SEQUENCE_OTHERSBR'
                            when    flattened.index = 3   then      'INDIVIDUAL_RELATIONSHIP_CODE_OTHERSBR'
                            when    flattened.index = 4   then      'GROUP_NUMBER_OTHERSBR'
                            when    flattened.index = 5   then      'GROUP_NAME_OTHERSBR'
                            when    flattened.index = 6   then      'INSURANCE_TYPE_CODE_OTHERSBR'
                            when    flattened.index = 7   then      'COORDINATION_OF_BENEFITS_CODE_OTHERSBR'
                            when    flattened.index = 8   then      'EMPLOYMENT_CODE_OTHERSBR'
                            when    flattened.index = 9   then      'CLAIM_FILING_INDICATOR_CODE_OTHERSBR'
                            when    flattened.index = 10  then      'PATIENT_SIGNATURE_SOURCE_CODE_OTHERSBR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^SBR.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SBR_PREFIX_OTHERSBR',
                        'PAYOR_RESPONSIBILITY_SEQUENCE_OTHERSBR',
                        'INDIVIDUAL_RELATIONSHIP_CODE_OTHERSBR',
                        'GROUP_NUMBER_OTHERSBR',
                        'GROUP_NAME_OTHERSBR',
                        'INSURANCE_TYPE_CODE_OTHERSBR',
                        'COORDINATION_OF_BENEFITS_CODE_OTHERSBR',
                        'EMPLOYMENT_CODE_OTHERSBR',
                        'CLAIM_FILING_INDICATOR_CODE_OTHERSBR',
                        'PATIENT_SIGNATURE_SOURCE_CODE_OTHERSBR'
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
                    OTHER_SBR_INDEX,
                    SBR_PREFIX_OTHERSBR,
                    PAYOR_RESPONSIBILITY_SEQUENCE_OTHERSBR,
                    INDIVIDUAL_RELATIONSHIP_CODE_OTHERSBR,
                    GROUP_NUMBER_OTHERSBR,
                    GROUP_NAME_OTHERSBR,
                    INSURANCE_TYPE_CODE_OTHERSBR,
                    COORDINATION_OF_BENEFITS_CODE_OTHERSBR,
                    EMPLOYMENT_CODE_OTHERSBR,
                    CLAIM_FILING_INDICATOR_CODE_OTHERSBR,
                    PATIENT_SIGNATURE_SOURCE_CODE_OTHERSBR
                )
)
, claim_sbr_cas as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then      'PREFIX_CAS'
                            when    flattened.index = 2     then      'CLM_ADJ_GROUP_CODE'
                            when    flattened.index = 3     then      'ADJ_REASON_CODE_1'
                            when    flattened.index = 4     then      'ADJ_AMOUNT_1'
                            when    flattened.index = 5     then      'ADJ_QUANTITY_1'
                            when    flattened.index = 6     then      'ADJ_REASON_CODE_2'
                            when    flattened.index = 7     then      'ADJ_AMOUNT_2'
                            when    flattened.index = 8     then      'ADJ_QUANTITY_2'
                            when    flattened.index = 9     then      'ADJ_REASON_CODE_3'
                            when    flattened.index = 10    then      'ADJ_AMOUNT_3'
                            when    flattened.index = 11    then      'ADJ_QUANTITY_3'
                            when    flattened.index = 12    then      'ADJ_REASON_CODE_4'
                            when    flattened.index = 13    then      'ADJ_AMOUNT_4'
                            when    flattened.index = 14    then      'ADJ_QUANTITY_4'
                            when    flattened.index = 15    then      'ADJ_REASON_CODE_5'
                            when    flattened.index = 16    then      'ADJ_AMOUNT_5'
                            when    flattened.index = 17    then      'ADJ_QUANTITY_5'
                            when    flattened.index = 18    then      'ADJ_REASON_CODE_6'
                            when    flattened.index = 19    then      'ADJ_AMOUNT_6'
                            when    flattened.index = 20    then      'ADJ_QUANTITY_6'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^CAS.*')                         --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'PREFIX_CAS',
                            'CLM_ADJ_GROUP_CODE',
                            'ADJ_REASON_CODE_1',
                            'ADJ_AMOUNT_1',
                            'ADJ_QUANTITY_1',
                            'ADJ_REASON_CODE_2',
                            'ADJ_AMOUNT_2',
                            'ADJ_QUANTITY_2',
                            'ADJ_REASON_CODE_3',
                            'ADJ_AMOUNT_3',
                            'ADJ_QUANTITY_3',
                            'ADJ_REASON_CODE_4',
                            'ADJ_AMOUNT_4',
                            'ADJ_QUANTITY_4',
                            'ADJ_REASON_CODE_5',
                            'ADJ_AMOUNT_5',
                            'ADJ_QUANTITY_5',
                            'ADJ_REASON_CODE_6',
                            'ADJ_AMOUNT_6',
                            'ADJ_QUANTITY_6'
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
                        OTHER_SBR_INDEX,
                        PREFIX_CAS,
                        CLM_ADJ_GROUP_CODE,
                        ADJ_REASON_CODE_1,
                        ADJ_AMOUNT_1,
                        ADJ_QUANTITY_1,
                        ADJ_REASON_CODE_2,
                        ADJ_AMOUNT_2,
                        ADJ_QUANTITY_2,
                        ADJ_REASON_CODE_3,
                        ADJ_AMOUNT_3,
                        ADJ_QUANTITY_3,
                        ADJ_REASON_CODE_4,
                        ADJ_AMOUNT_4,
                        ADJ_QUANTITY_4,
                        ADJ_REASON_CODE_5,
                        ADJ_AMOUNT_5,
                        ADJ_QUANTITY_5,
                        ADJ_REASON_CODE_6,
                        ADJ_AMOUNT_6,
                        ADJ_QUANTITY_6
                    )
    )
    , unpivoted as
    (
        select      response_id,
                    nth_functional_group,
                    nth_transaction_set,
                    index,
                    hl_index_current,
                    hl_index_billing_20,
                    hl_index_subscriber_22,
                    hl_index_patient_23,
                    claim_index,
                    other_sbr_index,
                    clm_adj_group_code,
                    regexp_substr(unpvt.metric_name, '\\d+$') as nth_element,
                    regexp_replace(unpvt.metric_name, '_\\d+$', '') as metric_name,
                    metric_value
        from        pivoted
                    unpivot include nulls (
                        metric_value for metric_name in (
                            ADJ_REASON_CODE_1,
                            ADJ_AMOUNT_1,
                            ADJ_QUANTITY_1,
                            ADJ_REASON_CODE_2,
                            ADJ_AMOUNT_2,
                            ADJ_QUANTITY_2,
                            ADJ_REASON_CODE_3,
                            ADJ_AMOUNT_3,
                            ADJ_QUANTITY_3,
                            ADJ_REASON_CODE_4,
                            ADJ_AMOUNT_4,
                            ADJ_QUANTITY_4,
                            ADJ_REASON_CODE_5,
                            ADJ_AMOUNT_5,
                            ADJ_QUANTITY_5,
                            ADJ_REASON_CODE_6,
                            ADJ_AMOUNT_6,
                            ADJ_QUANTITY_6
                        )
                    )   as unpvt
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                claim_index,
                other_sbr_index,
                array_agg(
                    object_construct_keep_null(
                        'adj_group_code',   clm_adj_group_code,
                        'adj_detail',       object_construct_keep_null(
                                                'adj_reason_code',  adj_reason_code::varchar,
                                                'adj_amount',       adj_amount::number(18,2),
                                                'adj_quantity',     adj_quantity::number(18,2)
                                            )
                    )
                )   as cas_adj_array
    from        unpivoted
                pivot (
                    max(metric_value) for metric_name in (
                        'ADJ_REASON_CODE',
                        'ADJ_AMOUNT',
                        'ADJ_QUANTITY'
                    )
                )   as pvt (
                    response_id,
                    nth_functional_group,
                    nth_transaction_set,
                    index,
                    hl_index_current,
                    hl_index_billing_20,
                    hl_index_subscriber_22,
                    hl_index_patient_23,
                    claim_index,
                    other_sbr_index,
                    clm_adj_group_code,
                    nth_element,
                    adj_reason_code,
                    adj_amount,
                    adj_quantity
                )
    where       not (
                    adj_reason_code is null
                    and adj_amount is null
                    and adj_quantity is null
                )
    group by    1,2,3,4,5
    order by    1,2,3,4,5
)
, claim_sbr_amt as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then      'PREFIX_AMT'
                            when    flattened.index = 2     then      'AMT_QUALIFIER_CODE'
                            when    flattened.index = 3     then      'MONETARY_AMOUNT'
                            when    flattened.index = 4     then      'CREDIT_DEBIT_FLAG'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^AMT.*')                         --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'PREFIX_AMT',
                            'AMT_QUALIFIER_CODE',
                            'MONETARY_AMOUNT',
                            'CREDIT_DEBIT_FLAG'
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
                        OTHER_SBR_INDEX,
                        PREFIX_AMT,
                        AMT_QUALIFIER_CODE,
                        MONETARY_AMOUNT,
                        CREDIT_DEBIT_FLAG
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                claim_index,
                other_sbr_index,
                array_agg(
                    object_construct_keep_null(
                        'amt_qualifier_code',   amt_qualifier_code::varchar,
                        'monetary_amount',      monetary_amount::number(18,2),
                        'credit_debit_flag',    credit_debit_flag::varchar
                    )
                )   as amt_adj_array
    from        pivoted
    group by    1,2,3,4,5
    order by    1,2,3,4,5
)
, claim_sbr_oi as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PREFIX_OTHERSBR'
                            when    flattened.index = 2   then      'EMPTY1_OTHERSBR'
                            when    flattened.index = 3   then      'EMPTY2_OTHERSBR'
                            when    flattened.index = 4   then      'BENEFITS_ASSIGNMENT_OTHERSBR'
                            when    flattened.index = 5   then      'PATIENT_SIGNATURE_SOURCE_OTHERSBR'
                            when    flattened.index = 6   then      'EMPTY5_OTHERSBR'
                            when    flattened.index = 7   then      'RELEASE_OF_INFO_OTHERSBR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^OI.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PREFIX_OTHERSBR',
                        'EMPTY1_OTHERSBR',
                        'EMPTY2_OTHERSBR',
                        'BENEFITS_ASSIGNMENT_OTHERSBR',
                        'PATIENT_SIGNATURE_SOURCE_OTHERSBR',
                        'EMPTY5_OTHERSBR',
                        'RELEASE_OF_INFO_OTHERSBR'
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
                    OTHER_SBR_INDEX,
                    PREFIX_OTHERSBR,
                    EMPTY1_OTHERSBR,
                    EMPTY2_OTHERSBR,
                    BENEFITS_ASSIGNMENT_OTHERSBR,
                    PATIENT_SIGNATURE_SOURCE_OTHERSBR,
                    EMPTY5_OTHERSBR,
                    RELEASE_OF_INFO_OTHERSBR
                )
)
, claim_sbr_nmIL as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_OTHERSBR'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_OTHERSBR'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_OTHERSBR'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_OTHERSBR'
                            when    flattened.index = 5   then      'FIRST_NAME_OTHERSBR'
                            when    flattened.index = 6   then      'MIDDLE_NAME_OTHERSBR'
                            when    flattened.index = 7   then      'NAME_PREFIX_OTHERSBR'
                            when    flattened.index = 8   then      'NAME_SUFFIX_OTHERSBR'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_OTHERSBR'
                            when    flattened.index = 10  then      'ID_CODE_OTHERSBR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^NM1\\*IL.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_OTHERSBR',
                        'ENTITY_IDENTIFIER_CODE_OTHERSBR',
                        'ENTITY_TYPE_QUALIFIER_OTHERSBR',
                        'LAST_NAME_ORG_OTHERSBR',
                        'FIRST_NAME_OTHERSBR',
                        'MIDDLE_NAME_OTHERSBR',
                        'NAME_PREFIX_OTHERSBR',
                        'NAME_SUFFIX_OTHERSBR',
                        'ID_CODE_QUALIFIER_OTHERSBR',
                        'ID_CODE_OTHERSBR'
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
                    OTHER_SBR_INDEX,
                    NAME_CODE_OTHERSBR,
                    ENTITY_IDENTIFIER_CODE_OTHERSBR,
                    ENTITY_TYPE_QUALIFIER_OTHERSBR,
                    LAST_NAME_ORG_OTHERSBR,
                    FIRST_NAME_OTHERSBR,
                    MIDDLE_NAME_OTHERSBR,
                    NAME_PREFIX_OTHERSBR,
                    NAME_SUFFIX_OTHERSBR,
                    ID_CODE_QUALIFIER_OTHERSBR,
                    ID_CODE_OTHERSBR_NMIL
                )
)
, claim_sbr_nmIL_n3 as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_OTHERSBR'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_OTHERSBR'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_OTHERSBR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_clm_sbr.lag_name_indicator = 'NM1*IL'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_OTHERSBR',
                        'ADDRESS_LINE_1_OTHERSBR',
                        'ADDRESS_LINE_2_OTHERSBR'
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
                    OTHER_SBR_INDEX,
                    ADDRESS_CODE_OTHERSBR_NMIL_N3,
                    ADDRESS_LINE_1_OTHERSBR,
                    ADDRESS_LINE_2_OTHERSBR
                )
)
, claim_sbr_nmIL_n4 as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_OTHERSBR'
                            when    flattened.index = 2   then      'CITY_OTHERSBR'
                            when    flattened.index = 3   then      'ST_OTHERSBR'
                            when    flattened.index = 4   then      'ZIP_OTHERSBR'
                            when    flattened.index = 5   then      'COUNTRY_OTHERSBR'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_OTHERSBR'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_OTHERSBR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_clm_sbr.lag_name_indicator = 'NM1*IL'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_OTHERSBR',
                        'CITY_OTHERSBR',
                        'ST_OTHERSBR',
                        'ZIP_OTHERSBR',
                        'COUNTRY_OTHERSBR',
                        'LOCATION_QUALIFIER_OTHERSBR',
                        'LOCATION_IDENTIFIER_OTHERSBR'
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
                    OTHER_SBR_INDEX,
                    ADDRESS_CODE_OTHERSBR_NMIL_N4,
                    CITY_OTHERSBR,
                    ST_OTHERSBR,
                    ZIP_OTHERSBR,
                    COUNTRY_OTHERSBR,
                    LOCATION_QUALIFIER_OTHERSBR,
                    LOCATION_IDENTIFIER_OTHERSBR
                )
)
, claim_sbr_nmPR as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_OTHERPYR'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_OTHERPYR'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_OTHERPYR'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_OTHERPYR'
                            when    flattened.index = 5   then      'FIRST_NAME_OTHERPYR'
                            when    flattened.index = 6   then      'MIDDLE_NAME_OTHERPYR'
                            when    flattened.index = 7   then      'NAME_PREFIX_OTHERPYR'
                            when    flattened.index = 8   then      'NAME_SUFFIX_OTHERPYR'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_OTHERPYR'
                            when    flattened.index = 10  then      'ID_CODE_OTHERSBR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^NM1\\*PR.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_OTHERPYR',
                        'ENTITY_IDENTIFIER_CODE_OTHERPYR',
                        'ENTITY_TYPE_QUALIFIER_OTHERPYR',
                        'LAST_NAME_ORG_OTHERPYR',
                        'FIRST_NAME_OTHERPYR',
                        'MIDDLE_NAME_OTHERPYR',
                        'NAME_PREFIX_OTHERPYR',
                        'NAME_SUFFIX_OTHERPYR',
                        'ID_CODE_QUALIFIER_OTHERPYR',
                        'ID_CODE_OTHERSBR'
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
                    OTHER_SBR_INDEX,
                    NAME_CODE_OTHERPYR,
                    ENTITY_IDENTIFIER_CODE_OTHERPYR,
                    ENTITY_TYPE_QUALIFIER_OTHERPYR,
                    LAST_NAME_ORG_OTHERPYR,
                    FIRST_NAME_OTHERPYR,
                    MIDDLE_NAME_OTHERPYR,
                    NAME_PREFIX_OTHERPYR,
                    NAME_SUFFIX_OTHERPYR,
                    ID_CODE_QUALIFIER_OTHERPYR,
                    ID_CODE_OTHERSBR_NMPR
                )
)
, claim_sbr_nmPR_n3 as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_OTHERPYR'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_OTHERPYR'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_OTHERPYR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^N3.*')                          --1 Filter
                    and filtered_clm_sbr.lag_name_indicator = 'NM1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_OTHERPYR',
                        'ADDRESS_LINE_1_OTHERPYR',
                        'ADDRESS_LINE_2_OTHERPYR'
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
                    OTHER_SBR_INDEX,
                    ADDRESS_CODE_OTHERPYR_NMPR_N3,
                    ADDRESS_LINE_1_OTHERPYR,
                    ADDRESS_LINE_2_OTHERPYR
                )
)
, claim_sbr_nmPR_n4 as
(
    with long as
    (
        select      filtered_clm_sbr.response_id,
                    filtered_clm_sbr.nth_functional_group,
                    filtered_clm_sbr.nth_transaction_set,
                    filtered_clm_sbr.index,
                    filtered_clm_sbr.hl_index_current,
                    filtered_clm_sbr.hl_index_billing_20,
                    filtered_clm_sbr.hl_index_subscriber_22,
                    filtered_clm_sbr.hl_index_patient_23,
                    filtered_clm_sbr.claim_index,
                    filtered_clm_sbr.other_sbr_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_OTHERPYR'
                            when    flattened.index = 2   then      'CITY_OTHERPYR'
                            when    flattened.index = 3   then      'ST_OTHERPYR'
                            when    flattened.index = 4   then      'ZIP_OTHERPYR'
                            when    flattened.index = 5   then      'COUNTRY_OTHERPYR'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_OTHERPYR'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_OTHERPYR'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm_sbr,
                    lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm_sbr.line_element_837, '^N4.*')                          --1 Filter
                    and filtered_clm_sbr.lag_name_indicator = 'NM1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_OTHERPYR',
                        'CITY_OTHERPYR',
                        'ST_OTHERPYR',
                        'ZIP_OTHERPYR',
                        'COUNTRY_OTHERPYR',
                        'LOCATION_QUALIFIER_OTHERPYR',
                        'LOCATION_IDENTIFIER_OTHERPYR'
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
                    OTHER_SBR_INDEX,
                    ADDRESS_CODE_OTHERPYR_NMPR_N4,
                    CITY_OTHERPYR,
                    ST_OTHERPYR,
                    ZIP_OTHERPYR,
                    COUNTRY_OTHERPYR,
                    LOCATION_QUALIFIER_OTHERPYR,
                    LOCATION_IDENTIFIER_OTHERPYR
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
            header.claim_index,
            header.other_sbr_index,
            header.sbr_prefix_othersbr,
            header.payor_responsibility_sequence_othersbr,
            header.individual_relationship_code_othersbr,
            header.group_number_othersbr,
            header.group_name_othersbr,
            header.insurance_type_code_othersbr,
            header.coordination_of_benefits_code_othersbr,
            header.employment_code_othersbr,
            header.claim_filing_indicator_code_othersbr,
            header.patient_signature_source_code_othersbr,
            oi.prefix_othersbr,
            oi.empty1_othersbr,
            oi.empty2_othersbr,
            oi.benefits_assignment_othersbr,
            oi.patient_signature_source_othersbr,
            oi.empty5_othersbr,
            oi.release_of_info_othersbr,
            nmIL.name_code_othersbr,
            nmIL.entity_identifier_code_othersbr,
            nmIL.entity_type_qualifier_othersbr,
            nmIL.last_name_org_othersbr,
            nmIL.first_name_othersbr,
            nmIL.middle_name_othersbr,
            nmIL.name_prefix_othersbr,
            nmIL.name_suffix_othersbr,
            nmIL.id_code_qualifier_othersbr,
            nmIL.id_code_othersbr_nmIL,
            nmIL_n3.address_code_othersbr_nmIL_n3,
            nmIL_n3.address_line_1_othersbr,
            nmIL_n3.address_line_2_othersbr,
            nmIL_n4.address_code_othersbr_nmIL_n4,
            nmIL_n4.city_othersbr,
            nmIL_n4.st_othersbr,
            nmIL_n4.zip_othersbr,
            nmIL_n4.country_othersbr,
            nmIL_n4.location_qualifier_othersbr,
            nmIL_n4.location_identifier_othersbr,
            nmPR.name_code_otherpyr,
            nmPR.entity_identifier_code_otherpyr,
            nmPR.entity_type_qualifier_otherpyr,
            nmPR.last_name_org_otherpyr,
            nmPR.first_name_otherpyr,
            nmPR.middle_name_otherpyr,
            nmPR.name_prefix_otherpyr,
            nmPR.name_suffix_otherpyr,
            nmPR.id_code_qualifier_otherpyr,
            nmPR.id_code_othersbr_nmPR,
            nmPR_n3.address_code_otherpyr_nmPR_n3,
            nmPR_n3.address_line_1_otherpyr,
            nmPR_n3.address_line_2_otherpyr,
            nmPR_n4.address_code_otherpyr_nmPR_n4,
            nmPR_n4.city_otherpyr,
            nmPR_n4.st_otherpyr,
            nmPR_n4.zip_otherpyr,
            nmPR_n4.country_otherpyr,
            nmPR_n4.location_qualifier_otherpyr,
            nmPR_n4.location_identifier_otherpyr,

            cas.cas_adj_array,
            amt.amt_adj_array

from        claim_sbr_header    as header
            left join
                claim_sbr_oi        as oi
                on  header.response_id          = oi.response_id
                and header.nth_functional_group = oi.nth_functional_group
                and header.nth_transaction_set  = oi.nth_transaction_set
                and header.claim_index          = oi.claim_index
                and header.other_sbr_index      = oi.other_sbr_index
            left join
                claim_sbr_nmIL      as nmIL
                on  header.response_id          = nmIL.response_id
                and header.nth_functional_group = nmIL.nth_functional_group
                and header.nth_transaction_set  = nmIL.nth_transaction_set
                and header.claim_index          = nmIL.claim_index
                and header.other_sbr_index      = nmIL.other_sbr_index
            left join
                claim_sbr_nmIL_n3   as nmIL_n3
                on  header.response_id          = nmIL_n3.response_id
                and header.nth_functional_group = nmIL_n3.nth_functional_group
                and header.nth_transaction_set  = nmIL_n3.nth_transaction_set
                and header.claim_index          = nmIL_n3.claim_index
                and header.other_sbr_index      = nmIL_n3.other_sbr_index
            left join
                claim_sbr_nmIL_n4   as nmIL_n4
                on  header.response_id          = nmIL_n4.response_id
                and header.nth_functional_group = nmIL_n4.nth_functional_group
                and header.nth_transaction_set  = nmIL_n4.nth_transaction_set
                and header.claim_index          = nmIL_n4.claim_index
                and header.other_sbr_index      = nmIL_n4.other_sbr_index
            left join
                claim_sbr_nmPR      as nmPR
                on  header.response_id          = nmPR.response_id
                and header.nth_functional_group = nmPR.nth_functional_group
                and header.nth_transaction_set  = nmPR.nth_transaction_set
                and header.claim_index          = nmPR.claim_index
                and header.other_sbr_index      = nmPR.other_sbr_index
            left join
                claim_sbr_nmPR_n3   as nmPR_n3
                on  header.response_id          = nmPR_n3.response_id
                and header.nth_functional_group = nmPR_n3.nth_functional_group
                and header.nth_transaction_set  = nmPR_n3.nth_transaction_set
                and header.claim_index          = nmPR_n3.claim_index
                and header.other_sbr_index      = nmPR_n3.other_sbr_index
            left join
                claim_sbr_nmPR_n4   as nmPR_n4
                on  header.response_id          = nmPR_n4.response_id
                and header.nth_functional_group = nmPR_n4.nth_functional_group
                and header.nth_transaction_set  = nmPR_n4.nth_transaction_set
                and header.claim_index          = nmPR_n4.claim_index
                and header.other_sbr_index      = nmPR_n4.other_sbr_index
                
            left join
                claim_sbr_cas       as cas
                on  header.response_id          = cas.response_id
                and header.nth_functional_group = cas.nth_functional_group
                and header.nth_transaction_set  = cas.nth_transaction_set
                and header.claim_index          = cas.claim_index
                and header.other_sbr_index      = cas.other_sbr_index
            left join
                claim_sbr_amt       as amt
                on  header.response_id          = amt.response_id
                and header.nth_functional_group = amt.nth_functional_group
                and header.nth_transaction_set  = amt.nth_transaction_set
                and header.claim_index          = amt.claim_index
                and header.other_sbr_index      = amt.other_sbr_index
                
order by    1,2,3
;