--  As of 10/24/2025, this table is NOT unique to: Response ID*Nth Transaction Set*Index*LX Index.
--  This is because a single LX element may have more than one DTP*573, one for each refill of the prescription.
--  Potential solutions: aggregate (preferred) or delete.

create or replace table
    edwprodhh.edi_837i_parser.claim_service_lines
as
with filtered_lx as
(
    select      *
    from        edwprodhh.edi_837i_parser.response_flat
    where       claim_index is not null --0 Pre-Filter
                and lx_index is not null
)
, servline_lx_header as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_functional_group,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'LX_PREFIX'
                            when    flattened.index = 2   then      'LX_ASSIGNED_LINE_NUMBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^LX.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'LX_PREFIX',
                        'LX_ASSIGNED_LINE_NUMBER'
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
                    LX_INDEX,
                    LX_PREFIX,
                    LX_ASSIGNED_LINE_NUMBER
                )
)
, servline_lx_sv2 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_functional_group,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'SV2_PREFIX'
                            when    flattened.index = 2   then      'REVENUE_CODE'
                            when    flattened.index = 3   then      'PROCEDURE_CODE'
                            when    flattened.index = 4   then      'CHARGE_AMOUNT'
                            when    flattened.index = 5   then      'MEASUREMENT_CODE'
                            when    flattened.index = 6   then      'SERVICE_UNITS'
                            when    flattened.index = 7   then      'SV2_MOD_1'
                            when    flattened.index = 8   then      'SV2_MOD_2'
                            when    flattened.index = 9   then      'SV2_MOD_3'
                            when    flattened.index = 10  then      'SV2_MOD_4'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^SV2.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SV2_PREFIX',
                        'REVENUE_CODE',
                        'PROCEDURE_CODE',
                        'CHARGE_AMOUNT',
                        'MEASUREMENT_CODE',
                        'SERVICE_UNITS',
                        'SV2_MOD_1',
                        'SV2_MOD_2',
                        'SV2_MOD_3',
                        'SV2_MOD_4'
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
                    LX_INDEX,
                    SV2_PREFIX,
                    REVENUE_CODE,
                    PROCEDURE_CODE,
                    CHARGE_AMOUNT,
                    MEASUREMENT_CODE,
                    SERVICE_UNITS,
                    SV2_MOD_1,
                    SV2_MOD_2,
                    SV2_MOD_3,
                    SV2_MOD_4
                )
)
, servline_lx_dtp_471 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_functional_group,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_LX_PRESCRIPTION'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_LX_PRESCRIPTION'
                            when    flattened.index = 3   then      'DATE_FORMAT_LX_PRESCRIPTION'
                            when    flattened.index = 4   then      'DATE_LX_PRESCRIPTION'
                            end     as value_header,

                    case    when    value_header = 'DATE_LX_PRESCRIPTION'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^DTP\\*471.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_LX_PRESCRIPTION',
                        'DATE_QUALIFIER_LX_PRESCRIPTION',
                        'DATE_FORMAT_LX_PRESCRIPTION',
                        'DATE_LX_PRESCRIPTION'
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
                    LX_INDEX,
                    DTP_PREFIX_LX_PRESCRIPTION,
                    DATE_QUALIFIER_LX_PRESCRIPTION,
                    DATE_FORMAT_LX_PRESCRIPTION,
                    DATE_LX_PRESCRIPTION
                )
)
, servline_lx_dtp_472 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_functional_group,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_LX_SERVICE'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_LX_SERVICE'
                            when    flattened.index = 3   then      'DATE_FORMAT_LX_SERVICE'
                            when    flattened.index = 4   then      'DATE_LX_SERVICE'
                            end     as value_header,

                    case    when    value_header = 'DATE_LX_SERVICE'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^DTP\\*472.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_LX_SERVICE',
                        'DATE_QUALIFIER_LX_SERVICE',
                        'DATE_FORMAT_LX_SERVICE',
                        'DATE_LX_SERVICE'
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
                    LX_INDEX,
                    DTP_PREFIX_LX_SERVICE,
                    DATE_QUALIFIER_LX_SERVICE,
                    DATE_FORMAT_LX_SERVICE,
                    DATE_LX_SERVICE
                )
)
, servline_lx_dtp_573 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_functional_group,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_LX_PRESCRIPTION_FILLED'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_LX_PRESCRIPTION_FILLED'
                            when    flattened.index = 3   then      'DATE_FORMAT_LX_PRESCRIPTION_FILLED'
                            when    flattened.index = 4   then      'DATE_LX_PRESCRIPTION_FILLED'
                            end     as value_header,

                    case    when    value_header = 'DATE_LX_PRESCRIPTION_FILLED'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^DTP\\*573.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_LX_PRESCRIPTION_FILLED',
                        'DATE_QUALIFIER_LX_PRESCRIPTION_FILLED',
                        'DATE_FORMAT_LX_PRESCRIPTION_FILLED',
                        'DATE_LX_PRESCRIPTION_FILLED'
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
                    LX_INDEX,
                    DTP_PREFIX_LX_PRESCRIPTION_FILLED,
                    DATE_QUALIFIER_LX_PRESCRIPTION_FILLED,
                    DATE_FORMAT_LX_PRESCRIPTION_FILLED,
                    DATE_LX_PRESCRIPTION_FILLED
                )
)
, servline_lx_ref as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_functional_group,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_PREFIX_LX'
                            when    flattened.index = 2   then      'REFERENCE_ID_QUALIFIER_LX'
                            when    flattened.index = 3   then      'REFERENCE_ID_LX'
                            when    flattened.index = 4   then      'DESCRIPTION_LX'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^REF.*')                         --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'REF_PREFIX_LX',
                            'REFERENCE_ID_QUALIFIER_LX',
                            'REFERENCE_ID_LX',
                            'DESCRIPTION_LX'
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
                        LX_INDEX,
                        REF_PREFIX_LX,
                        REFERENCE_ID_QUALIFIER_LX,
                        REFERENCE_ID_LX,
                        DESCRIPTION_LX
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                claim_index,
                lx_index,
                array_agg(
                    object_construct_keep_null(
                        'claim_ref_code',           reference_id_qualifier_lx::varchar,
                        'claim_ref_value',          reference_id_lx::varchar,
                        'claim_ref_description',    description_lx::varchar
                    )
                )   as lx_ref_array
    from        pivoted
    group by    1,2,3,4,5
    order by    1,2,3,4,5
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
            header.lx_index,
            header.lx_prefix,
            header.lx_assigned_line_number,
            sv2.sv2_prefix,
            sv2.revenue_code,
            sv2.procedure_code,
            sv2.charge_amount,
            sv2.measurement_code,
            sv2.service_units,
            sv2.sv2_mod_1,
            sv2.sv2_mod_2,
            sv2.sv2_mod_3,
            sv2.sv2_mod_4,
            dtp_471.dtp_prefix_lx_prescription,
            dtp_471.date_qualifier_lx_prescription,
            dtp_471.date_format_lx_prescription,
            dtp_471.date_lx_prescription,
            dtp_472.dtp_prefix_lx_service,
            dtp_472.date_qualifier_lx_service,
            dtp_472.date_format_lx_service,
            dtp_472.date_lx_service,
            dtp_573.dtp_prefix_lx_prescription_filled,
            dtp_573.date_qualifier_lx_prescription_filled,
            dtp_573.date_format_lx_prescription_filled,
            dtp_573.date_lx_prescription_filled,
            ref.lx_ref_array

from        servline_lx_header as header
            left join
                servline_lx_sv2 as sv2
                on  header.response_id          = sv2.response_id
                and header.nth_functional_group = sv2.nth_functional_group
                and header.nth_transaction_set  = sv2.nth_transaction_set
                and header.claim_index          = sv2.claim_index
                and header.lx_index             = sv2.lx_index
            left join
                servline_lx_dtp_471 as dtp_471
                on  header.response_id          = dtp_471.response_id
                and header.nth_functional_group = dtp_471.nth_functional_group
                and header.nth_transaction_set  = dtp_471.nth_transaction_set
                and header.claim_index          = dtp_471.claim_index
                and header.lx_index             = dtp_471.lx_index
            left join
                servline_lx_dtp_472 as dtp_472
                on  header.response_id          = dtp_472.response_id
                and header.nth_functional_group = dtp_472.nth_functional_group
                and header.nth_transaction_set  = dtp_472.nth_transaction_set
                and header.claim_index          = dtp_472.claim_index
                and header.lx_index             = dtp_472.lx_index
            left join
                servline_lx_dtp_573 as dtp_573
                on  header.response_id          = dtp_573.response_id
                and header.nth_functional_group = dtp_573.nth_functional_group
                and header.nth_transaction_set  = dtp_573.nth_transaction_set
                and header.claim_index          = dtp_573.claim_index
                and header.lx_index             = dtp_573.lx_index
            left join
                servline_lx_ref as ref
                on  header.response_id          = ref.response_id
                and header.nth_functional_group = ref.nth_functional_group
                and header.nth_transaction_set  = ref.nth_transaction_set
                and header.claim_index          = ref.claim_index
                and header.lx_index             = ref.lx_index
                
order by    1,2,3
;



create or replace task
    edwprodhh.edi_837i_parser.insert_claim_service_lines
    warehouse = analysis_wh
    after edwprodhh.edi_837i_parser.insert_response_flat
as
insert into
    edwprodhh.edi_837i_parser.claim_service_lines
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
    LX_INDEX,
    LX_PREFIX,
    LX_ASSIGNED_LINE_NUMBER,
    SV2_PREFIX,
    REVENUE_CODE,
    PROCEDURE_CODE,
    CHARGE_AMOUNT,
    MEASUREMENT_CODE,
    SERVICE_UNITS,
    SV2_MOD_1,
    SV2_MOD_2,
    SV2_MOD_3,
    SV2_MOD_4,
    DTP_PREFIX_LX_PRESCRIPTION,
    DATE_QUALIFIER_LX_PRESCRIPTION,
    DATE_FORMAT_LX_PRESCRIPTION,
    DATE_LX_PRESCRIPTION,
    DTP_PREFIX_LX_SERVICE,
    DATE_QUALIFIER_LX_SERVICE,
    DATE_FORMAT_LX_SERVICE,
    DATE_LX_SERVICE,
    DTP_PREFIX_LX_PRESCRIPTION_FILLED,
    DATE_QUALIFIER_LX_PRESCRIPTION_FILLED,
    DATE_FORMAT_LX_PRESCRIPTION_FILLED,
    DATE_LX_PRESCRIPTION_FILLED,
    LX_REF_ARRAY
)
with filtered_lx as
(
    select      *
    from        edwprodhh.edi_837i_parser.response_flat
    where       claim_index is not null --0 Pre-Filter
                and lx_index is not null
                and response_id not in (select response_id from edwprodhh.edi_837i_parser.claim_service_lines)
)
, servline_lx_header as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_functional_group,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'LX_PREFIX'
                            when    flattened.index = 2   then      'LX_ASSIGNED_LINE_NUMBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^LX.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'LX_PREFIX',
                        'LX_ASSIGNED_LINE_NUMBER'
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
                    LX_INDEX,
                    LX_PREFIX,
                    LX_ASSIGNED_LINE_NUMBER
                )
)
, servline_lx_sv2 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_functional_group,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'SV2_PREFIX'
                            when    flattened.index = 2   then      'REVENUE_CODE'
                            when    flattened.index = 3   then      'PROCEDURE_CODE'
                            when    flattened.index = 4   then      'CHARGE_AMOUNT'
                            when    flattened.index = 5   then      'MEASUREMENT_CODE'
                            when    flattened.index = 6   then      'SERVICE_UNITS'
                            when    flattened.index = 7   then      'SV2_MOD_1'
                            when    flattened.index = 8   then      'SV2_MOD_2'
                            when    flattened.index = 9   then      'SV2_MOD_3'
                            when    flattened.index = 10  then      'SV2_MOD_4'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^SV2.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SV2_PREFIX',
                        'REVENUE_CODE',
                        'PROCEDURE_CODE',
                        'CHARGE_AMOUNT',
                        'MEASUREMENT_CODE',
                        'SERVICE_UNITS',
                        'SV2_MOD_1',
                        'SV2_MOD_2',
                        'SV2_MOD_3',
                        'SV2_MOD_4'
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
                    LX_INDEX,
                    SV2_PREFIX,
                    REVENUE_CODE,
                    PROCEDURE_CODE,
                    CHARGE_AMOUNT,
                    MEASUREMENT_CODE,
                    SERVICE_UNITS,
                    SV2_MOD_1,
                    SV2_MOD_2,
                    SV2_MOD_3,
                    SV2_MOD_4
                )
)
, servline_lx_dtp_471 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_functional_group,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_LX_PRESCRIPTION'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_LX_PRESCRIPTION'
                            when    flattened.index = 3   then      'DATE_FORMAT_LX_PRESCRIPTION'
                            when    flattened.index = 4   then      'DATE_LX_PRESCRIPTION'
                            end     as value_header,

                    case    when    value_header = 'DATE_LX_PRESCRIPTION'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^DTP\\*471.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_LX_PRESCRIPTION',
                        'DATE_QUALIFIER_LX_PRESCRIPTION',
                        'DATE_FORMAT_LX_PRESCRIPTION',
                        'DATE_LX_PRESCRIPTION'
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
                    LX_INDEX,
                    DTP_PREFIX_LX_PRESCRIPTION,
                    DATE_QUALIFIER_LX_PRESCRIPTION,
                    DATE_FORMAT_LX_PRESCRIPTION,
                    DATE_LX_PRESCRIPTION
                )
)
, servline_lx_dtp_472 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_functional_group,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_LX_SERVICE'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_LX_SERVICE'
                            when    flattened.index = 3   then      'DATE_FORMAT_LX_SERVICE'
                            when    flattened.index = 4   then      'DATE_LX_SERVICE'
                            end     as value_header,

                    case    when    value_header = 'DATE_LX_SERVICE'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^DTP\\*472.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_LX_SERVICE',
                        'DATE_QUALIFIER_LX_SERVICE',
                        'DATE_FORMAT_LX_SERVICE',
                        'DATE_LX_SERVICE'
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
                    LX_INDEX,
                    DTP_PREFIX_LX_SERVICE,
                    DATE_QUALIFIER_LX_SERVICE,
                    DATE_FORMAT_LX_SERVICE,
                    DATE_LX_SERVICE
                )
)
, servline_lx_dtp_573 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_functional_group,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_LX_PRESCRIPTION_FILLED'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_LX_PRESCRIPTION_FILLED'
                            when    flattened.index = 3   then      'DATE_FORMAT_LX_PRESCRIPTION_FILLED'
                            when    flattened.index = 4   then      'DATE_LX_PRESCRIPTION_FILLED'
                            end     as value_header,

                    case    when    value_header = 'DATE_LX_PRESCRIPTION_FILLED'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^DTP\\*573.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_LX_PRESCRIPTION_FILLED',
                        'DATE_QUALIFIER_LX_PRESCRIPTION_FILLED',
                        'DATE_FORMAT_LX_PRESCRIPTION_FILLED',
                        'DATE_LX_PRESCRIPTION_FILLED'
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
                    LX_INDEX,
                    DTP_PREFIX_LX_PRESCRIPTION_FILLED,
                    DATE_QUALIFIER_LX_PRESCRIPTION_FILLED,
                    DATE_FORMAT_LX_PRESCRIPTION_FILLED,
                    DATE_LX_PRESCRIPTION_FILLED
                )
)
, servline_lx_ref as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_functional_group,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_PREFIX_LX'
                            when    flattened.index = 2   then      'REFERENCE_ID_QUALIFIER_LX'
                            when    flattened.index = 3   then      'REFERENCE_ID_LX'
                            when    flattened.index = 4   then      'DESCRIPTION_LX'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^REF.*')                         --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'REF_PREFIX_LX',
                            'REFERENCE_ID_QUALIFIER_LX',
                            'REFERENCE_ID_LX',
                            'DESCRIPTION_LX'
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
                        LX_INDEX,
                        REF_PREFIX_LX,
                        REFERENCE_ID_QUALIFIER_LX,
                        REFERENCE_ID_LX,
                        DESCRIPTION_LX
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                claim_index,
                lx_index,
                array_agg(
                    object_construct_keep_null(
                        'claim_ref_code',           reference_id_qualifier_lx::varchar,
                        'claim_ref_value',          reference_id_lx::varchar,
                        'claim_ref_description',    description_lx::varchar
                    )
                )   as lx_ref_array
    from        pivoted
    group by    1,2,3,4,5
    order by    1,2,3,4,5
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
            header.lx_index,
            header.lx_prefix,
            header.lx_assigned_line_number,
            sv2.sv2_prefix,
            sv2.revenue_code,
            sv2.procedure_code,
            sv2.charge_amount,
            sv2.measurement_code,
            sv2.service_units,
            sv2.sv2_mod_1,
            sv2.sv2_mod_2,
            sv2.sv2_mod_3,
            sv2.sv2_mod_4,
            dtp_471.dtp_prefix_lx_prescription,
            dtp_471.date_qualifier_lx_prescription,
            dtp_471.date_format_lx_prescription,
            dtp_471.date_lx_prescription,
            dtp_472.dtp_prefix_lx_service,
            dtp_472.date_qualifier_lx_service,
            dtp_472.date_format_lx_service,
            dtp_472.date_lx_service,
            dtp_573.dtp_prefix_lx_prescription_filled,
            dtp_573.date_qualifier_lx_prescription_filled,
            dtp_573.date_format_lx_prescription_filled,
            dtp_573.date_lx_prescription_filled,
            ref.lx_ref_array

from        servline_lx_header as header
            left join
                servline_lx_sv2 as sv2
                on  header.response_id          = sv2.response_id
                and header.nth_functional_group = sv2.nth_functional_group
                and header.nth_transaction_set  = sv2.nth_transaction_set
                and header.claim_index          = sv2.claim_index
                and header.lx_index             = sv2.lx_index
            left join
                servline_lx_dtp_471 as dtp_471
                on  header.response_id          = dtp_471.response_id
                and header.nth_functional_group = dtp_471.nth_functional_group
                and header.nth_transaction_set  = dtp_471.nth_transaction_set
                and header.claim_index          = dtp_471.claim_index
                and header.lx_index             = dtp_471.lx_index
            left join
                servline_lx_dtp_472 as dtp_472
                on  header.response_id          = dtp_472.response_id
                and header.nth_functional_group = dtp_472.nth_functional_group
                and header.nth_transaction_set  = dtp_472.nth_transaction_set
                and header.claim_index          = dtp_472.claim_index
                and header.lx_index             = dtp_472.lx_index
            left join
                servline_lx_dtp_573 as dtp_573
                on  header.response_id          = dtp_573.response_id
                and header.nth_functional_group = dtp_573.nth_functional_group
                and header.nth_transaction_set  = dtp_573.nth_transaction_set
                and header.claim_index          = dtp_573.claim_index
                and header.lx_index             = dtp_573.lx_index
            left join
                servline_lx_ref as ref
                on  header.response_id          = ref.response_id
                and header.nth_functional_group = ref.nth_functional_group
                and header.nth_transaction_set  = ref.nth_transaction_set
                and header.claim_index          = ref.claim_index
                and header.lx_index             = ref.lx_index
                
order by    1,2,3
;