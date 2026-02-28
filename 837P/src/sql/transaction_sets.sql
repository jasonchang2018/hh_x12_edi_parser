create or replace table
    edwprodhh.edi_837p_parser.transaction_sets
as
with filtered as
(
    select      *
    from        edwprodhh.edi_837p_parser.response_flat
)
, header_st as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'TRANSACTION_SET_HEADER'
                            when    flattened.index = 2   then      'TRANSACTION_SET_ID_CODE'
                            when    flattened.index = 3   then      'TRANSACTION_SET_CONTROL_NUMBER_HEADER'
                            when    flattened.index = 4   then      'IMPLEMENTATION_CONVENTION_REFERENCE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_837, '^ST.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'TRANSACTION_SET_HEADER',
                        'TRANSACTION_SET_ID_CODE',
                        'TRANSACTION_SET_CONTROL_NUMBER_HEADER',
                        'IMPLEMENTATION_CONVENTION_REFERENCE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    TRANSACTION_SET_HEADER,
                    TRANSACTION_SET_ID_CODE,
                    TRANSACTION_SET_CONTROL_NUMBER_HEADER,
                    IMPLEMENTATION_CONVENTION_REFERENCE
                )
)
, trailer_se as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'TRANSACTION_SET_TRAILER'
                            when    flattened.index = 2   then      'TRANSACTION_SEGMENT_COUNT'
                            when    flattened.index = 3   then      'TRANSACTION_SET_CONTROL_NUMBER_TRAILER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_837, '^SE.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'TRANSACTION_SET_TRAILER',
                        'TRANSACTION_SEGMENT_COUNT',
                        'TRANSACTION_SET_CONTROL_NUMBER_TRAILER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    TRANSACTION_SET_TRAILER,
                    TRANSACTION_SEGMENT_COUNT,
                    TRANSACTION_SET_CONTROL_NUMBER_TRAILER
                )
)
, beginning_bht as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'BEGINNING_OF_HIERARCHICAL_TRANSACTION'
                            when    flattened.index = 2   then      'HIERARCHICAL_STRUCTURE_CODE'
                            when    flattened.index = 3   then      'TRANSACTION_SET_PURPOSE_CODE'
                            when    flattened.index = 4   then      'ORIGINATOR_APPLICATION_TRANSACTION_ID'
                            when    flattened.index = 5   then      'TRANSACTION_SET_CREATED_DATE'
                            when    flattened.index = 6   then      'TRANSACTION_SET_CREATED_TIME'
                            when    flattened.index = 7   then      'TRANSACTION_TYPE_CODE'
                            end     as value_header,

                    case    when    value_header = 'TRANSACTION_SET_CREATED_DATE'  then     to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'TRANSACTION_SET_CREATED_TIME'  then     case    when    length(nullif(trim(flattened.value), '')) = 4
                                                                                                    then    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                                                                                    else    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                                                                                    end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_837, '^BHT.*')                         --1 Filter
    )
    select      *,
                (TRANSACTION_SET_CREATED_DATE || ' ' || TRANSACTION_SET_CREATED_TIME)::timestamp as TRANSACTION_SET_CREATED_timestamp
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'BEGINNING_OF_HIERARCHICAL_TRANSACTION',
                        'HIERARCHICAL_STRUCTURE_CODE',
                        'TRANSACTION_SET_PURPOSE_CODE',
                        'ORIGINATOR_APPLICATION_TRANSACTION_ID',
                        'TRANSACTION_SET_CREATED_DATE',
                        'TRANSACTION_SET_CREATED_TIME',
                        'TRANSACTION_TYPE_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    BEGINNING_OF_HIERARCHICAL_TRANSACTION,
                    HIERARCHICAL_STRUCTURE_CODE,
                    TRANSACTION_SET_PURPOSE_CODE,
                    ORIGINATOR_APPLICATION_TRANSACTION_ID,
                    TRANSACTION_SET_CREATED_DATE,
                    TRANSACTION_SET_CREATED_TIME,
                    TRANSACTION_TYPE_CODE
                )
)
, submitter_nm41 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_SUBMITTER'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_SUBMITTER'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_SUBMITTER'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_SUBMITTER'
                            when    flattened.index = 5   then      'FIRST_NAME_SUBMITTER'
                            when    flattened.index = 6   then      'MIDDLE_NAME_SUBMITTER'
                            when    flattened.index = 7   then      'NAME_PREFIX_SUBMITTER'
                            when    flattened.index = 8   then      'NAME_SUFFIX_SUBMITTER'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_SUBMITTER'
                            when    flattened.index = 10  then      'ID_CODE_SUBMITTER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_837, '^NM1\\*41.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_SUBMITTER',
                        'ENTITY_IDENTIFIER_CODE_SUBMITTER',
                        'ENTITY_TYPE_QUALIFIER_SUBMITTER',
                        'LAST_NAME_ORG_SUBMITTER',
                        'FIRST_NAME_SUBMITTER',
                        'MIDDLE_NAME_SUBMITTER',
                        'NAME_PREFIX_SUBMITTER',
                        'NAME_SUFFIX_SUBMITTER',
                        'ID_CODE_QUALIFIER_SUBMITTER',
                        'ID_CODE_SUBMITTER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    NAME_CODE_SUBMITTER,
                    ENTITY_IDENTIFIER_CODE_SUBMITTER,
                    ENTITY_TYPE_QUALIFIER_SUBMITTER,
                    LAST_NAME_ORG_SUBMITTER,
                    FIRST_NAME_SUBMITTER,
                    MIDDLE_NAME_SUBMITTER,
                    NAME_PREFIX_SUBMITTER,
                    NAME_SUFFIX_SUBMITTER,
                    ID_CODE_QUALIFIER_SUBMITTER,
                    ID_CODE_SUBMITTER
                )
)
, submitter_nm41_per as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'SUBMITTER_CONTACT_PREFIX'
                            when    flattened.index = 2   then      'CONTACT_FUNCTION_CODE'
                            when    flattened.index = 3   then      'SUBMITTER_CONTACT_NAME'
                            when    flattened.index = 4   then      'COMMUNICATION_QUALIFIER_1'
                            when    flattened.index = 5   then      'COMMUNICATION_NUMBER_1'
                            when    flattened.index = 6   then      'COMMUNICATION_QUALIFIER_2'
                            when    flattened.index = 7   then      'COMMUNICATION_NUMBER_2'
                            when    flattened.index = 8   then      'COMMUNICATION_QUALIFIER_3'
                            when    flattened.index = 9   then      'COMMUNICATION_NUMBER_3'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_837, '^PER.*')                         --1 Filter
                    and filtered.lag_name_indicator = 'NM1*41'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SUBMITTER_CONTACT_PREFIX',
                        'CONTACT_FUNCTION_CODE',
                        'SUBMITTER_CONTACT_NAME',
                        'COMMUNICATION_QUALIFIER_1',
                        'COMMUNICATION_NUMBER_1',
                        'COMMUNICATION_QUALIFIER_2',
                        'COMMUNICATION_NUMBER_2',
                        'COMMUNICATION_QUALIFIER_3',
                        'COMMUNICATION_NUMBER_3'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    SUBMITTER_CONTACT_PREFIX,
                    CONTACT_FUNCTION_CODE,
                    SUBMITTER_CONTACT_NAME,
                    COMMUNICATION_QUALIFIER_1,
                    COMMUNICATION_NUMBER_1,
                    COMMUNICATION_QUALIFIER_2,
                    COMMUNICATION_NUMBER_2,
                    COMMUNICATION_QUALIFIER_3,
                    COMMUNICATION_NUMBER_3
                )
)
, receiver_nm40 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_RECEIVER'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_RECEIVER'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_RECEIVER'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_RECEIVER'
                            when    flattened.index = 5   then      'FIRST_NAME_RECEIVER'
                            when    flattened.index = 6   then      'MIDDLE_NAME_RECEIVER'
                            when    flattened.index = 7   then      'NAME_PREFIX_RECEIVER'
                            when    flattened.index = 8   then      'NAME_SUFFIX_RECEIVER'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_RECEIVER'
                            when    flattened.index = 10  then      'ID_CODE_RECEIVER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_837, '^NM1\\*40.*')                   --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_RECEIVER',
                        'ENTITY_IDENTIFIER_CODE_RECEIVER',
                        'ENTITY_TYPE_QUALIFIER_RECEIVER',
                        'LAST_NAME_ORG_RECEIVER',
                        'FIRST_NAME_RECEIVER',
                        'MIDDLE_NAME_RECEIVER',
                        'NAME_PREFIX_RECEIVER',
                        'NAME_SUFFIX_RECEIVER',
                        'ID_CODE_QUALIFIER_RECEIVER',
                        'ID_CODE_RECEIVER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    NAME_CODE_RECEIVER,
                    ENTITY_IDENTIFIER_CODE_RECEIVER,
                    ENTITY_TYPE_QUALIFIER_RECEIVER,
                    LAST_NAME_ORG_RECEIVER,
                    FIRST_NAME_RECEIVER,
                    MIDDLE_NAME_RECEIVER,
                    NAME_PREFIX_RECEIVER,
                    NAME_SUFFIX_RECEIVER,
                    ID_CODE_QUALIFIER_RECEIVER,
                    ID_CODE_RECEIVER
                )
)
select      header.response_id,
            header.nth_transaction_set,
            header.transaction_set_header,
            header.transaction_set_id_code,
            header.transaction_set_control_number_header,
            header.implementation_convention_reference,
            trailer.transaction_set_trailer,
            trailer.transaction_segment_count,
            trailer.transaction_set_control_number_trailer,
            bht.beginning_of_hierarchical_transaction,
            bht.hierarchical_structure_code,
            bht.transaction_set_purpose_code,
            bht.originator_application_transaction_id,
            bht.transaction_set_created_date,
            bht.transaction_set_created_time,
            bht.transaction_type_code,
            nm41.name_code_submitter,
            nm41.entity_identifier_code_submitter,
            nm41.entity_type_qualifier_submitter,
            nm41.last_name_org_submitter,
            nm41.first_name_submitter,
            nm41.middle_name_submitter,
            nm41.name_prefix_submitter,
            nm41.name_suffix_submitter,
            nm41.id_code_qualifier_submitter,
            nm41.id_code_submitter,
            nm41_per.submitter_contact_prefix,
            nm41_per.contact_function_code,
            nm41_per.submitter_contact_name,
            nm41_per.communication_qualifier_1,
            nm41_per.communication_number_1,
            nm41_per.communication_qualifier_2,
            nm41_per.communication_number_2,
            nm41_per.communication_qualifier_3,
            nm41_per.communication_number_3,
            nm40.name_code_receiver,
            nm40.entity_identifier_code_receiver,
            nm40.entity_type_qualifier_receiver,
            nm40.last_name_org_receiver,
            nm40.first_name_receiver,
            nm40.middle_name_receiver,
            nm40.name_prefix_receiver,
            nm40.name_suffix_receiver,
            nm40.id_code_qualifier_receiver,
            nm40.id_code_receiver

from        header_st               as header
            left join
                trailer_se          as trailer
                on  header.response_id                   = trailer.response_id
                and header.nth_transaction_set      = trailer.nth_transaction_set
            left join
                beginning_bht       as bht
                on  header.response_id                   = bht.response_id
                and header.nth_transaction_set      = bht.nth_transaction_set
            left join
                submitter_nm41      as nm41
                on  header.response_id                   = nm41.response_id
                and header.nth_transaction_set      = nm41.nth_transaction_set
            left join
                submitter_nm41_per  as nm41_per
                on  header.response_id                   = nm41_per.response_id
                and header.nth_transaction_set      = nm41_per.nth_transaction_set
            left join
                receiver_nm40       as nm40
                on  header.response_id                   = nm40.response_id
                and header.nth_transaction_set      = nm40.nth_transaction_set
order by    1,2,3
;



create or replace task
    edwprodhh.edi_837p_parser.insert_transaction_sets
    warehouse = analysis_wh
    after edwprodhh.edi_837p_parser.insert_response_flat
as
insert into
    edwprodhh.edi_837p_parser.transaction_sets
(
    RESPONSE_ID,
    NTH_TRANSACTION_SET,
    TRANSACTION_SET_HEADER,
    TRANSACTION_SET_ID_CODE,
    TRANSACTION_SET_CONTROL_NUMBER_HEADER,
    IMPLEMENTATION_CONVENTION_REFERENCE,
    TRANSACTION_SET_TRAILER,
    TRANSACTION_SEGMENT_COUNT,
    TRANSACTION_SET_CONTROL_NUMBER_TRAILER,
    BEGINNING_OF_HIERARCHICAL_TRANSACTION,
    HIERARCHICAL_STRUCTURE_CODE,
    TRANSACTION_SET_PURPOSE_CODE,
    ORIGINATOR_APPLICATION_TRANSACTION_ID,
    TRANSACTION_SET_CREATED_DATE,
    TRANSACTION_SET_CREATED_TIME,
    TRANSACTION_TYPE_CODE,
    NAME_CODE_SUBMITTER,
    ENTITY_IDENTIFIER_CODE_SUBMITTER,
    ENTITY_TYPE_QUALIFIER_SUBMITTER,
    LAST_NAME_ORG_SUBMITTER,
    FIRST_NAME_SUBMITTER,
    MIDDLE_NAME_SUBMITTER,
    NAME_PREFIX_SUBMITTER,
    NAME_SUFFIX_SUBMITTER,
    ID_CODE_QUALIFIER_SUBMITTER,
    ID_CODE_SUBMITTER,
    SUBMITTER_CONTACT_PREFIX,
    CONTACT_FUNCTION_CODE,
    SUBMITTER_CONTACT_NAME,
    COMMUNICATION_QUALIFIER_1,
    COMMUNICATION_NUMBER_1,
    COMMUNICATION_QUALIFIER_2,
    COMMUNICATION_NUMBER_2,
    COMMUNICATION_QUALIFIER_3,
    COMMUNICATION_NUMBER_3,
    NAME_CODE_RECEIVER,
    ENTITY_IDENTIFIER_CODE_RECEIVER,
    ENTITY_TYPE_QUALIFIER_RECEIVER,
    LAST_NAME_ORG_RECEIVER,
    FIRST_NAME_RECEIVER,
    MIDDLE_NAME_RECEIVER,
    NAME_PREFIX_RECEIVER,
    NAME_SUFFIX_RECEIVER,
    ID_CODE_QUALIFIER_RECEIVER,
    ID_CODE_RECEIVER
)
with filtered as
(
    select      *
    from        edwprodhh.edi_837p_parser.response_flat
    where       response_id not in (select response_id from edwprodhh.edi_837p_parser.transaction_sets)
)
, header_st as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'TRANSACTION_SET_HEADER'
                            when    flattened.index = 2   then      'TRANSACTION_SET_ID_CODE'
                            when    flattened.index = 3   then      'TRANSACTION_SET_CONTROL_NUMBER_HEADER'
                            when    flattened.index = 4   then      'IMPLEMENTATION_CONVENTION_REFERENCE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_837, '^ST.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'TRANSACTION_SET_HEADER',
                        'TRANSACTION_SET_ID_CODE',
                        'TRANSACTION_SET_CONTROL_NUMBER_HEADER',
                        'IMPLEMENTATION_CONVENTION_REFERENCE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    TRANSACTION_SET_HEADER,
                    TRANSACTION_SET_ID_CODE,
                    TRANSACTION_SET_CONTROL_NUMBER_HEADER,
                    IMPLEMENTATION_CONVENTION_REFERENCE
                )
)
, trailer_se as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'TRANSACTION_SET_TRAILER'
                            when    flattened.index = 2   then      'TRANSACTION_SEGMENT_COUNT'
                            when    flattened.index = 3   then      'TRANSACTION_SET_CONTROL_NUMBER_TRAILER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_837, '^SE.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'TRANSACTION_SET_TRAILER',
                        'TRANSACTION_SEGMENT_COUNT',
                        'TRANSACTION_SET_CONTROL_NUMBER_TRAILER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    TRANSACTION_SET_TRAILER,
                    TRANSACTION_SEGMENT_COUNT,
                    TRANSACTION_SET_CONTROL_NUMBER_TRAILER
                )
)
, beginning_bht as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'BEGINNING_OF_HIERARCHICAL_TRANSACTION'
                            when    flattened.index = 2   then      'HIERARCHICAL_STRUCTURE_CODE'
                            when    flattened.index = 3   then      'TRANSACTION_SET_PURPOSE_CODE'
                            when    flattened.index = 4   then      'ORIGINATOR_APPLICATION_TRANSACTION_ID'
                            when    flattened.index = 5   then      'TRANSACTION_SET_CREATED_DATE'
                            when    flattened.index = 6   then      'TRANSACTION_SET_CREATED_TIME'
                            when    flattened.index = 7   then      'TRANSACTION_TYPE_CODE'
                            end     as value_header,

                    case    when    value_header = 'TRANSACTION_SET_CREATED_DATE'  then     to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'TRANSACTION_SET_CREATED_TIME'  then     case    when    length(nullif(trim(flattened.value), '')) = 4
                                                                                                    then    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                                                                                    else    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                                                                                    end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_837, '^BHT.*')                         --1 Filter
    )
    select      *,
                (TRANSACTION_SET_CREATED_DATE || ' ' || TRANSACTION_SET_CREATED_TIME)::timestamp as TRANSACTION_SET_CREATED_timestamp
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'BEGINNING_OF_HIERARCHICAL_TRANSACTION',
                        'HIERARCHICAL_STRUCTURE_CODE',
                        'TRANSACTION_SET_PURPOSE_CODE',
                        'ORIGINATOR_APPLICATION_TRANSACTION_ID',
                        'TRANSACTION_SET_CREATED_DATE',
                        'TRANSACTION_SET_CREATED_TIME',
                        'TRANSACTION_TYPE_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    BEGINNING_OF_HIERARCHICAL_TRANSACTION,
                    HIERARCHICAL_STRUCTURE_CODE,
                    TRANSACTION_SET_PURPOSE_CODE,
                    ORIGINATOR_APPLICATION_TRANSACTION_ID,
                    TRANSACTION_SET_CREATED_DATE,
                    TRANSACTION_SET_CREATED_TIME,
                    TRANSACTION_TYPE_CODE
                )
)
, submitter_nm41 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_SUBMITTER'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_SUBMITTER'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_SUBMITTER'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_SUBMITTER'
                            when    flattened.index = 5   then      'FIRST_NAME_SUBMITTER'
                            when    flattened.index = 6   then      'MIDDLE_NAME_SUBMITTER'
                            when    flattened.index = 7   then      'NAME_PREFIX_SUBMITTER'
                            when    flattened.index = 8   then      'NAME_SUFFIX_SUBMITTER'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_SUBMITTER'
                            when    flattened.index = 10  then      'ID_CODE_SUBMITTER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_837, '^NM1\\*41.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_SUBMITTER',
                        'ENTITY_IDENTIFIER_CODE_SUBMITTER',
                        'ENTITY_TYPE_QUALIFIER_SUBMITTER',
                        'LAST_NAME_ORG_SUBMITTER',
                        'FIRST_NAME_SUBMITTER',
                        'MIDDLE_NAME_SUBMITTER',
                        'NAME_PREFIX_SUBMITTER',
                        'NAME_SUFFIX_SUBMITTER',
                        'ID_CODE_QUALIFIER_SUBMITTER',
                        'ID_CODE_SUBMITTER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    NAME_CODE_SUBMITTER,
                    ENTITY_IDENTIFIER_CODE_SUBMITTER,
                    ENTITY_TYPE_QUALIFIER_SUBMITTER,
                    LAST_NAME_ORG_SUBMITTER,
                    FIRST_NAME_SUBMITTER,
                    MIDDLE_NAME_SUBMITTER,
                    NAME_PREFIX_SUBMITTER,
                    NAME_SUFFIX_SUBMITTER,
                    ID_CODE_QUALIFIER_SUBMITTER,
                    ID_CODE_SUBMITTER
                )
)
, submitter_nm41_per as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'SUBMITTER_CONTACT_PREFIX'
                            when    flattened.index = 2   then      'CONTACT_FUNCTION_CODE'
                            when    flattened.index = 3   then      'SUBMITTER_CONTACT_NAME'
                            when    flattened.index = 4   then      'COMMUNICATION_QUALIFIER_1'
                            when    flattened.index = 5   then      'COMMUNICATION_NUMBER_1'
                            when    flattened.index = 6   then      'COMMUNICATION_QUALIFIER_2'
                            when    flattened.index = 7   then      'COMMUNICATION_NUMBER_2'
                            when    flattened.index = 8   then      'COMMUNICATION_QUALIFIER_3'
                            when    flattened.index = 9   then      'COMMUNICATION_NUMBER_3'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_837, '^PER.*')                         --1 Filter
                    and filtered.lag_name_indicator = 'NM1*41'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SUBMITTER_CONTACT_PREFIX',
                        'CONTACT_FUNCTION_CODE',
                        'SUBMITTER_CONTACT_NAME',
                        'COMMUNICATION_QUALIFIER_1',
                        'COMMUNICATION_NUMBER_1',
                        'COMMUNICATION_QUALIFIER_2',
                        'COMMUNICATION_NUMBER_2',
                        'COMMUNICATION_QUALIFIER_3',
                        'COMMUNICATION_NUMBER_3'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    SUBMITTER_CONTACT_PREFIX,
                    CONTACT_FUNCTION_CODE,
                    SUBMITTER_CONTACT_NAME,
                    COMMUNICATION_QUALIFIER_1,
                    COMMUNICATION_NUMBER_1,
                    COMMUNICATION_QUALIFIER_2,
                    COMMUNICATION_NUMBER_2,
                    COMMUNICATION_QUALIFIER_3,
                    COMMUNICATION_NUMBER_3
                )
)
, receiver_nm40 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_RECEIVER'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_RECEIVER'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_RECEIVER'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_RECEIVER'
                            when    flattened.index = 5   then      'FIRST_NAME_RECEIVER'
                            when    flattened.index = 6   then      'MIDDLE_NAME_RECEIVER'
                            when    flattened.index = 7   then      'NAME_PREFIX_RECEIVER'
                            when    flattened.index = 8   then      'NAME_SUFFIX_RECEIVER'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_RECEIVER'
                            when    flattened.index = 10  then      'ID_CODE_RECEIVER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_837, '^NM1\\*40.*')                   --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_RECEIVER',
                        'ENTITY_IDENTIFIER_CODE_RECEIVER',
                        'ENTITY_TYPE_QUALIFIER_RECEIVER',
                        'LAST_NAME_ORG_RECEIVER',
                        'FIRST_NAME_RECEIVER',
                        'MIDDLE_NAME_RECEIVER',
                        'NAME_PREFIX_RECEIVER',
                        'NAME_SUFFIX_RECEIVER',
                        'ID_CODE_QUALIFIER_RECEIVER',
                        'ID_CODE_RECEIVER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    NAME_CODE_RECEIVER,
                    ENTITY_IDENTIFIER_CODE_RECEIVER,
                    ENTITY_TYPE_QUALIFIER_RECEIVER,
                    LAST_NAME_ORG_RECEIVER,
                    FIRST_NAME_RECEIVER,
                    MIDDLE_NAME_RECEIVER,
                    NAME_PREFIX_RECEIVER,
                    NAME_SUFFIX_RECEIVER,
                    ID_CODE_QUALIFIER_RECEIVER,
                    ID_CODE_RECEIVER
                )
)
select      header.response_id,
            header.nth_transaction_set,
            header.transaction_set_header,
            header.transaction_set_id_code,
            header.transaction_set_control_number_header,
            header.implementation_convention_reference,
            trailer.transaction_set_trailer,
            trailer.transaction_segment_count,
            trailer.transaction_set_control_number_trailer,
            bht.beginning_of_hierarchical_transaction,
            bht.hierarchical_structure_code,
            bht.transaction_set_purpose_code,
            bht.originator_application_transaction_id,
            bht.transaction_set_created_date,
            bht.transaction_set_created_time,
            bht.transaction_type_code,
            nm41.name_code_submitter,
            nm41.entity_identifier_code_submitter,
            nm41.entity_type_qualifier_submitter,
            nm41.last_name_org_submitter,
            nm41.first_name_submitter,
            nm41.middle_name_submitter,
            nm41.name_prefix_submitter,
            nm41.name_suffix_submitter,
            nm41.id_code_qualifier_submitter,
            nm41.id_code_submitter,
            nm41_per.submitter_contact_prefix,
            nm41_per.contact_function_code,
            nm41_per.submitter_contact_name,
            nm41_per.communication_qualifier_1,
            nm41_per.communication_number_1,
            nm41_per.communication_qualifier_2,
            nm41_per.communication_number_2,
            nm41_per.communication_qualifier_3,
            nm41_per.communication_number_3,
            nm40.name_code_receiver,
            nm40.entity_identifier_code_receiver,
            nm40.entity_type_qualifier_receiver,
            nm40.last_name_org_receiver,
            nm40.first_name_receiver,
            nm40.middle_name_receiver,
            nm40.name_prefix_receiver,
            nm40.name_suffix_receiver,
            nm40.id_code_qualifier_receiver,
            nm40.id_code_receiver

from        header_st               as header
            left join
                trailer_se          as trailer
                on  header.response_id                   = trailer.response_id
                and header.nth_transaction_set      = trailer.nth_transaction_set
            left join
                beginning_bht       as bht
                on  header.response_id                   = bht.response_id
                and header.nth_transaction_set      = bht.nth_transaction_set
            left join
                submitter_nm41      as nm41
                on  header.response_id                   = nm41.response_id
                and header.nth_transaction_set      = nm41.nth_transaction_set
            left join
                submitter_nm41_per  as nm41_per
                on  header.response_id                   = nm41_per.response_id
                and header.nth_transaction_set      = nm41_per.nth_transaction_set
            left join
                receiver_nm40       as nm40
                on  header.response_id                   = nm40.response_id
                and header.nth_transaction_set      = nm40.nth_transaction_set
order by    1,2,3
;