create or replace table
    edwprodhh.edi_837i_parser.header_functional_group
as
with header_gs as
(
    with long as
    (
        select      flatten_837.response_id,
                    flatten_837.nth_functional_group,
                    flatten_837.index,
        
                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then    'FUNCTIONAL_GROUP_HEADER'
                            when    flattened.index = 2     then    'FUNCTIONAL_IDENTIFIER_CODE'
                            when    flattened.index = 3     then    'APPLICATION_SENDER_CODE'
                            when    flattened.index = 4     then    'APPLICATION_RECEIVER_CODE'
                            when    flattened.index = 5     then    'FUNCTIONAL_GROUP_CREATED_DATE'
                            when    flattened.index = 6     then    'FUNCTIONAL_GROUP_CREATED_TIME'
                            when    flattened.index = 7     then    'CONTROL_GROUP_NUMBER'
                            when    flattened.index = 8     then    'RESPONSIBLE_AGENCY_CODE'
                            when    flattened.index = 9     then    'VERSION_IDENTIFIER_CODE'
                            end     as value_header,

                    case    when    value_header = 'FUNCTIONAL_GROUP_CREATED_DATE'  then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'FUNCTIONAL_GROUP_CREATED_TIME'  then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                            else    nullif(trim(flattened.value), '')
                            end     as value_format


        from        edwprodhh.edi_837i_parser.response_flat as flatten_837,
                    lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_837.line_element_837, '^GS.*')                          --1 Filter
    )
    select      *,
                (functional_group_created_date || ' ' || functional_group_created_time)::timestamp as functional_group_created_timestamp
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'FUNCTIONAL_GROUP_HEADER',
                        'FUNCTIONAL_IDENTIFIER_CODE',
                        'APPLICATION_SENDER_CODE',
                        'APPLICATION_RECEIVER_CODE',
                        'FUNCTIONAL_GROUP_CREATED_DATE',
                        'FUNCTIONAL_GROUP_CREATED_TIME',
                        'CONTROL_GROUP_NUMBER',
                        'RESPONSIBLE_AGENCY_CODE',
                        'VERSION_IDENTIFIER_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    INDEX,
                    FUNCTIONAL_GROUP_HEADER,
                    FUNCTIONAL_IDENTIFIER_CODE,
                    APPLICATION_SENDER_CODE,
                    APPLICATION_RECEIVER_CODE,
                    FUNCTIONAL_GROUP_CREATED_DATE,
                    FUNCTIONAL_GROUP_CREATED_TIME,
                    CONTROL_GROUP_NUMBER,
                    RESPONSIBLE_AGENCY_CODE,
                    VERSION_IDENTIFIER_CODE
                )
    order by    1
)
, trailer_ge as
(
    with long as
    (
        select      flatten_837.response_id,
                    flatten_837.nth_functional_group,
                    flatten_837.index,
        
                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then    'FUNCTIONAL_GROUP_TRAILER'
                            when    flattened.index = 2     then    'TS_COUNT_INCLUDED'
                            when    flattened.index = 3     then    'GROUP_CONTROL_NUMBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format


        from        edwprodhh.edi_837i_parser.response_flat as flatten_837,
                    lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_837.line_element_837, '^GE.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'FUNCTIONAL_GROUP_TRAILER',
                        'TS_COUNT_INCLUDED',
                        'GROUP_CONTROL_NUMBER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    INDEX,
                    FUNCTIONAL_GROUP_TRAILER,
                    TS_COUNT_INCLUDED,
                    GROUP_CONTROL_NUMBER
                )
    order by    1
)
select      header_gs.response_id,
            header_gs.nth_functional_group,
            header_gs.index as index_gs,
            header_gs.functional_group_header,
            header_gs.functional_identifier_code,
            header_gs.application_sender_code,
            header_gs.application_receiver_code,
            header_gs.functional_group_created_date,
            header_gs.functional_group_created_time,
            header_gs.control_group_number,
            header_gs.responsible_agency_code,
            header_gs.version_identifier_code,
            header_gs.functional_group_created_timestamp,
            trailer_ge.index as index_ge,
            trailer_ge.functional_group_trailer,
            trailer_ge.ts_count_included,
            trailer_ge.group_control_number
from        header_gs
            left join
                trailer_ge
                on  header_gs.response_id           = trailer_ge.response_id
                and header_gs.nth_functional_group  = trailer_ge.nth_functional_group
;



create or replace task
    edwprodhh.edi_837i_parser.insert_header_functional_group
    warehouse = analysis_wh
    after edwprodhh.edi_837i_parser.insert_response_flat
as
insert into
    edwprodhh.edi_837i_parser.header_functional_group
(
    RESPONSE_ID,
    NTH_FUNCTIONAL_GROUP,
    INDEX_GS,
    FUNCTIONAL_GROUP_HEADER,
    FUNCTIONAL_IDENTIFIER_CODE,
    APPLICATION_SENDER_CODE,
    APPLICATION_RECEIVER_CODE,
    FUNCTIONAL_GROUP_CREATED_DATE,
    FUNCTIONAL_GROUP_CREATED_TIME,
    CONTROL_GROUP_NUMBER,
    RESPONSIBLE_AGENCY_CODE,
    VERSION_IDENTIFIER_CODE,
    FUNCTIONAL_GROUP_CREATED_TIMESTAMP,
    INDEX_GE,
    FUNCTIONAL_GROUP_TRAILER,
    TS_COUNT_INCLUDED,
    GROUP_CONTROL_NUMBER
)
with header_gs as
(
    with long as
    (
        select      flatten_837.response_id,
                    flatten_837.nth_functional_group,
                    flatten_837.index,
        
                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then    'FUNCTIONAL_GROUP_HEADER'
                            when    flattened.index = 2     then    'FUNCTIONAL_IDENTIFIER_CODE'
                            when    flattened.index = 3     then    'APPLICATION_SENDER_CODE'
                            when    flattened.index = 4     then    'APPLICATION_RECEIVER_CODE'
                            when    flattened.index = 5     then    'FUNCTIONAL_GROUP_CREATED_DATE'
                            when    flattened.index = 6     then    'FUNCTIONAL_GROUP_CREATED_TIME'
                            when    flattened.index = 7     then    'CONTROL_GROUP_NUMBER'
                            when    flattened.index = 8     then    'RESPONSIBLE_AGENCY_CODE'
                            when    flattened.index = 9     then    'VERSION_IDENTIFIER_CODE'
                            end     as value_header,

                    case    when    value_header = 'FUNCTIONAL_GROUP_CREATED_DATE'  then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'FUNCTIONAL_GROUP_CREATED_TIME'  then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                            else    nullif(trim(flattened.value), '')
                            end     as value_format


        from        edwprodhh.edi_837i_parser.response_flat as flatten_837,
                    lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_837.line_element_837, '^GS.*')                          --1 Filter
                    and flatten_837.response_id not in (select response_id from edwprodhh.edi_837i_parser.header_functional_group)
    )
    select      *,
                (functional_group_created_date || ' ' || functional_group_created_time)::timestamp as functional_group_created_timestamp
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'FUNCTIONAL_GROUP_HEADER',
                        'FUNCTIONAL_IDENTIFIER_CODE',
                        'APPLICATION_SENDER_CODE',
                        'APPLICATION_RECEIVER_CODE',
                        'FUNCTIONAL_GROUP_CREATED_DATE',
                        'FUNCTIONAL_GROUP_CREATED_TIME',
                        'CONTROL_GROUP_NUMBER',
                        'RESPONSIBLE_AGENCY_CODE',
                        'VERSION_IDENTIFIER_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    INDEX,
                    FUNCTIONAL_GROUP_HEADER,
                    FUNCTIONAL_IDENTIFIER_CODE,
                    APPLICATION_SENDER_CODE,
                    APPLICATION_RECEIVER_CODE,
                    FUNCTIONAL_GROUP_CREATED_DATE,
                    FUNCTIONAL_GROUP_CREATED_TIME,
                    CONTROL_GROUP_NUMBER,
                    RESPONSIBLE_AGENCY_CODE,
                    VERSION_IDENTIFIER_CODE
                )
    order by    1
)
, trailer_ge as
(
    with long as
    (
        select      flatten_837.response_id,
                    flatten_837.nth_functional_group,
                    flatten_837.index,
        
                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then    'FUNCTIONAL_GROUP_TRAILER'
                            when    flattened.index = 2     then    'TS_COUNT_INCLUDED'
                            when    flattened.index = 3     then    'GROUP_CONTROL_NUMBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format


        from        edwprodhh.edi_837i_parser.response_flat as flatten_837,
                    lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_837.line_element_837, '^GE.*')                          --1 Filter
                    and flatten_837.response_id not in (select response_id from edwprodhh.edi_837i_parser.header_functional_group)
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'FUNCTIONAL_GROUP_TRAILER',
                        'TS_COUNT_INCLUDED',
                        'GROUP_CONTROL_NUMBER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    INDEX,
                    FUNCTIONAL_GROUP_TRAILER,
                    TS_COUNT_INCLUDED,
                    GROUP_CONTROL_NUMBER
                )
    order by    1
)
select      header_gs.response_id,
            header_gs.nth_functional_group,
            header_gs.index as index_gs,
            header_gs.functional_group_header,
            header_gs.functional_identifier_code,
            header_gs.application_sender_code,
            header_gs.application_receiver_code,
            header_gs.functional_group_created_date,
            header_gs.functional_group_created_time,
            header_gs.control_group_number,
            header_gs.responsible_agency_code,
            header_gs.version_identifier_code,
            header_gs.functional_group_created_timestamp,
            trailer_ge.index as index_ge,
            trailer_ge.functional_group_trailer,
            trailer_ge.ts_count_included,
            trailer_ge.group_control_number
from        header_gs
            left join
                trailer_ge
                on  header_gs.response_id           = trailer_ge.response_id
                and header_gs.nth_functional_group  = trailer_ge.nth_functional_group
;