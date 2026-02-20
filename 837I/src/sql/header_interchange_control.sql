create or replace table
    edwprodhh.edi_837i_parser.header_interchange_control
as
with header_isa as
(
    with long as
    (
        select      flatten_837.response_id,
                    flatten_837.index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'INTERCHANGE_CONTROL_HEADER'
                            when    flattened.index = 2   then      'AUTHORIZATION_INFORMATION_QUALIFIER'
                            when    flattened.index = 3   then      'AUTHORIZATION_INFORMATION'
                            when    flattened.index = 4   then      'SECURITY_INFORMATION_QUALIFIER'
                            when    flattened.index = 5   then      'SECURITY_INFORMATION'
                            when    flattened.index = 6   then      'INTERCHANGE_ID_QUALIFIER_SENDER'
                            when    flattened.index = 7   then      'INTERCHANGE_SENDER_ID'
                            when    flattened.index = 8   then      'INTERCHANGE_ID_QUALIFIER_RECEIVER'
                            when    flattened.index = 9   then      'INTERCHANGE_RECEIVER_ID'
                            when    flattened.index = 10  then      'INTERCHANGE_DATE'
                            when    flattened.index = 11  then      'INTERCHANGE_TIME'
                            when    flattened.index = 12  then      'REPETITION_SEPARATOR'
                            when    flattened.index = 13  then      'INTERCHANGE_CONTROL_VERSION'
                            when    flattened.index = 14  then      'INTERCHANGE_CONTROL_NUMBER'
                            when    flattened.index = 15  then      'ACKNOWLEDGEMENT_REQUESTED'
                            when    flattened.index = 16  then      'USAGE_INDICATOR'
                            when    flattened.index = 17  then      'COMPONENT_SEPARATOR'
                            end     as value_header,

                    case    when    value_header = 'INTERCHANGE_DATE'   then    to_date(nullif(trim(flattened.value), ''), 'YYMMDD')::text
                            when    value_header = 'INTERCHANGE_TIME'   then    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        edwprodhh.edi_837i_parser.response_flat as flatten_837,
                    lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_837.line_element_837, '^ISA.*')                         --1 Filter
    )
    select      *,
                (interchange_date || ' ' || interchange_time)::timestamp as interchange_timestamp
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'INTERCHANGE_CONTROL_HEADER',
                        'AUTHORIZATION_INFORMATION_QUALIFIER',
                        'AUTHORIZATION_INFORMATION',
                        'SECURITY_INFORMATION_QUALIFIER',
                        'SECURITY_INFORMATION',
                        'INTERCHANGE_ID_QUALIFIER_SENDER',
                        'INTERCHANGE_SENDER_ID',
                        'INTERCHANGE_ID_QUALIFIER_RECEIVER',
                        'INTERCHANGE_RECEIVER_ID',
                        'INTERCHANGE_DATE',
                        'INTERCHANGE_TIME',
                        'REPETITION_SEPARATOR',
                        'INTERCHANGE_CONTROL_VERSION',
                        'INTERCHANGE_CONTROL_NUMBER',
                        'ACKNOWLEDGEMENT_REQUESTED',
                        'USAGE_INDICATOR',
                        'COMPONENT_SEPARATOR'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    INDEX,
                    INTERCHANGE_CONTROL_HEADER,
                    AUTHORIZATION_INFORMATION_QUALIFIER,
                    AUTHORIZATION_INFORMATION,
                    SECURITY_INFORMATION_QUALIFIER,
                    SECURITY_INFORMATION,
                    INTERCHANGE_ID_QUALIFIER_SENDER,
                    INTERCHANGE_SENDER_ID,
                    INTERCHANGE_ID_QUALIFIER_RECEIVER,
                    INTERCHANGE_RECEIVER_ID,
                    INTERCHANGE_DATE,
                    INTERCHANGE_TIME,
                    REPETITION_SEPARATOR,
                    INTERCHANGE_CONTROL_VERSION,
                    INTERCHANGE_CONTROL_NUMBER,
                    ACKNOWLEDGEMENT_REQUESTED,
                    USAGE_INDICATOR,
                    COMPONENT_SEPARATOR
                )
    order by    1
)
, trailer_iea as
(
    with long as
    (
        select      flatten_837.response_id,
                    flatten_837.index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'INTERCHANGE_CONTROL_TRAILER'
                            when    flattened.index = 2   then      'GS_COUNT_INCLUDED'
                            when    flattened.index = 3   then      'INTERCHANGE_CONTROL_NUMBER_IEA'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        edwprodhh.edi_837i_parser.response_flat as flatten_837,
                    lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_837.line_element_837, '^IEA.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'INTERCHANGE_CONTROL_TRAILER',
                        'GS_COUNT_INCLUDED',
                        'INTERCHANGE_CONTROL_NUMBER_IEA'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    INDEX,
                    INTERCHANGE_CONTROL_TRAILER,
                    GS_COUNT_INCLUDED,
                    INTERCHANGE_CONTROL_NUMBER_IEA
                )
    order by    1
)
select      header_isa.response_id,
            header_isa.index as index_isa,
            header_isa.interchange_control_header,
            header_isa.authorization_information_qualifier,
            header_isa.authorization_information,
            header_isa.security_information_qualifier,
            header_isa.security_information,
            header_isa.interchange_id_qualifier_sender,
            header_isa.interchange_sender_id,
            header_isa.interchange_id_qualifier_receiver,
            header_isa.interchange_receiver_id,
            header_isa.interchange_date,
            header_isa.interchange_time,
            header_isa.repetition_separator,
            header_isa.interchange_control_version,
            header_isa.interchange_control_number,
            header_isa.acknowledgement_requested,
            header_isa.usage_indicator,
            header_isa.component_separator,
            header_isa.interchange_timestamp,
            trailer_iea.index as index_iea,
            trailer_iea.interchange_control_trailer,
            trailer_iea.gs_count_included,
            trailer_iea.interchange_control_number_iea
from        header_isa
            left join
                trailer_iea
                on  header_isa.response_id = trailer_iea.response_id
;



create or replace task
    edwprodhh.edi_837i_parser.insert_header_interchange_control
    warehouse = analysis_wh
    after edwprodhh.edi_837i_parser.insert_response_flat
as
insert into
    edwprodhh.edi_837i_parser.header_interchange_control
(
    RESPONSE_ID,
    INDEX_ISA,
    INTERCHANGE_CONTROL_HEADER,
    AUTHORIZATION_INFORMATION_QUALIFIER,
    AUTHORIZATION_INFORMATION,
    SECURITY_INFORMATION_QUALIFIER,
    SECURITY_INFORMATION,
    INTERCHANGE_ID_QUALIFIER_SENDER,
    INTERCHANGE_SENDER_ID,
    INTERCHANGE_ID_QUALIFIER_RECEIVER,
    INTERCHANGE_RECEIVER_ID,
    INTERCHANGE_DATE,
    INTERCHANGE_TIME,
    REPETITION_SEPARATOR,
    INTERCHANGE_CONTROL_VERSION,
    INTERCHANGE_CONTROL_NUMBER,
    ACKNOWLEDGEMENT_REQUESTED,
    USAGE_INDICATOR,
    COMPONENT_SEPARATOR,
    INTERCHANGE_TIMESTAMP,
    INDEX_IEA,
    INTERCHANGE_CONTROL_TRAILER,
    GS_COUNT_INCLUDED,
    INTERCHANGE_CONTROL_NUMBER_IEA
)
with header_isa as
(
    with long as
    (
        select      flatten_837.response_id,
                    flatten_837.index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'INTERCHANGE_CONTROL_HEADER'
                            when    flattened.index = 2   then      'AUTHORIZATION_INFORMATION_QUALIFIER'
                            when    flattened.index = 3   then      'AUTHORIZATION_INFORMATION'
                            when    flattened.index = 4   then      'SECURITY_INFORMATION_QUALIFIER'
                            when    flattened.index = 5   then      'SECURITY_INFORMATION'
                            when    flattened.index = 6   then      'INTERCHANGE_ID_QUALIFIER_SENDER'
                            when    flattened.index = 7   then      'INTERCHANGE_SENDER_ID'
                            when    flattened.index = 8   then      'INTERCHANGE_ID_QUALIFIER_RECEIVER'
                            when    flattened.index = 9   then      'INTERCHANGE_RECEIVER_ID'
                            when    flattened.index = 10  then      'INTERCHANGE_DATE'
                            when    flattened.index = 11  then      'INTERCHANGE_TIME'
                            when    flattened.index = 12  then      'REPETITION_SEPARATOR'
                            when    flattened.index = 13  then      'INTERCHANGE_CONTROL_VERSION'
                            when    flattened.index = 14  then      'INTERCHANGE_CONTROL_NUMBER'
                            when    flattened.index = 15  then      'ACKNOWLEDGEMENT_REQUESTED'
                            when    flattened.index = 16  then      'USAGE_INDICATOR'
                            when    flattened.index = 17  then      'COMPONENT_SEPARATOR'
                            end     as value_header,

                    case    when    value_header = 'INTERCHANGE_DATE'   then    to_date(nullif(trim(flattened.value), ''), 'YYMMDD')::text
                            when    value_header = 'INTERCHANGE_TIME'   then    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        edwprodhh.edi_837i_parser.response_flat as flatten_837,
                    lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_837.line_element_837, '^ISA.*')                         --1 Filter
                    and flatten_837.response_id not in (select response_id from edwprodhh.edi_837i_parser.header_interchange_control)
    )
    select      *,
                (interchange_date || ' ' || interchange_time)::timestamp as interchange_timestamp
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'INTERCHANGE_CONTROL_HEADER',
                        'AUTHORIZATION_INFORMATION_QUALIFIER',
                        'AUTHORIZATION_INFORMATION',
                        'SECURITY_INFORMATION_QUALIFIER',
                        'SECURITY_INFORMATION',
                        'INTERCHANGE_ID_QUALIFIER_SENDER',
                        'INTERCHANGE_SENDER_ID',
                        'INTERCHANGE_ID_QUALIFIER_RECEIVER',
                        'INTERCHANGE_RECEIVER_ID',
                        'INTERCHANGE_DATE',
                        'INTERCHANGE_TIME',
                        'REPETITION_SEPARATOR',
                        'INTERCHANGE_CONTROL_VERSION',
                        'INTERCHANGE_CONTROL_NUMBER',
                        'ACKNOWLEDGEMENT_REQUESTED',
                        'USAGE_INDICATOR',
                        'COMPONENT_SEPARATOR'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    INDEX,
                    INTERCHANGE_CONTROL_HEADER,
                    AUTHORIZATION_INFORMATION_QUALIFIER,
                    AUTHORIZATION_INFORMATION,
                    SECURITY_INFORMATION_QUALIFIER,
                    SECURITY_INFORMATION,
                    INTERCHANGE_ID_QUALIFIER_SENDER,
                    INTERCHANGE_SENDER_ID,
                    INTERCHANGE_ID_QUALIFIER_RECEIVER,
                    INTERCHANGE_RECEIVER_ID,
                    INTERCHANGE_DATE,
                    INTERCHANGE_TIME,
                    REPETITION_SEPARATOR,
                    INTERCHANGE_CONTROL_VERSION,
                    INTERCHANGE_CONTROL_NUMBER,
                    ACKNOWLEDGEMENT_REQUESTED,
                    USAGE_INDICATOR,
                    COMPONENT_SEPARATOR
                )
    order by    1
)
, trailer_iea as
(
    with long as
    (
        select      flatten_837.response_id,
                    flatten_837.index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'INTERCHANGE_CONTROL_TRAILER'
                            when    flattened.index = 2   then      'GS_COUNT_INCLUDED'
                            when    flattened.index = 3   then      'INTERCHANGE_CONTROL_NUMBER_IEA'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        edwprodhh.edi_837i_parser.response_flat as flatten_837,
                    lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_837.line_element_837, '^IEA.*')                         --1 Filter
                    and flatten_837.response_id not in (select response_id from edwprodhh.edi_837i_parser.header_interchange_control)
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'INTERCHANGE_CONTROL_TRAILER',
                        'GS_COUNT_INCLUDED',
                        'INTERCHANGE_CONTROL_NUMBER_IEA'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    INDEX,
                    INTERCHANGE_CONTROL_TRAILER,
                    GS_COUNT_INCLUDED,
                    INTERCHANGE_CONTROL_NUMBER_IEA
                )
    order by    1
)
select      header_isa.response_id,
            header_isa.index as index_isa,
            header_isa.interchange_control_header,
            header_isa.authorization_information_qualifier,
            header_isa.authorization_information,
            header_isa.security_information_qualifier,
            header_isa.security_information,
            header_isa.interchange_id_qualifier_sender,
            header_isa.interchange_sender_id,
            header_isa.interchange_id_qualifier_receiver,
            header_isa.interchange_receiver_id,
            header_isa.interchange_date,
            header_isa.interchange_time,
            header_isa.repetition_separator,
            header_isa.interchange_control_version,
            header_isa.interchange_control_number,
            header_isa.acknowledgement_requested,
            header_isa.usage_indicator,
            header_isa.component_separator,
            header_isa.interchange_timestamp,
            trailer_iea.index as index_iea,
            trailer_iea.interchange_control_trailer,
            trailer_iea.gs_count_included,
            trailer_iea.interchange_control_number_iea
from        header_isa
            left join
                trailer_iea
                on  header_isa.response_id = trailer_iea.response_id
;