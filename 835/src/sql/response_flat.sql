create or replace table
    edwprodhh.edi_835_parser.response_flat
as
with flattened as
(
    select      response_id,
                to_date(regexp_substr(file_name, '835_(\\d{8})', 1, 1, 'e'), 'yyyymmdd')                    as file_date, --*this will need to update to be conditional on pl_group
                line_number                                                                                 as index,
                case    when    regexp_like(response_body, '^(ISA|GS)\\*.*')
                        then    response_body
                        else    trim(regexp_replace(regexp_replace(response_body, '\\s+', ' '), '~', ''))
                        end                                                                                 as line_element_835
    from        edwprodhh.edi_835_parser.response
)
, add_functional_group_index as
(  
    select      *,

                count_if(regexp_like(line_element_835, '^GS.*')) over (partition by response_id order by index asc)
                    as nth_functional_group

    from        flattened
)
, add_transaction_set_index as
(  
    select      *,

                count_if(regexp_like(line_element_835, '^ST.*')) over (partition by response_id, nth_functional_group order by index asc)
                    as nth_transaction_set

    from        add_functional_group_index
)
, add_lx_index as
(
    select      *,
                max(case when regexp_like(line_element_835, '^LX.*') then index end) over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc)
                    as lx_index,

                coalesce(
                    regexp_substr(line_element_835, '^(N1\\*[^\\*]*)'),
                    lag(case when regexp_like(line_element_835, '^N1.*') then regexp_substr(line_element_835, '^(N1\\*[^\\*]*)') end) ignore nulls over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc)
                )   as lag_n1_indicator

    from        add_transaction_set_index
)
, add_svc_index as
(
    select      *,
                max(case when regexp_like(line_element_835, '^SVC.*') then index end) over (partition by response_id, nth_functional_group, nth_transaction_set, lx_index order by index asc)
                    as svc_index,
                
                coalesce(
                    regexp_substr(line_element_835, '^(LX\\*[^\\*]*)'),
                    lag(case when regexp_like(line_element_835, '^LX.*') then regexp_substr(line_element_835, '^(LX\\*[^\\*]*)') end) ignore nulls over (partition by response_id, nth_functional_group, nth_transaction_set, lx_index order by index asc)
                )   as lag_lx_indicator

    from        add_lx_index
)
, add_svc_indicator as
(
    select      *,
                
                coalesce(
                    regexp_substr(line_element_835, '^(SVC)') || '-' || svc_index,
                    lag(case when regexp_like(line_element_835, '^SVC.*') then regexp_substr(line_element_835, '^(SVC)') || '-' || svc_index end) ignore nulls over (partition by response_id, nth_functional_group, nth_transaction_set, lx_index, svc_index order by index asc)
                )   as lag_svc_indicator

    from        add_svc_index
)
select      *
from        add_svc_indicator
;



create or replace task
    edwprodhh.edi_835_parser.insert_response_flat
    warehouse = analysis_wh
    after edwprodhh.edi_835_parser.sp_insert_835_from_stage
as
insert into
    edwprodhh.edi_835_parser.response_flat
(
    RESPONSE_ID,
    FILE_DATE,
    INDEX,
    LINE_ELEMENT_835,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    LX_INDEX,
    LAG_N1_INDICATOR,
    SVC_INDEX,
    LAG_LX_INDICATOR,
    LAG_SVC_INDICATOR
)
with flattened as
(
    select      response_id,
                to_date(regexp_substr(file_name, '835_(\\d{8})', 1, 1, 'e'), 'yyyymmdd')                    as file_date,
                line_number                                                                                 as index,
                case    when    regexp_like(response_body, '^(ISA|GS)\\*.*')
                        then    response_body
                        else    trim(regexp_replace(regexp_replace(response_body, '\\s+', ' '), '~', ''))
                        end                                                                                 as line_element_835
    from        edwprodhh.edi_835_parser.response
    where       response_id not in (select response_id from edwprodhh.edi_835_parser.response_flat)
)
, add_functional_group_index as
(  
    select      *,

                count_if(regexp_like(line_element_835, '^GS.*')) over (partition by response_id order by index asc)
                    as nth_functional_group

    from        flattened
)
, add_transaction_set_index as
(  
    select      *,

                count_if(regexp_like(line_element_835, '^ST.*')) over (partition by response_id, nth_functional_group order by index asc)
                    as nth_transaction_set

    from        add_functional_group_index
)
, add_lx_index as
(
    select      *,
                max(case when regexp_like(line_element_835, '^LX.*') then index end) over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc)
                    as lx_index,

                coalesce(
                    regexp_substr(line_element_835, '^(N1\\*[^\\*]*)'),
                    lag(case when regexp_like(line_element_835, '^N1.*') then regexp_substr(line_element_835, '^(N1\\*[^\\*]*)') end) ignore nulls over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc)
                )   as lag_n1_indicator

    from        add_transaction_set_index
)
, add_svc_index as
(
    select      *,
                max(case when regexp_like(line_element_835, '^SVC.*') then index end) over (partition by response_id, nth_functional_group, nth_transaction_set, lx_index order by index asc)
                    as svc_index,
                
                coalesce(
                    regexp_substr(line_element_835, '^(LX\\*[^\\*]*)'),
                    lag(case when regexp_like(line_element_835, '^LX.*') then regexp_substr(line_element_835, '^(LX\\*[^\\*]*)') end) ignore nulls over (partition by response_id, nth_functional_group, nth_transaction_set, lx_index order by index asc)
                )   as lag_lx_indicator

    from        add_lx_index
)
, add_svc_indicator as
(
    select      *,
                
                coalesce(
                    regexp_substr(line_element_835, '^(SVC)') || '-' || svc_index,
                    lag(case when regexp_like(line_element_835, '^SVC.*') then regexp_substr(line_element_835, '^(SVC)') || '-' || svc_index end) ignore nulls over (partition by response_id, nth_functional_group, nth_transaction_set, lx_index, svc_index order by index asc)
                )   as lag_svc_indicator

    from        add_svc_index
)
select      *
from        add_svc_indicator
;