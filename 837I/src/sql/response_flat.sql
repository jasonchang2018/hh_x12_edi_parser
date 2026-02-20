create or replace table
    edwprodhh.edi_837i_parser.response_flat
as
with flattened as
(
    select      response_id,
                to_date(regexp_replace(regexp_substr(file_name, '(_\\d{6}_)'), '_', ''), 'yymmdd')          as file_date, --*this will need to update to be conditional on pl_group
                line_number                                                                                 as index,
                case    when    regexp_like(response_body, '^(ISA|GS)\\*.*')
                        then    response_body
                        else    trim(regexp_replace(regexp_replace(response_body, '\\s+', ' '), '~', ''))
                        end                                                                                 as line_element_837
    from        edwprodhh.edi_837i_parser.response
)
, add_functional_group_index as
(  
    select      *,
                count_if(regexp_like(line_element_837, '^GS.*')) over (partition by response_id order by index asc) as nth_functional_group
    from        flattened
)
, add_transaction_set_index as
(  
    select      *,
                count_if(regexp_like(line_element_837, '^ST.*')) over (partition by response_id, nth_functional_group order by index asc) as nth_transaction_set
    from        add_functional_group_index
)
, add_hl_index as
(
    select      *,
                max(case when regexp_like(line_element_837, '^HL.*')                then index end) over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc) as hl_index_current,
                max(case when regexp_like(line_element_837, '^HL.*20\\*[^\\*]*$')   then index end) over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc) as hl_index_billing_20,
                max(case when regexp_like(line_element_837, '^HL.*22\\*[^\\*]*$')   then index end) over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc) as hl_index_subscriber_22,
                max(case when regexp_like(line_element_837, '^HL.*23\\*[^\\*]*$')   then index end) over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc) as hl_index_patient_23,
                
    from        add_transaction_set_index
)
, add_clm_index as
(
    select      *,
                max(case when regexp_like(line_element_837, '^CLM.*')               then index end)                                                             over (partition by response_id, nth_functional_group, nth_transaction_set, hl_index_current order by index asc)   as claim_index,
                coalesce(
                    regexp_substr(line_element_837, '^(NM1\\*[^\\*]*)'),
                    lag(case when regexp_like(line_element_837, '^NM1.*')           then regexp_substr(line_element_837, '^(NM1\\*[^\\*]*)') end) ignore nulls  over (partition by response_id, nth_functional_group, nth_transaction_set, hl_index_current order by index asc)
                )                                                                                                                                                                                                                                           as lag_name_indicator
    from        add_hl_index
)
, add_lx_index as
(
    select      *,
                max(case when regexp_like(line_element_837, '^LX.*')                then index end) over (partition by response_id, nth_functional_group, nth_transaction_set, claim_index order by index asc) as lx_index,
                max(case when regexp_like(line_element_837, '^SBR.*')               then index end) over (partition by response_id, nth_functional_group, nth_transaction_set, claim_index order by index asc) as other_sbr_index
    from        add_clm_index
)
select      response_id,
            file_date,
            index,
            line_element_837,
            nth_functional_group,
            nth_transaction_set,
            hl_index_current,
            hl_index_billing_20,
            hl_index_subscriber_22,
            hl_index_patient_23,
            claim_index,
            lag_name_indicator,
            lx_index,
            other_sbr_index
from        add_lx_index
order by    1,2
;



create or replace task
    edwprodhh.edi_837i_parser.insert_response_flat
    warehouse = analysis_wh
    after edwprodhh.edi_837i_parser.sp_insert_837i_from_stage
as
insert into
    edwprodhh.edi_837i_parser.response_flat
(
    RESPONSE_ID,
    FILE_DATE,
    INDEX,
    LINE_ELEMENT_837,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    HL_INDEX_CURRENT,
    HL_INDEX_BILLING_20,
    HL_INDEX_SUBSCRIBER_22,
    HL_INDEX_PATIENT_23,
    CLAIM_INDEX,
    LAG_NAME_INDICATOR,
    LX_INDEX,
    OTHER_SBR_INDEX
)
with flattened as
(
    select      response_id,
                to_date(regexp_replace(regexp_substr(file_name, '(_\\d{6}_)'), '_', ''), 'yymmdd')          as file_date,
                line_number                                                                                 as index,
                case    when    regexp_like(response_body, '^(ISA|GS)\\*.*')
                        then    response_body
                        else    trim(regexp_replace(regexp_replace(response_body, '\\s+', ' '), '~', ''))
                        end                                                                                 as line_element_837
    from        edwprodhh.edi_837i_parser.response
    where       response_id not in (select response_id from edwprodhh.edi_837i_parser.response_flat)
)
, add_functional_group_index as
(  
    select      *,
                count_if(regexp_like(line_element_837, '^GS.*')) over (partition by response_id order by index asc) as nth_functional_group
    from        flattened
)
, add_transaction_set_index as
(  
    select      *,
                count_if(regexp_like(line_element_837, '^ST.*')) over (partition by response_id, nth_functional_group order by index asc) as nth_transaction_set
    from        add_functional_group_index
)
, add_hl_index as
(
    select      *,
                max(case when regexp_like(line_element_837, '^HL.*')                then index end) over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc) as hl_index_current,
                max(case when regexp_like(line_element_837, '^HL.*20\\*[^\\*]*$')   then index end) over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc) as hl_index_billing_20,
                max(case when regexp_like(line_element_837, '^HL.*22\\*[^\\*]*$')   then index end) over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc) as hl_index_subscriber_22,
                max(case when regexp_like(line_element_837, '^HL.*23\\*[^\\*]*$')   then index end) over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc) as hl_index_patient_23,
                
    from        add_transaction_set_index
)
, add_clm_index as
(
    select      *,
                max(case when regexp_like(line_element_837, '^CLM.*')               then index end)                                                             over (partition by response_id, nth_functional_group, nth_transaction_set, hl_index_current order by index asc)   as claim_index,
                coalesce(
                    regexp_substr(line_element_837, '^(NM1\\*[^\\*]*)'),
                    lag(case when regexp_like(line_element_837, '^NM1.*')           then regexp_substr(line_element_837, '^(NM1\\*[^\\*]*)') end) ignore nulls  over (partition by response_id, nth_functional_group, nth_transaction_set, hl_index_current order by index asc)
                )                                                                                                                                                                                                                                           as lag_name_indicator
    from        add_hl_index
)
, add_lx_index as
(
    select      *,
                max(case when regexp_like(line_element_837, '^LX.*')                then index end) over (partition by response_id, nth_functional_group, nth_transaction_set, claim_index order by index asc) as lx_index,
                max(case when regexp_like(line_element_837, '^SBR.*')               then index end) over (partition by response_id, nth_functional_group, nth_transaction_set, claim_index order by index asc) as other_sbr_index
    from        add_clm_index
)
select      response_id,
            file_date,
            index,
            line_element_837,
            nth_functional_group,
            nth_transaction_set,
            hl_index_current,
            hl_index_billing_20,
            hl_index_subscriber_22,
            hl_index_patient_23,
            claim_index,
            lag_name_indicator,
            lx_index,
            other_sbr_index
from        add_lx_index
order by    1,2
;