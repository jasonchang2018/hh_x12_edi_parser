create or replace view
    edwprodhh.edi_835_parser.report_835_funnel
as
with claims_837 as
(
    with claims as
    (
        with unioned as
        (
            select      distinct
                        claim_id,
                        upload_date
            from        edwprodhh.edi_837p_parser.export_data_dimensions_log
            union all
            select      distinct
                        claim_id,
                        upload_date
            from        edwprodhh.edi_837i_parser.export_data_dimensions_log
        )
        select      claim_id            as claim_id_format,
                    min(upload_date)    as upload_date
        from        unioned
        group by    1
    )
    , map_claim_id as
    (
        with unioned_map as
        (
            select      line_element_837,
            from        edwprodhh.edi_837p_parser.export_data_dimensions_log
            where       left(line_element_837, 3) = 'CLM'
            union all
            select      line_element_837,
            from        edwprodhh.edi_837i_parser.export_data_dimensions_log
            where       left(line_element_837, 3) = 'CLM'
        )
        select      regexp_substr(line_element_837, 'CLM\\*([^\\*]*)', 1, 1, 'e')   as claim_id,
                    regexp_substr(ltrim(claim_id, '0'), '(\\d*$)', 1, 1, 'e')       as claim_id_format,
        from        unioned_map
    )
    , get_claim_info as
    (
        with unioned as
        (
            select      response_id,
                        claim_id,
                        total_claim_charge
            from        edwprodhh.edi_837i_parser.claims
            union all
            select      response_id,
                        claim_id,
                        total_claim_charge
            from        edwprodhh.edi_837p_parser.claims
        )
        select      unioned.*
        from        unioned
                    left join
                        (
                            select      distinct
                                        response_id,
                                        file_date
                            from        edwprodhh.edi_837i_parser.response_flat
                        )   as file_dates
                        on unioned.response_id = file_dates.response_id
        qualify     row_number() over (partition by unioned.claim_id order by file_dates.file_date desc) = 1
    )
    select      claims.claim_id_format,
                claims.upload_date,
                map_claim_id.claim_id,
                coalesce(get_claim_info.total_claim_charge, 0) as total_claim_charge_837
    from        claims
                inner join
                    map_claim_id
                    on claims.claim_id_format = map_claim_id.claim_id_format
                left join
                    get_claim_info
                    on map_claim_id.claim_id = get_claim_info.claim_id
)
, status_277 as
(
    select      trn_trace_id                                                                                        as claim_id,
                regexp_substr(stc_status_category_code, '(^[^\\:]*)')                                               as status_category,
                regexp_substr(regexp_replace(stc_status_category_code, status_category || '\\:'), '(^[^\\:]*)')     as status_code,
                stc_date                                                                                            as status_date,
                case    when    status_category in ('A0', 'A1', 'A2', 'R4')
                        then    1
                        else    0
                        end                                                                                         as is_accepted
                -- stc_status_category_code,
                -- stc_action_code,
                -- stc_monetary_amount
    from        edwprodhh.edi_277_parser.hl_pt_patient
    qualify     row_number() over (partition by claim_id order by stc_date desc, is_accepted desc) = 1
)
, remits_835 as
(
    select      clp_claim_id                                                    as claim_id,
                regexp_substr(ltrim(clp_claim_id, '0'), '(\\d*$)', 1, 1, 'e')   as claim_id_format,
                clp_claim_status_code                                           as claim_status_code,
                clp_claim_charge_amount::number(18,2)                           as claim_charge_amount,
                clp_claim_payment_amount::number(18,2)                          as claim_payment_amount,
                clp_claim_patient_resp_amount::number(18,2)                     as claim_patient_resp_amount,
                clp_claim_filing_indicator_code                                 as claim_filing_indicator_code,
                clp_claim_payer_control_num                                     as claim_payer_control_num,
                dtm_232_date,
                dtm_233_date,
                dtm_050_date
    from        edwprodhh.edi_835_parser.remits
)
, posted_cubs as
(
    select      remits.claim_id,
                /*Inflates match when multiple payments of same amount. 
                  Assumes cubs posts at claim level and not at service line level.*/
                max(case when trans.trans_idx is not null then 1 else 0 end) as is_posted_cubs_,
                sum(trans.sig_trans_amt) as dol_posted_cubs
    from        remits_835 as remits
                left join
                    edwprodhh.pub_jchang.master_debtor as debtor
                    on remits.claim_id_format = debtor.client_debtornumber
                    and debtor.pl_group in (
                        'IU HEALTH - TPL'
                    )
                left join
                    edwprodhh.pub_jchang.master_transactions as trans
                    on  debtor.debtor_idx = trans.debtor_idx
                    and trans.is_payment = 1
                    and remits.claim_payment_amount = trans.sig_trans_amt
    group by    1
    order by    1
)
, joined as
(
    select      claims_837.*,

                1                                                                                           as is_submit_837,
                case when status_277.claim_id is not null then 1 else 0 end                                 as is_response_277,
                coalesce(status_277.is_accepted, 0)                                                         as is_accepted_277,
                case when remits_835.claim_id is not null then 1 else 0 end                                 as is_remit_835,
                case when coalesce(remits_835.claim_status_code, '0') in ('1', '2', '3') then 1 else 0 end  as is_processed_835,
                case when remits_835.claim_payment_amount > 0 then 1 else 0 end                             as is_paid_835,
                coalesce(posted_cubs.is_posted_cubs_, 0)                                                    as is_posted_cubs,

                remits_835.claim_charge_amount,
                remits_835.claim_payment_amount as claim_payment_amount_835,
                remits_835.claim_patient_resp_amount,
                posted_cubs.dol_posted_cubs


    from        claims_837
                left join
                    status_277
                    on claims_837.claim_id = status_277.claim_id
                left join
                    remits_835
                    on claims_837.claim_id = remits_835.claim_id
                left join
                    posted_cubs
                    on claims_837.claim_id = posted_cubs.claim_id
                    
    order by    1
)
select      upload_date,

            sum(is_submit_837)                                                  as n_submit_837,
            sum(is_response_277)                                                as n_response_277,
            sum(is_accepted_277)                                                as n_accepted_277,
            sum(is_remit_835)                                                   as n_remit_835,
            sum(is_processed_835)                                               as n_processed_835,
            sum(is_paid_835)                                                    as n_paid_835,
            sum(is_posted_cubs)                                                 as n_posted_cubs,

            -- avg(case when is_submit_837     = 1 then is_response_277    end)    as p_response_277,
            -- avg(case when is_response_277   = 1 then is_accepted_277    end)    as p_accepted_277,
            -- avg(case when is_accepted_277   = 1 then is_remit_835       end)    as p_remit_835,
            -- avg(case when is_remit_835      = 1 then is_processed_835   end)    as p_processed_835,
            -- avg(case when is_processed_835  = 1 then is_paid_835        end)    as p_paid_835,
            -- avg(case when is_paid_835       = 1 then is_posted_cubs     end)    as p_posted_cubs,

            sum(total_claim_charge_837)                                         as total_claim_charge_837,
            sum(claim_payment_amount_835)                                       as claim_payment_amount_835,
            sum(dol_posted_cubs)                                                as dol_posted_cubs

from        joined
group by    1
order by    1
;