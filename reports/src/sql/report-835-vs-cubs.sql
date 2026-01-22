with remits_835 as
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
select      remits.claim_id,
            remits.claim_id_format,
            remits.claim_status_code,
            remits.claim_charge_amount,
            remits.claim_payment_amount,
            remits.claim_patient_resp_amount,

            debtor.debtor_idx,
            debtor.fullname,
            debtor.client_debtornumber as cdn,
            debtor.client_packet as drl,

            trans.trans_idx,
            trans.post_date,
            trans.sig_trans_amt,
            trans.sig_comm_amt
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
-- where       remits.claim_payment_amount > 0
--             and trans.trans_idx is null
order by    1
;