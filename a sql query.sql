WITH cte_uniquecredits
     AS (SELECT *,
                Row_number()
                  OVER (
                    partition BY wva_commitmentoid
                    ORDER BY wva_commitmentoid, overriddencreatedon DESC) rnk
         FROM   stg1.designated_credit_commitment),
     cte_designationvaiation
     AS (SELECT dv.designation_uuid,
                echo_product_key,
                echo_product_code,
                oid
         FROM   stg1.designationvariation dv
                INNER JOIN lkp_products p
                        ON dv.echo_product_code = p.cs_code),
     cte_transaction
     AS (SELECT designated_uuid,
                wva_commitmentoid,
                wva_commitmentpaymentoid,
                a.supporter_uuid                wva_payeeid,
                a1.supporter_uuid               wva_payorid,
                a2.supporter_uuid               wva_claimantid,
                'account'                       payee_type,
                'account'                       payor_type,
                'account'                       claimant_type,
                'transact'                      credit_type,
                msnfp_transactionid,
                response_trigger_id,
                Row_number()
                  OVER (
                    partition BY dct.wva_commitmentoid
                    ORDER BY wva_commitmentoid) rnk
         FROM   stg1.designated_credit_transaction dct
                INNER JOIN cte_designationvaiation dv
                        ON dv.oid = dct.myproduct
                INNER JOIN stg1.account a
                        ON a.wva_supporter_oid = dct.payeeoid
                INNER JOIN stg1.account a1
                        ON a1.wva_supporter_oid = dct.payoroid
                INNER JOIN stg1.account a2
                        ON a2.wva_supporter_oid = dct.claimantoid
         WHERE  payee_type IN ( 'Group', 'Organisation', 'Family' )
                AND payor_type IN ( 'Group', 'Organisation', 'Family' )
                AND claimant_type IN ( 'Group', 'Organisation', 'Family' )
         UNION ALL
         SELECT designated_uuid,
                wva_commitmentoid,
                wva_commitmentpaymentoid,
                a.supporter_uuid                wva_payeeid,
                a1.supporter_uuid               wva_payorid,
                a2.supporter_uuid               wva_claimantid,
                'account'                       payee_type,
                'account'                       payor_type,
                'contact'                       claimant_type,
                'transact'                      credit_type,
                msnfp_transactionid,
                response_trigger_id,
                Row_number()
                  OVER (
                    partition BY dct.wva_commitmentoid
                    ORDER BY wva_commitmentoid) rnk
         FROM   stg1.designated_credit_transaction dct
                INNER JOIN cte_designationvaiation dv
                        ON dv.oid = dct.myproduct
                INNER JOIN stg1.account a
                        ON a.wva_supporter_oid = dct.payeeoid
                INNER JOIN stg1.account a1
                        ON a1.wva_supporter_oid = dct.payoroid
                INNER JOIN stg1.contact a2
                        ON a2.wva_supporter_oid = dct.claimantoid
         WHERE  payee_type IN ( 'Group', 'Organisation', 'Family' )
                AND payor_type IN ( 'Group', 'Organisation', 'Family' )
                AND claimant_type IN ( 'Person' )
         UNION ALL
         SELECT designated_uuid,
                wva_commitmentoid,
                wva_commitmentpaymentoid,
                a.supporter_uuid                wva_payeeid,
                a1.supporter_uuid               wva_payorid,
                a2.supporter_uuid               wva_claimantid,
                'account'                       payee_type,
                'contact'                       payor_type,
                'contact'                       claimant_type,
                'transact'                      credit_type,
                msnfp_transactionid,
                response_trigger_id,
                Row_number()
                  OVER (
                    partition BY dct.wva_commitmentoid
                    ORDER BY wva_commitmentoid) rnk
         FROM   stg1.designated_credit_transaction dct
                INNER JOIN cte_designationvaiation dv
                        ON dv.oid = dct.myproduct
                INNER JOIN stg1.account a
                        ON a.wva_supporter_oid = dct.payeeoid
                INNER JOIN stg1.contact a1
                        ON a1.wva_supporter_oid = dct.payoroid
                INNER JOIN stg1.contact a2
                        ON a2.wva_supporter_oid = dct.claimantoid
         WHERE  payee_type IN ( 'Group', 'Organisation', 'Family' )
                AND payor_type IN ( 'Person' )
                AND claimant_type IN ( 'Person' )
         UNION ALL
         SELECT designated_uuid,
                wva_commitmentoid,
                wva_commitmentpaymentoid,
                a.supporter_uuid                wva_payeeid,
                a1.supporter_uuid               wva_payorid,
                a2.supporter_uuid               wva_claimantid,
                'account'                       payee_type,
                'contact'                       payor_type,
                'account'                       claimant_type,
                'transact'                      credit_type,
                msnfp_transactionid,
                response_trigger_id,
                Row_number()
                  OVER (
                    partition BY dct.wva_commitmentoid
                    ORDER BY wva_commitmentoid) rnk
         FROM   stg1.designated_credit_transaction dct
                INNER JOIN cte_designationvaiation dv
                        ON dv.oid = dct.myproduct
                INNER JOIN stg1.account a
                        ON a.wva_supporter_oid = dct.payeeoid
                INNER JOIN stg1.contact a1
                        ON a1.wva_supporter_oid = dct.payoroid
                INNER JOIN stg1.account a2
                        ON a2.wva_supporter_oid = dct.claimantoid
         WHERE  payee_type IN ( 'Group', 'Organisation', 'Family' )
                AND payor_type IN ( 'Person' )
                AND claimant_type IN ( 'Group', 'Organisation', 'Family' )
         UNION ALL
         SELECT designated_uuid,
                wva_commitmentoid,
                wva_commitmentpaymentoid,
                a.supporter_uuid                wva_payeeid,
                a1.supporter_uuid               wva_payorid,
                a2.supporter_uuid               wva_claimantid,
                'contact'                       payee_type,
                'contact'                       payor_type,
                'contact'                       claimant_type,
                'transact'                      credit_type,
                msnfp_transactionid,
                response_trigger_id,
                Row_number()
                  OVER (
                    partition BY dct.wva_commitmentoid
                    ORDER BY wva_commitmentoid) rnk
         FROM   stg1.designated_credit_transaction dct
                INNER JOIN cte_designationvaiation dv
                        ON dv.oid = dct.myproduct
                INNER JOIN stg1.contact a
                        ON a.wva_supporter_oid = dct.payeeoid
                INNER JOIN stg1.contact a1
                        ON a1.wva_supporter_oid = dct.payoroid
                INNER JOIN stg1.contact a2
                        ON a2.wva_supporter_oid = dct.claimantoid
         WHERE  payee_type IN ( 'Person' )
                AND payor_type IN ( 'Person' )
                AND claimant_type IN ( 'Person' )
         UNION ALL
         SELECT designated_uuid,
                wva_commitmentoid,
                wva_commitmentpaymentoid,
                a.supporter_uuid                wva_payeeid,
                a1.supporter_uuid               wva_payorid,
                a2.supporter_uuid               wva_claimantid,
                'contact'                       payee_type,
                'contact'                       payor_type,
                'account'                       claimant_type,
                'transact'                      credit_type,
                msnfp_transactionid,
                response_trigger_id,
                Row_number()
                  OVER (
                    partition BY dct.wva_commitmentoid
                    ORDER BY wva_commitmentoid) rnk
         FROM   stg1.designated_credit_transaction dct
                INNER JOIN cte_designationvaiation dv
                        ON dv.oid = dct.myproduct
                INNER JOIN stg1.contact a
                        ON a.wva_supporter_oid = dct.payeeoid
                INNER JOIN stg1.contact a1
                        ON a1.wva_supporter_oid = dct.payoroid
                INNER JOIN stg1.account a2
                        ON a2.wva_supporter_oid = dct.claimantoid
         WHERE  payee_type IN ( 'Person' )
                AND payor_type IN ( 'Person' )
                AND claimant_type IN ( 'Group', 'Organisation', 'Family' )
         UNION ALL
         SELECT designated_uuid,
                wva_commitmentoid,
                wva_commitmentpaymentoid,
                a.supporter_uuid                wva_payeeid,
                a1.supporter_uuid               wva_payorid,
                a2.supporter_uuid               wva_claimantid,
                'contact'                       payee_type,
                'account'                       payor_type,
                'contact'                       claimant_type,
                'transact'                      credit_type,
                msnfp_transactionid,
                response_trigger_id,
                Row_number()
                  OVER (
                    partition BY dct.wva_commitmentoid
                    ORDER BY wva_commitmentoid) rnk
         FROM   stg1.designated_credit_transaction dct
                INNER JOIN cte_designationvaiation dv
                        ON dv.oid = dct.myproduct
                INNER JOIN stg1.contact a
                        ON a.wva_supporter_oid = dct.payeeoid
                INNER JOIN stg1.account a1
                        ON a1.wva_supporter_oid = dct.payoroid
                INNER JOIN stg1.contact a2
                        ON a2.wva_supporter_oid = dct.claimantoid
         WHERE  payee_type IN ( 'Person' )
                AND payor_type IN ( 'Group', 'Organisation', 'Family' )
                AND claimant_type IN ( 'Person' )
         UNION ALL
         SELECT designated_uuid,
                wva_commitmentoid,
                wva_commitmentpaymentoid,
                a.supporter_uuid                wva_payeeid,
                a1.supporter_uuid               wva_payorid,
                a2.supporter_uuid               wva_claimantid,
                'contact'                       payee_type,
                'account'                       payor_type,
                'account'                       claimant_type,
                'transact'                      credit_type,
                msnfp_transactionid,
                response_trigger_id,
                Row_number()
                  OVER (
                    partition BY dct.wva_commitmentoid
                    ORDER BY wva_commitmentoid) rnk
         FROM   stg1.designated_credit_transaction dct
                INNER JOIN cte_designationvaiation dv
                        ON dv.oid = dct.myproduct
                INNER JOIN stg1.contact a
                        ON a.wva_supporter_oid = dct.payeeoid
                INNER JOIN stg1.account a1
                        ON a1.wva_supporter_oid = dct.payoroid
                INNER JOIN stg1.account a2
                        ON a2.wva_supporter_oid = dct.claimantoid
         WHERE  payee_type IN ( 'Person' )
                AND payor_type IN ( 'Group', 'Organisation', 'Family' )
                AND claimant_type IN ( 'Group', 'Organisation', 'Family' )),
     cte_bsbnumbers
     AS (SELECT oid,
                CASE
                  WHEN Len(bsb) > 0 THEN Substring(bsb, 1, 3) + '-' + Substring(
                                         bsb,
                                         4,
                                         6)
                  ELSE ''
                END bsb
         FROM   stg1.payment_method),
     cte_paymentmethod
     AS (SELECT pm.oid,
                pm.paymentmethodid,
                pm.type
         FROM   stg1.payment_method pm
                INNER JOIN cte_bsbnumbers cb
                        ON cb.oid = pm.oid
                INNER JOIN stg1.account a
                        ON a.wva_supporter_oid = pm.supporter
         WHERE  pm.supportertype IN ( 'Grp', 'Org', 'Fam' )
         UNION ALL
         SELECT pm.oid,
                pm.paymentmethodid,
                pm.type
         FROM   stg1.payment_method pm
                INNER JOIN cte_bsbnumbers cb
                        ON cb.oid = pm.oid
                INNER JOIN stg1.contact a
                        ON a.wva_supporter_oid = pm.supporter
         WHERE  pm.supportertype IN ( 'Ind' )),
     cte_designated_credit_transaction
     AS (SELECT response_trigger_id,
                msnfp_transactionid,
                Row_number()
                  OVER (
                    partition BY msnfp_transactionid
                    ORDER BY msnfp_transactionid ) rnk
         FROM   cte_transaction),
     cte_uniquesingle
     AS (SELECT *,
                Row_number()
                  OVER (
                    partition BY wva_financialoperation_oid
                    ORDER BY wva_financialoperation_oid) rnk
         FROM   stg1.single_donation),
     cte_singledonation
     AS (SELECT sd.*
         FROM   cte_uniquesingle sd
                INNER JOIN stg1.contact c
                        ON sd.msnfp_customerid = c.wva_supporter_oid
                INNER JOIN cte_paymentmethod pm
                        ON pm.oid = sd.msnfp_transaction_paymentmethodid
                INNER JOIN cte_designated_credit_transaction dct
                        ON dct.msnfp_transactionid =
                           sd.wva_financialoperation_oid
                           AND dct.rnk = 1
                INNER JOIN stg1.trigger_code tc
                        ON tc.trigger_oid = dct.response_trigger_id
                           AND dct.rnk = 1
                INNER JOIN stg1.appeal ap
                        ON ap.oid = tc.wva_appeal
                INNER JOIN stg1.campaign cap
                        ON cap.[campaign oid] = tc.wva_campaign
         WHERE  sd.rnk = 1
                AND sd.supportertype = 'Ind'
         UNION ALL
         SELECT sd.*
         FROM   cte_uniquesingle sd
                INNER JOIN stg1.account c
                        ON sd.msnfp_customerid = c.wva_supporter_oid
                INNER JOIN cte_paymentmethod pm
                        ON pm.oid = sd.msnfp_transaction_paymentmethodid
                INNER JOIN cte_designated_credit_transaction dct
                        ON dct.msnfp_transactionid =
                           sd.wva_financialoperation_oid
                           AND dct.rnk = 1
                INNER JOIN stg1.trigger_code tc
                        ON tc.trigger_oid = dct.response_trigger_id
                           AND dct.rnk = 1
                INNER JOIN stg1.appeal ap
                        ON ap.oid = tc.wva_appeal
                INNER JOIN stg1.campaign cap
                        ON cap.[campaign oid] = tc.wva_campaign
         WHERE  sd.rnk = 1
                AND sd.supportertype IN ( 'Grp', 'Org', 'Fam' )),
     cte_recurringdonation
     AS (SELECT recurringdonation_uuid,
                recurringdonationid,
                rd.statuscode
         FROM   stg1.recurring_donation rd
                INNER JOIN stg1.account a
                        ON rd.msnfp_customeroid = a.wva_supporter_oid
                INNER JOIN stg1.campaign ca
                        ON rd.campaign_code = ca.code
                INNER JOIN stg1.trigger_code tc
                        ON tc.trigger_oid = rd.trigger_code
                INNER JOIN stg1.appeal ap
                        ON ap.oid = tc.wva_appeal
         WHERE  rd.supportertype IN ( 'Grp', 'Org', 'Fam' )
         UNION ALL
         SELECT recurringdonation_uuid,
                recurringdonationid,
                rd.statuscode
         FROM   stg1.recurring_donation rd
                INNER JOIN stg1.contact a
                        ON rd.msnfp_customeroid = a.wva_supporter_oid
                INNER JOIN stg1.campaign ca
                        ON rd.campaign_code = ca.code
                INNER JOIN stg1.trigger_code tc
                        ON tc.trigger_oid = rd.trigger_code
                INNER JOIN stg1.appeal ap
                        ON ap.oid = tc.wva_appeal
         WHERE  rd.supportertype IN ( 'Ind' )),
     cte_child
     AS (SELECT c.child_uuid,
                c.party_key,
                c.childoid,
                c.child_pool_key
         FROM   stg1.child c
                INNER JOIN stg1.child_pool ccp
                        ON c.child_pool_key = ccp.wva_oid
                INNER JOIN stg1.child_project scp
                        ON c.project_key = scp.project_id),
     cte_childsponsorship
     AS (SELECT CASE
                  WHEN cp.wva_oid IS NULL THEN NULL
                  ELSE dcs.childsponsorship_uuid
                END childsponsorship_uuid,
                dcs.childid,
                dcs.wva_enddate
         FROM   stg1.donation_child_sponsorship dcs
                INNER JOIN stg1.account a
                        ON dcs.supporterid = a.wva_supporterid
                           AND dcs.supportertype IN ( 'Fam', 'Grp', 'Org' )
                LEFT JOIN cte_child ch
                       ON dcs.childid = ch.childoid
                LEFT JOIN stg1.child_pool cp
                       ON cp.wva_oid = ch.child_pool_key
         UNION ALL
         SELECT CASE
                  WHEN cp.wva_oid IS NULL THEN NULL
                  ELSE dcs.childsponsorship_uuid
                END childsponsorship_uuid,
                dcs.childid,
                dcs.wva_enddate
         FROM   stg1.donation_child_sponsorship dcs
                INNER JOIN stg1.contact a
                        ON dcs.supporterid = a.wva_supporterid
                           AND dcs.supportertype IN ( 'Ind' )
                LEFT JOIN cte_child ch
                       ON dcs.childid = ch.childoid
                LEFT JOIN stg1.child_pool cp
                       ON cp.wva_oid = ch.child_pool_key
         UNION ALL
         SELECT CASE
                  WHEN cp.wva_oid IS NULL THEN NULL
                  ELSE dcs.childsponsorship_uuid
                END childsponsorship_uuid,
                dcs.childid,
                dcs.wva_enddate
         FROM   stg1.donation_child_sponsorship dcs
                LEFT JOIN cte_child ch
                       ON dcs.childid = ch.childoid
                LEFT JOIN stg1.child_pool cp
                       ON cp.wva_oid = ch.child_pool_key
         WHERE  dcs.supporterid IS NULL)


		 /* Main Query */
/* Designated Credit Commitment */ 



SELECT designated_uuid           msnfp_designatedcreditid,
       CASE gst_status
         WHEN 'E - GST - Entity 2 (T2=01)' THEN 547660003
         WHEN 'F - GST - Entity 4 (T2=01)' THEN 547660003
         WHEN 'H - GST - Entity 2 (T2=11)' THEN 547660003
         WHEN 'G - GST - Entity 1 (T2=01)' THEN 547660003
         WHEN 'B - GST Free Entity 2 (T2=03)' THEN 547660001
         WHEN 'A - GST Free Entity 1 (T2 = 03)' THEN 547660000
         WHEN 'C - GST Free Entity 6 (T2=03)' THEN 547660000
         WHEN 'J - GST Free Entity 4 (T2 = 03)' THEN 547660002
         WHEN 'I - GST Free Entity 5 (T2=03)' THEN 547660002
         WHEN '99 - Supporter History Clearing' THEN NULL
       END                       wva_gststatus,
       msnfp_amount,
       NULL                      wva_claimantid,
       NULL                      claimant_type,
       NULL                      wva_payeeid,
       NULL                      payee_type,
       NULL                      wva_payorid,
       NULL                      payor_type,
       NULL                      msnfp_posteddate,
       uc.overriddencreatedon,
       CASE
         WHEN u.userid IS NOT NULL THEN u.userid
         ELSE dbo.[Fn_getdefaultuser]()
       END                       createdby,
       NULL                      wva_singledonationnewid,
       CASE
         WHEN cp1.child_pool_uuid IS NULL
               OR cproj.childprojectid IS NULL THEN NULL
         ELSE c.child_uuid
       END                       wva_childid,
       cp1.child_pool_uuid       wva_childpoolid,
       dcs.childsponsorship_uuid wva_childsponsorship,
       cproj.childprojectid      wva_projectid,
       tax.guid                  wva_taxdeductibled01,
       CASE commitmentstatus_code
         WHEN 'C' THEN 1
         WHEN 'D' THEN 4
         WHEN 'X' THEN 3
         WHEN 'E' THEN 1
       END                       statuscode,
       0                         statecode,
       O3.guid                   wva_dimension03,
       p.child_designation_name  msnfp_name,
       dv.designation_uuid       wva_designatedcreditid,
       ld.designation_uuid       wva_designationid,
       ldp.designation_uuid      msnfp_designatedcredit_designationid,
       f.guid                    wva_dimension08,
       wva_commitmentoid,
       CASE tax_statement
         WHEN 'Y' THEN 1
         ELSE 0
       END                       wva_taxstatement,
       fd1.guid                  wva_dimension06,
       fd.guid                   wva_dimension07,
       CASE acquisitionaction
         WHEN 'New' THEN 547660000
         WHEN 'Reassigned' THEN 547660001
         WHEN 'Replaced' THEN 547660002
         WHEN 'Recruited' THEN 547660003
       END                       wva_acquisitionaction,
       CASE acquisitiontype
         WHEN 'Administration' THEN 547660000
         WHEN 'New' THEN 547660001
       END                       wva_acquisitiontype,
       cam.campaign_uuid         wva_campaignid,
       e.event_uuid              wva_choseneventid,
       CASE uc.cancelreason
         WHEN 'Admin Cost - Accurate' THEN 547660000
         WHEN 'P|Admin:Change of Supp Type' THEN 547660001
         WHEN 'Admn Cost-inaccurate' THEN 547660002
         WHEN 'Auto Canc/Term' THEN 547660003
         WHEN 'A|Change in Circ - Children' THEN 547660004
         WHEN 'A|Change in Circ - Other' THEN 547660005
         WHEN 'A|Change in Circ - Sep/Div/Dth' THEN 547660006
         WHEN 'Change from Legacy' THEN 547660007
         WHEN 'Change to WPG' THEN 547660008
         WHEN 'Comms - Field' THEN 547660009
         WHEN 'Comms - WVA' THEN 547660010
         WHEN 'Cred Concern accurat' THEN 547660011
         WHEN 'Cred Concern inaccur' THEN 547660012
         WHEN 'A|Delinquent Contactable' THEN 547660013
         WHEN 'P|Delinquent Un-contactable' THEN 547660014
         WHEN 'Dissatisfied-Forced Transition' THEN 547660015
         WHEN 'Dissatisfied-Gaza' THEN 547660016
         WHEN 'Dissatisfied-No Regi' THEN 547660017
         WHEN 'Enquiry Info NR' THEN 547660018
         WHEN 'A|Field Drop Motivated' THEN 547660019
         WHEN 'A|FR - Decreased Income' THEN 547660020
         WHEN 'A|FR - Increased Expenses' THEN 547660021
         WHEN 'A|FR - Other' THEN 547660022
         WHEN 'A|FR - Retirement' THEN 547660023
         WHEN 'A|FR - Unemployment' THEN 547660024
         WHEN 'IAF -No Friend Found' THEN 547660025
         WHEN 'Increase Rate' THEN 547660026
         WHEN 'A|No Reason Given' THEN 547660027
         WHEN 'Political' THEN 547660028
         WHEN 'A|Product Change' THEN 547660029
         WHEN 'A|Program Migration' THEN 547660030
         WHEN 'A|Project Closure Motivated' THEN 547660031
         WHEN 'A|Reconsidered' THEN 547660032
         WHEN 'Relig Not Christian' THEN 547660033
         WHEN 'Relig -too Christian' THEN 547660034
         WHEN 'A|Too Much Mail' THEN 547660035
         WHEN 'A|Transfer to another charity' THEN 547660036
         WHEN 'A|Transfer Out' THEN 547660037
         WHEN 'WVA_701005' THEN 547660038
         ELSE NULL
       END                       wva_drcancelreason,
       rd.recurringdonation_uuid wva_recurringdonationid,
       CASE replacementstatus
         WHEN 'C' THEN 547660000
         WHEN 'I' THEN 547660001
         WHEN 'M' THEN 547660002
         WHEN 'N' THEN 547660003
         WHEN 'P' THEN 547660004
         WHEN 'R' THEN 547660005
         WHEN 'T' THEN 547660006
         WHEN 'U' THEN 547660007
       END                       wva_replacementstatus,
       startdateconfirmed        wva_startdateconfirmed,
       NULL                      quantity,
       NULL                      wva_commitmentpaymentoid
FROM   (SELECT *
        FROM   cte_uniquecredits
        WHERE  rnk = 1) uc
       LEFT JOIN cte_recurringdonation rd
              ON rd.recurringdonationid = uc.wva_recurringdonationid
                 AND rd.statuscode = 'Active'
       LEFT JOIN stg1.child c
              ON c.childoid = uc.child_oid
       LEFT JOIN stg1.child_pool cp1
              ON cp1.wva_oid = c.child_pool_key
       LEFT JOIN stg1.child_project cproj
              ON cproj.project_id = c.project_key
       INNER JOIN cte_designationvaiation dv
               ON dv.oid = uc.myproduct
       LEFT JOIN lkp_products p
              ON dv.echo_product_code = p.cs_code
       LEFT JOIN lkp_designations ld
              ON ld.designation_name = p.child_designation_code
                 AND ld.designation_type = 'Child'
       LEFT JOIN stg1.designation d
              ON d.designation_code = ld.designation_name
       LEFT JOIN lkp_designations ldp
              ON d.parent_designation_name = ldp.designation_name
                 AND ldp.designation_type = 'Parent'
       LEFT JOIN lkp_financedimensions O3
              ON d.new_d3_code = O3.value
                 AND O3.category = 'D03Product'
       LEFT JOIN lkp_financedimensions tax
              ON Substring(d.tax_deductable, 1, 1) = tax.value
                 AND tax.category = 'D01TaxDeductability'
       LEFT JOIN (SELECT DISTINCT wva_code,
                                  wva_campaign
                  FROM   stg1.trigger_code) tc
              ON tc.wva_code = uc.campaign
       LEFT JOIN stg1.campaign cam
              ON cam.[campaign oid] = tc.wva_campaign
       LEFT JOIN lkp_financedimensions f
              ON cam.dimension08code = f.value
                 AND f.category = 'D08Campaign'
       LEFT JOIN lkp_users u
              ON u.wva_name = uc.createdby
       LEFT JOIN stg1.event e
              ON e.chosen_event_key = uc.wva_choseneventid
       LEFT JOIN cte_childsponsorship dcs
              ON dcs.childid = uc.child_oid
                 AND dcs.wva_enddate IS NULL
       LEFT JOIN lkp_financedimensions fd
              ON fd.value = cproj.dimension07
                 AND fd.category = 'D07WVIProject'
       LEFT JOIN lkp_financedimensions fd1
              ON fd1.value = cproj.dimension06
                 AND fd1.category = 'D06WVAProject'
WHERE  uc.loaddatetime = (SELECT CONVERT(DATE, load_datetime)
                          FROM   dm_control
                          WHERE  entity_set = 'Donation'
                                 AND target_table_name =
                                     'designated_credit_commitment'
                                 AND target_schema_name = 'stg1')
UNION ALL /* Designated Credit Transaction */
SELECT uc.designated_uuid        msnfp_designatedcreditid,
       CASE d.gst_status
         WHEN 'E - GST - Entity 2 (T2=01)' THEN 547660003
         WHEN 'F - GST - Entity 4 (T2=01)' THEN 547660003
         WHEN 'H - GST - Entity 2 (T2=11)' THEN 547660003
         WHEN 'G - GST - Entity 1 (T2=01)' THEN 547660003
         WHEN 'B - GST Free Entity 2 (T2=03)' THEN 547660001
         WHEN 'A - GST Free Entity 1 (T2 = 03)' THEN 547660000
         WHEN 'C - GST Free Entity 6 (T2=03)' THEN 547660000
         WHEN 'J - GST Free Entity 4 (T2 = 03)' THEN 547660002
         WHEN 'I - GST Free Entity 5 (T2=03)' THEN 547660002
         WHEN '99 - Supporter History Clearing' THEN NULL
       END                       wva_gststatus,
       uc.msnfp_amount           msnfp_amount,
       ct.wva_claimantid,
       ct.claimant_type,
       ct.wva_payeeid,
       ct.payee_type,
       ct.wva_payorid,
       ct.payor_type,
       uc.msnfp_posteddate       msnfp_posteddate,
       uc.overriddencreatedon    overriddencreatedon,
       CASE
         WHEN u.userid IS NOT NULL THEN u.userid
         ELSE dbo.[Fn_getdefaultuser]()
       END                       createdby,
       sd.singledonation_uuid    wva_singledonationnewid,
       CASE
         WHEN cp1.child_pool_uuid IS NULL
               OR cproj.childprojectid IS NULL THEN NULL
         ELSE c.child_uuid
       END                       wva_childid,
       cp1.child_pool_uuid       wva_childpoolid,
       dcs.childsponsorship_uuid wva_childsponsorship,
       cproj.childprojectid      wva_projectid,
       tax.guid                  wva_taxdeductibled01,
       CASE uc.statuscode
         WHEN 'P' THEN 4
         WHEN 'U' THEN 5
         WHEN 'B' THEN 6
         WHEN 'C' THEN 3
       END                       statuscode,
       0                         statecode,
       O3.guid                   wva_dimension03,
       p.child_designation_name  msnfp_name,
       dv.designation_uuid       wva_designatedcreditid,
       ld.designation_uuid       wva_designationid,
       ldp.designation_uuid      msnfp_designatedcredit_designationid,
       f.guid                    wva_dimension08,
       uc.wva_commitmentoid,
       CASE tax_statement
         WHEN 'Y' THEN 1
         ELSE 0
       END                       wva_taxstatement,
       fd1.guid                  wva_dimension06,
       fd.guid                   wva_dimension07,
       NULL                      wva_acquisitionaction,
       NULL                      wva_acquisitiontype,
       NULL                      wva_campaignid,
       NULL                      wva_choseneventid,
       NULL                      wva_drcancelreason,
       NULL                      wva_recurringdonationid,
       NULL                      wva_replacementstatus,
       NULL                      wva_startdateconfirmed,
       quantity,
       uc.wva_commitmentpaymentoid
FROM   stg1.designated_credit_transaction uc
       INNER JOIN cte_designationvaiation dv
               ON dv.oid = uc.myproduct
       INNER JOIN cte_transaction ct
               ON uc.designated_uuid = ct.designated_uuid
       LEFT JOIN lkp_users u
              ON u.wva_name = uc.createdby
       LEFT JOIN cte_singledonation sd
              ON sd.wva_financialoperation_oid = uc.msnfp_transactionid
                 AND sd.rnk = 1
       LEFT JOIN stg1.child c
              ON c.childid = uc.wva_childid
       LEFT JOIN stg1.child_pool cp1
              ON cp1.wva_oid = c.child_pool_key
       LEFT JOIN stg1.child_project cproj
              ON cproj.project_id = c.project_key
       LEFT JOIN lkp_products p
              ON dv.echo_product_code = p.cs_code
       LEFT JOIN lkp_designations ld
              ON ld.designation_name = p.child_designation_code
                 AND ld.designation_type = 'Child'
       LEFT JOIN stg1.designation d
              ON d.designation_code = ld.designation_name
       LEFT JOIN lkp_designations ldp
              ON d.parent_designation_name = ldp.designation_name
                 AND ldp.designation_type = 'Parent'
       LEFT JOIN lkp_financedimensions O3
              ON d.new_d3_code = O3.value
                 AND O3.category = 'D03Product'
       LEFT JOIN lkp_financedimensions tax
              ON Substring(d.tax_deductable, 1, 1) = tax.value
                 AND tax.category = 'D01TaxDeductability'
       LEFT JOIN (SELECT DISTINCT wva_code,
                                  wva_campaign
                  FROM   stg1.trigger_code) tc
              ON tc.wva_code = uc.mycampaign
       LEFT JOIN stg1.campaign cam
              ON cam.[campaign oid] = tc.wva_campaign
       LEFT JOIN lkp_financedimensions f
              ON cam.dimension08code = f.value
                 AND f.category = 'D08Campaign'
       LEFT JOIN cte_childsponsorship dcs
              ON dcs.childid = c.childoid
                 AND dcs.wva_enddate IS NULL
       LEFT JOIN lkp_financedimensions fd
              ON fd.value = cproj.dimension07
                 AND fd.category = 'D07WVIProject'
       LEFT JOIN lkp_financedimensions fd1
              ON fd1.value = cproj.dimension06
                 AND fd1.category = 'D06WVAProject'
WHERE  uc.loaddatetime = (SELECT CONVERT(DATE, load_datetime)
                          FROM   dm_control
                          WHERE  entity_set = 'Donation'
                                 AND
              target_table_name = 'designated_credit_transaction'
                                 AND target_schema_name = 'stg1') 