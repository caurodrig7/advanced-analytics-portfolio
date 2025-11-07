/* 
--------------------------------------------------------------------------------
SQL Script: Mail Refund Check
--------------------------------------------------------------------------------
Objective:
    Identify in-store orders that have been reversed and refunded by Check,
    to support reconciliation of refunds issued vs. accounting records.

Definition:
    - Mail Refund Check:
        • Original in-store sale followed by a reversing transaction
        • Refund tender type or refund method explicitly marked as CHECK
    - Reversed Order:
        • Transaction with a negative merchandise/total amount
        • Linked to an original sale via order/receipt reference

Scope:
    - Includes only in-store transactions.
    - Excludes ecommerce-only orders and non-monetary adjustments.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

<Query>
        <DataSourceName>XCenter</DataSourceName>
        <QueryParameters>
          <QueryParameter Name="@TransactionDateFrom">
            <Value>=Parameters!TransactionDateFrom.Value</Value>
          </QueryParameter>
          <QueryParameter Name="@TransactionDateTo">
            <Value>=CDate(Parameters!TransactionDateTo.Value).AddDays(1).AddSeconds(-1)</Value>
          </QueryParameter>
        </QueryParameters>
        <CommandText>SELECT 
 a.rtl_loc_id AS StoreId, a.create_date AS TransactionTimeStamp, ABS(d.amt) AS Amount
,d.tndr_id as TenderCode, a.trans_seq AS TransactionId, a.wkstn_id AS RegisterId, b.cust_party_id AS CustomerId
,c.payable_to_name AS Name, c.payable_to_address AS Adress, COALESCE(c.payable_to_address2, c.payable_to_apt) AS Apartment
,c.payable_to_city AS City, c.payable_to_state AS State, c.payable_to_postal_code AS ZipCode
,c.payable_to_country AS Country
--,(  SELECT TOP 1 sA.telephone_number 
--    FROM crm_party_telephone sA 
--    WHERE sA.party_id = b.cust_party_id 
--		AND sA.telephone_number IS NOT NULL
--		AND sA.contact_flag = 1
--  ) AS Phone
FROM trn_trans a
INNER JOIN trl_rtrans b ON a.organization_id = b.organization_id
    AND a.rtl_loc_id = b.rtl_loc_id
    AND a.business_date = b.business_date
    AND a.wkstn_id = b.wkstn_id
    AND a.trans_seq = b.trans_seq
INNER JOIN ttr_send_check_tndr_lineitm c ON a.organization_id = c.organization_id
    AND a.rtl_loc_id = c.rtl_loc_id
    AND a.business_date = c.business_date
    AND a.wkstn_id = c.wkstn_id
    AND a.trans_seq = c.trans_seq
INNER JOIN ttr_tndr_lineitm d on c.organization_id = d.organization_id
    AND c.rtl_loc_id = d.rtl_loc_id
    AND c.business_date = d.business_date
    AND c.wkstn_id = d.wkstn_id
    AND c.trans_seq = d.trans_seq
    AND c.rtrans_lineitm_seq = d.rtrans_lineitm_seq
INNER JOIN trl_rtrans_lineitm e ON c.organization_id = e.organization_id
    AND c.rtl_loc_id = e.rtl_loc_id
    AND c.business_date = e.business_date
    AND c.wkstn_id = e.wkstn_id
    AND c.trans_seq = e.trans_seq
    AND c.rtrans_lineitm_seq = e.rtrans_lineitm_seq
WHERE a.trans_statcode = 'COMPLETE'
AND a.post_void_flag = 0
AND d.tndr_id = 'HOME_OFFICE_CHECK'
AND e.void_flag = 0
AND a.create_date BETWEEN @TransactionDateFrom AND @TransactionDateTo
AND a.create_user_id &lt;&gt; 'DATALOADER' AND b.create_user_id &lt;&gt; 'DATALOADER' AND c.create_user_id &lt;&gt; 'DATALOADER' AND d.create_user_id &lt;&gt; 'DATALOADER'
AND e.create_user_id &lt;&gt; 'DATALOADER'
ORDER BY StoreId, TransactionTimeStamp</CommandText>
      </Query>