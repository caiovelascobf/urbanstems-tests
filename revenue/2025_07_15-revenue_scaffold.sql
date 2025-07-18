-- LINEAGE - TRACKING AN ORDER

-- SUMMARY -------------------------------------------------------------------------------------------------------------------------------------------------------
-- 1) HEVO OMS SUBORDERS -> dynamodb_test_schema.hevo_oms_suborders -> STG_SUBORDERS
-- -- suborder-level data ingested from the OMS source, including delivery details, recipient information, and identifiers (order_id, suborder_id)

-- 2) HEVO OMS ORDERS -> dynamodb_test_schema.hevo_oms_orders -> STG_ORDERS
-- -- order-level data ingested from the OMS source, including: subscription data and core identifiers (order_id, user_id)

-- 3) HEVO OMS TRANSACTIONS -> dynamodb_test_schema.hevo_oms_prod_transactions -> STG_TRANSACTIONS
-- -- transaction-level data ingested from the OMS source, including: payment data and core identifiers (order_id, tran_id)

-- 4) SHOPIFY TRANSACTIONS -> dynamodb_test_schema.shopify_order_transactions + STG_TRANSACTIONS -> STG_SHOP_TRANSACTIONS
-- -- transaction-level data ingested from both OMS and SHOPIFY transactions via UNION ALL: payment data and core identifiers (order_id, tran_id)

-- 5) STG_TRANSACTIONS -> OMS_REFUNDS
-- -- refunded-order-level data — one row per refunded order.
-- -- Extracts total refund amount, timestamp, and refund reasons for each order from raw transaction-level data (hevo_oms_prod_transactions).
-- -- Although tran_id is used to deduplicate transactions earlier in the pipeline, the aggregations (SUM, MIN, LISTAGG) are all done by order_id.

-- 6) HEVO OMS LINE ITEMS -> dynamodb_test_schema.hevo_oms_prod_lineitems -> STG_LINE_ITEMS
-- -- item-level detail within each suborder (SKU, name, quantity, and unit pricing)

-- 7) HEVO OMS LINE ITEMS -> dynamodb_test_schema.hevo_oms_prod_salelineitems -> STG_SPLIT_LINE_ITEMS
-- -- 


-- END OF SUMMARY -------------------------------------------------------------------------------------------------------------------------------------------------------


-- 1) SOURCES (HEVO OMS SUBORDERS -> dynamodb_test_schema.hevo_oms_suborders -> STG_SUBORDERS)
-- Details: Address (recipient's info), Delivery Info (start time, city, ...)
SELECT
    'dynamodb_test_schema.hevo_oms_suborders' AS source_table,
    pk, split_part(pk, '#', 2) AS order_id, sk_gsi1pk_gsi2sk, split_part(sk_gsi1pk_gsi2sk, '#', 2) AS suborder_id,
    gsi1sk_gsi4sk_gsi5sk, RIGHT(gsi1sk_gsi4sk_gsi5sk, LEN(gsi1sk_gsi4sk_gsi5sk) - regexp_instr(gsi1sk_gsi4sk_gsi5sk, '#')) AS ze_delivery_status,
    delivery_starttime, delivery_orderfulfilledby,
    __hevo__ingested_at, __hevo__marked_deleted 
FROM dynamodb_test_schema.hevo_oms_suborders
WHERE pk = 'ORDER#100010509';

    -- Questions
    -- recipient_email, recipient_address1, recipient_firstname, ... -- YES
    -- delivery_starttime, delivery_transport, delivery_fc_city, delivery_transportvendordeliveryname, delivery_transportvendordelivery_onfleettaskid -- YES
    -- lineitems,lineitems_0_price, lineitems_0_unitprice, lineitems_0_quantity, lineitems_0_sku, ... -- ALL NULL 
    -- customer_customerid, customer_firstname, customer_lastname, customer_email -- ALL NULL
    -- orderclassificationinfo_subscription_subscriptionnumber, ... -- ALL NULL

-- DEDUPLICATION LAST ORDER-SUBORDER = SUBORDERS -> STG_SUBORDERS
WITH
raw_suborders AS (
    SELECT
        'dynamodb_test_schema.hevo_oms_suborders' AS source_table,
        pk, split_part(pk, '#', 2) AS order_id, sk_gsi1pk_gsi2sk, split_part(sk_gsi1pk_gsi2sk, '#', 2) AS suborder_id,
        gsi1sk_gsi4sk_gsi5sk, RIGHT(gsi1sk_gsi4sk_gsi5sk, LEN(gsi1sk_gsi4sk_gsi5sk) - regexp_instr(gsi1sk_gsi4sk_gsi5sk, '#')) AS ze_delivery_status,
        __hevo__ingested_at, __hevo__marked_deleted,
        modifiedat,
        -- Indicates whether the suborder record was marked as deleted by the ingestion process. This ensures that if any version of a given suborder (sk_gsi1pk_gsi2sk) was marked as deleted, the flag will be 1 (true).
        MAX(__hevo__marked_deleted::INT) OVER (PARTITION BY sk_gsi1pk_gsi2sk) AS is_deleted,
        -- Flags if the suborder was ever marked as cancelled based on the delivery status. Extracts the last segment of the delivery status (gsi1sk_gsi4sk_gsi5sk) and checks if it equals 'Cancelled'. If it was ever cancelled, the value will be 1
        MAX((RIGHT(gsi1sk_gsi4sk_gsi5sk, 9) = 'Cancelled')::INT) OVER (PARTITION BY sk_gsi1pk_gsi2sk) AS is_cancelled,
        -- The timestamp of when the suborder reached "Delivered" status. It filters for records where the delivery status is "Delivered" and captures the earliest modifiedat timestamp for that transition.
        MIN(CASE WHEN split_part(gsi1sk_gsi4sk_gsi5sk, '#', 2) = 'Delivered' THEN modifiedat END) OVER (PARTITION BY sk_gsi1pk_gsi2sk) AS delivered_at,
        -- The original delivery date requested by the customer. Grabs the first known value for the delivery start date from the ingestion history for each suborder.
        FIRST_VALUE(delivery_starttime::DATE) OVER (PARTITION BY sk_gsi1pk_gsi2sk ORDER BY __hevo__ingested_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS delivery_date_requested,
        -- The first recorded prep/fulfillment date of the suborder. Gets the initial fulfillment responsibility (delivery_orderfulfilledby) for the suborder across all ingested versions
        FIRST_VALUE(delivery_orderfulfilledby) OVER (PARTITION BY sk_gsi1pk_gsi2sk ORDER BY __hevo__ingested_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS original_prep_date,
        -- Identifies the most recent version of each suborder record (sk_gsi1pk_gsi2sk) based on ingestion timestamp (__hevo__ingested_at) and ordered by the most recent one
        ROW_NUMBER() OVER (
            PARTITION BY sk_gsi1pk_gsi2sk
            ORDER BY __hevo__ingested_at DESC
        ) AS last_record
    FROM dynamodb_test_schema.hevo_oms_suborders
    WHERE pk = 'ORDER#100010509'
    ORDER BY suborder_id ASC
),
filtered_raw_suborders AS (
    SELECT *
    FROM raw_suborders
    WHERE NOT COALESCE(is_deleted, 0)
    AND COALESCE(last_record, 1) = 1
) SELECT * FROM filtered_raw_suborders;


-- 2) SOURCES (HEVO OMS ORDERS -> dynamodb_test_schema.hevo_oms_orders -> STG_ORDERS)
-- Details: Order-level, User ID information, Subscription Information
SELECT  
    'dynamodb_test_schema.hevo_oms_orders' AS source_table,
    pk, right(pk, len(pk) - 6) AS order_id, sk_gsi1pk_gsi2sk,
    gsi4pk AS user_id,
    gsi1sk_gsi4sk_gsi5sk, RIGHT(gsi1sk_gsi4sk_gsi5sk, LEN(gsi1sk_gsi4sk_gsi5sk) - regexp_instr(gsi1sk_gsi4sk_gsi5sk, '#')) AS ze_delivery_status,
    meta_flexibleshipdate AS is_flexible_ship_date,
    meta_flexibleshipdateamount AS flexible_ship_date_amount,
    subscription_source, subscription_subscriptionnumber, subscription_initialsubscriptionordernumber,
    case subscription_source
      when 'loop' then subscription_subscriptionnumber
      when 'sticky' then subscription_initialsubscriptionordernumber
      when 'aggregate' then subscription_subscriptionnumber
    end as subscription_id,
    max(__hevo__marked_deleted::int) over (partition by pk) as is_deleted,
    row_number() over (partition by sk_gsi1pk_gsi2sk order by modifiedat DESC) as last_record,
    __hevo__ingested_at, __hevo__marked_deleted 
FROM dynamodb_test_schema.hevo_oms_orders
WHERE pk = 'ORDER#100010509';

    -- Questions
    -- Is this dynamodb_test_schema.hevo_oms_orders important only because is brings subscription data and user id (when compared to stg_suborders)?
    -- meta_flexibleshipdateamount,  ... -- NULL or 5 (what is this and why there are only 2 values?) 
    -- subscription_source -- has 4 values (NULL, loop, sticky, aggregate) -- what are these? how can I know what they mean? why was it renamed in the way it was in the CASE statement?
    -- sk_gsi1pk_gsi2sk -- Why are you partitioning row number by sk_gsi1pk_gsi2sk (INFO#ORDER#100010509) and not by pk (ORDER#100010509) as in __hevo__marked_deleted?

-- DEDUPLICATION LAST ORDER -> STG_ORDERS
WITH raw_orders AS (
  SELECT  
      'dynamodb_test_schema.hevo_oms_orders' AS source_table,
      pk, right(pk, len(pk) - 6) AS order_id, sk_gsi1pk_gsi2sk,
      gsi4pk AS user_id,
      gsi1sk_gsi4sk_gsi5sk,
      RIGHT(gsi1sk_gsi4sk_gsi5sk, LEN(gsi1sk_gsi4sk_gsi5sk) - regexp_instr(gsi1sk_gsi4sk_gsi5sk, '#')) AS ze_delivery_status,
      meta_flexibleshipdate AS is_flexible_ship_date,
      meta_flexibleshipdateamount AS flexible_ship_date_amount,
      subscription_source, subscription_subscriptionnumber, subscription_initialsubscriptionordernumber,
      CASE subscription_source
        WHEN 'loop' THEN subscription_subscriptionnumber
        WHEN 'sticky' THEN subscription_initialsubscriptionordernumber
        WHEN 'aggregate' THEN subscription_subscriptionnumber
      END AS subscription_id,
      -- Flags whether the order has ever been marked as deleted. If any version of this order (same `pk`) was marked deleted, it returns 1; else 0
      MAX(__hevo__marked_deleted::INT) OVER (PARTITION BY pk) AS is_deleted,
      -- Identifies the most recent version of a given order (based on `sk_gsi1pk_gsi2sk`). Assigns 1 to the latest version (most recent `modifiedat`) and higher numbers to older versions.
      ---- Used to deduplicate and retain only the most up-to-date record.
      ROW_NUMBER() OVER (PARTITION BY sk_gsi1pk_gsi2sk ORDER BY modifiedat DESC) AS last_record,
      __hevo__ingested_at, __hevo__marked_deleted 
  FROM dynamodb_test_schema.hevo_oms_orders
  WHERE pk = 'ORDER#100010509'
),
filtered_raw_orders AS (
    SELECT *
    FROM raw_orders
    WHERE NOT COALESCE(is_deleted, 0)
    AND COALESCE(last_record, 1) = 1
) SELECT * FROM filtered_raw_orders;


-- 3) SOURCES (HEVO OMS TRANSACTIONS -> dynamodb_test_schema.hevo_oms_prod_transactions -> STG_TRANSACTIONS)
-- Details: 
select distinct paymenttype from dynamodb_test_schema.hevo_oms_prod_transactions limit 10
where __hevo_ref_id = 'ORDER#101104710:INFO#ORDER#101104710';

SELECT  
    *,
    'dynamodb_test_schema.hevo_oms_prod_transactions' AS source_table,
    __hevo_ref_id, 
    -- split_part(split_part(__hevo_ref_id, ':', 1), '#', 2) AS NEW_order_id,
    right(
        left(__hevo_ref_id, POSITION(':INFO' in replace(__hevo_ref_id, ' ', '')) -1),
        len(left(__hevo_ref_id, POSITION(':INFO' in replace(__hevo_ref_id, ' ', '')))) - 7
    ) as order_id,
    order_id || '___' || __hevo_array_index::text as tran_id,
    amount, -- THIS IS NOT PRESENT IN DBT
    brand as payment_brand,
    transactionid as payment_id,
    paymenttype as payment_type,
    case paymenttype
        when 'credit_card' then 'paypal'
        when 'credits' then null
        else paymenttype
        end as payment_processor,
    MAX(__hevo__marked_deleted::INT) OVER (PARTITION BY tran_id) AS NEW_is_deleted,  -- THIS IS NEW
    row_number() over (partition by tran_id order by __hevo__ingested_at DESC) as last_record,
    __hevo__ingested_at, __hevo__marked_deleted 
FROM dynamodb_test_schema.hevo_oms_prod_transactions
-- WHERE order_id = '100010509'
WHERE order_id = '101104710';

    -- Questions
    -- Is this source important because it brings information on payment transactions? Is there any documentation on this source?
    -- I proposed a new order_id logic, which is more robust. However, I need to know what part of this string is the important one? The order_id after INFO# or after ORDER# (e.g.: ORDER#100010509:INFO#ORDER#100010509)?
    -- why amount is not present in the model? (particularly because it laters UNION ALL with shopify orders)
	-- why paymenttype = credtit_card only brings paypal as payment processor? Is this the only processor urbanstems uses? 
    -- why "__hevo__marked_deleted / is_deleted" is not being considered along with the "last_record" deduplication method (as in the others models stg_orders and stg_suborders)?
    ---- check: order_id = '101104710'

-- DEDUPLICATION LAST TRANSACTION -> STG_TRANSACTIONS
WITH raw_transactions AS (
  SELECT  
    *,
    'dynamodb_test_schema.hevo_oms_prod_transactions' AS source_table,
    __hevo_ref_id, 
    -- split_part(split_part(__hevo_ref_id, ':', 1), '#', 2) AS NEW_order_id,
    right(
        left(__hevo_ref_id, POSITION(':INFO' in replace(__hevo_ref_id, ' ', '')) -1),
        len(left(__hevo_ref_id, POSITION(':INFO' in replace(__hevo_ref_id, ' ', '')))) - 7
    ) as order_id,
    order_id || '___' || __hevo_array_index::text as tran_id,
    amount, -- THIS IS NOT PRESENT IN DBT 
    brand as payment_brand,
    transactionid as payment_id,
    paymenttype as payment_type,
    case paymenttype
        when 'credit_card' then 'paypal'
        when 'credits' then null
        else paymenttype
    end as payment_processor,
    -- MAX(__hevo__marked_deleted::INT) OVER (PARTITION BY tran_id) AS NEW_is_deleted,  -- THIS IS NEW
    row_number() over (partition by tran_id order by __hevo__ingested_at DESC) as last_record,
    __hevo__ingested_at, 
    __hevo__marked_deleted 
FROM dynamodb_test_schema.hevo_oms_prod_transactions
-- WHERE order_id = '100010509'
WHERE order_id = '101104710'
),
filtered_raw_transactions AS (
    SELECT *
    FROM raw_transactions
    -- WHERE NOT COALESCE(NEW_is_deleted, 0)  -- WHY IS THIS NOT PRESENT IN THE DBT MODEL?
    WHERE COALESCE(last_record, 1) = 1
) SELECT * FROM filtered_raw_transactions;


-- 4)  SOURCES (SHOPIFY TRANSACTIONS -> dynamodb_test_schema.shopify_order_transactions + STG_TRANSACTIONS -> STG_SHOP_TRANSACTIONS)
-- Details: Bring orders from both OMS and SHOPIFY
SELECT distinct kind FROM dynamodb_test_schema.shopify_order_transactions

SELECT  
    'dynamodb_test_schema.shopify_order_transactions' AS source_table,
    id::text AS tran_id,
    order_id::text,
    status, -- distinct values: failure, success, error
    amount,
    gateway AS payment_processor,
    kind AS transactiontype, -- distinct values: sale, void, refund, authorization, capture 
    processed_at AS transactiondate,
    replace((receipt.charges.data[0].payment_method_details.card.brand)::text,'"','') AS payment_brand,
    "authorization" AS payment_id,
    replace((receipt.charges.data[0].payment_method_details.type)::text,'"','') AS payment_type,
    __hevo__ingested_at,
    __hevo__loaded_at,
    __hevo__marked_deleted 
FROM dynamodb_test_schema.shopify_order_transactions
WHERE order_id = '6006590210296';

    -- Questions
    -- Why status is not present?
    -- Why transactiontype and transactiondate are not snake case, like the rest of the columns?
	-- Why "kind" and "authorization" columns are within double quotes?
	-- Why you're not including "authorization" in the kind filter?

-- DEDUPLICATION LAST TRANSACTION -> STG_SHOP_TRANSACTIONS
WITH raw_shopify_transactions AS (
  SELECT  
    'shopify_order_transactions' AS source_table,
    order_id::text,
    id::text AS tran_id,
    amount,
    gateway AS payment_processor,
    kind AS transactiontype,
    processed_at AS transactiondate,
    replace((receipt.charges.data[0].payment_method_details.card.brand)::text, '"', '') AS payment_brand,
    "authorization" AS payment_id,
    replace((receipt.charges.data[0].payment_method_details.type)::text, '"', '') AS payment_type,
    __hevo__ingested_at,
    __hevo__loaded_at,
    __hevo__marked_deleted,
    row_number() OVER (
      PARTITION BY tran_id 
      ORDER BY __hevo__ingested_at DESC
    ) AS last_record
  FROM dynamodb_test_schema.shopify_order_transactions
  WHERE kind IN ('capture', 'refund', 'void', 'sale')
    -- AND order_id = '6006590210296'
    AND order_id = '5996555862264' -- USING THIS BECAUSE THE DBT MODEL FILTERS ONLY FOR THIS ONE
),
filtered_shopify AS (
  SELECT *
  FROM raw_shopify_transactions
  WHERE last_record = 1
), -- select * from filtered_shopify;

raw_oms_transactions AS (
  SELECT
    'dynamodb_test_schema.hevo_oms_prod_transactions' AS source_table,
    right(
        left(__hevo_ref_id, POSITION(':INFO' IN replace(__hevo_ref_id, ' ', '')) -1),
        len(left(__hevo_ref_id, POSITION(':INFO' IN replace(__hevo_ref_id, ' ', '')))) - 7
    ) AS order_id,
    order_id || '___' || __hevo_array_index::text AS tran_id,  -- NEEDS TO COME AFTER ORDER_ID SO THAT THE WHERE CLAUSE WORKS
    amount,
    CASE paymenttype
        WHEN 'credit_card' THEN 'paypal'
        WHEN 'credits' THEN NULL
        ELSE paymenttype
    END AS payment_processor,
    NULL AS transactiontype,
    NULL AS transactiondate,
    brand as payment_brand,
    transactionid as payment_id,
    paymenttype as payment_type,
    __hevo__ingested_at,
    __hevo__loaded_at,
    __hevo__marked_deleted,
    -- MAX(__hevo__marked_deleted::INT) OVER (PARTITION BY tran_id) AS NEW_is_deleted,  -- THIS IS NEW
    row_number() OVER (PARTITION BY tran_id order by __hevo__ingested_at DESC) as last_record
  FROM dynamodb_test_schema.hevo_oms_prod_transactions
  WHERE order_id = '101104710'
),
filtered_oms AS (
  SELECT *
  FROM raw_oms_transactions
  WHERE last_record = 1
) -- select * from filtered_oms;
SELECT * FROM filtered_shopify
UNION ALL
SELECT * FROM filtered_oms;

-- 5) STG_TRANSACTIONS -> OMS_REFUNDS
WITH raw_transactions AS (
  SELECT  
    *,
    'dynamodb_test_schema.hevo_oms_prod_transactions' AS source_table,
    __hevo_ref_id, 
    -- split_part(split_part(__hevo_ref_id, ':', 1), '#', 2) AS NEW_order_id,
    right(
        left(__hevo_ref_id, POSITION(':INFO' in replace(__hevo_ref_id, ' ', '')) -1),
        len(left(__hevo_ref_id, POSITION(':INFO' in replace(__hevo_ref_id, ' ', '')))) - 7
    ) as order_id,
    order_id || '___' || __hevo_array_index::text as tran_id,
    amount  AS amount_raw, -- THIS IS NOT PRESENT IN DBT 
    brand as payment_brand,
    transactionid as payment_id,
    paymenttype as payment_type,
    case paymenttype
        when 'credit_card' then 'paypal'
        when 'credits' then null
        else paymenttype
    end as payment_processor,
    -- MAX(__hevo__marked_deleted::INT) OVER (PARTITION BY tran_id) AS NEW_is_deleted,  -- THIS IS NEW
    row_number() over (partition by tran_id order by __hevo__ingested_at DESC) as last_record,
    __hevo__ingested_at, 
    __hevo__marked_deleted 
FROM dynamodb_test_schema.hevo_oms_prod_transactions
-- WHERE order_id = '100010509'
-- WHERE order_id = '101104710'
WHERE order_id = '100835570' or order_id = '100847523' -- adding two orders with refunds so that aggreagation work later on
),
filtered_raw_transactions AS (
    SELECT *
    FROM raw_transactions
    -- WHERE NOT COALESCE(NEW_is_deleted, 0)  -- WHY IS THIS NOT PRESENT IN THE DBT MODEL?
    WHERE COALESCE(last_record, 1) = 1
), -- SELECT * FROM filtered_raw_transactions;

refund_base AS (
	SELECT *
	FROM filtered_raw_transactions
	WHERE transactiontype = 'refund'
	AND paymenttype != 'credits'
), -- SELECT * FROM refund_base;

refund_transactions AS (
    SELECT
        order_id,
        SUM(amount_raw) AS refunds,
        MIN(transactiondate) AS refund_tstamp
    FROM refund_base
    GROUP BY order_id
), -- SELECT * FROM refund_transactions;

refund_reason AS (
    SELECT
        order_id,
        LISTAGG(reason, ', ') WITHIN GROUP (ORDER BY reason) AS refund_reason
    FROM (
        SELECT DISTINCT order_id, reason
        FROM refund_base
    ) AS distinct_reasons
    GROUP BY order_id
) -- SELECT * FROM refund_reason;

SELECT
    a.order_id,
    r.refund_reason,
    a.refunds,
    a.refund_tstamp
FROM refund_transactions a
JOIN refund_reason r ON a.order_id = r.order_id;

select * from analytics.stg_transactions WHERE transactiontype = 'refund' limit 10


-- ) SOURCES (PROD: HEVO OMS LINE ITEMS)
-- dynamodb_test_schema.hevo_oms_prod_lineitems
-- Details: adding Product Info (sku, name) and Pricing (quantity, price, ...)
SELECT 'dynamodb_test_schema.hevo_oms_prod_lineitems' AS source_table, 
    __hevo_ref_id, split_part(split_part(__hevo_ref_id, ':', 1), '#', 2) AS order_id, split_part(split_part(__hevo_ref_id, ':', 2), '#', 2) AS suborder_id,
    sku, name, 
    quantity, price, originalprice, unitprice, 
    __hevo__ingested_at
FROM dynamodb_test_schema.hevo_oms_prod_lineitems
WHERE __hevo_ref_id LIKE '%100010509%';

    -- Questions
    -- __hevo_ref_id  -- YES
    -- sku, name, isprimaryproduct  -- YES
    -- quantity, price, originalprice, unitprice -- YES
    -- taxamount, taxrate -- ALL NULL
    -- discountamount -- ALL NULL
    -- inventorystatus -- ALL NULL
    -- isrefunded -- ALL NULL
    -- netsuite fulfillments -- ALL NULLS

-- DEDUPLICATION: LAST LINE ITEMS PER ORDER-SUBORDER = SKUs
WITH
raw_lineitems AS (
    SELECT 'dynamodb_test_schema.hevo_oms_prod_lineitems' AS source_table, 
        __hevo_ref_id, split_part(split_part(__hevo_ref_id, ':', 1), '#', 2) AS order_id, split_part(split_part(__hevo_ref_id, ':', 2), '#', 2) AS suborder_id,
        sku, name, 
        quantity, price, originalprice, unitprice, 
        __hevo__ingested_at,
        ROW_NUMBER() OVER (
        PARTITION BY __hevo_ref_id, sku
        ORDER BY __hevo__ingested_at DESC
        ) AS row_num
    FROM dynamodb_test_schema.hevo_oms_prod_lineitems
    WHERE __hevo_ref_id LIKE '%100010509%'
),
filtered_lineitems AS (
    SELECT * FROM raw_lineitems
    WHERE row_num = 1
) SELECT * FROM filtered_lineitems;

-- SO FAR:
-- 1) dynamodb_test_schema.hevo_oms_suborders: suborder-level metadata ingested from the OMS source, including delivery details, recipient information, and identifiers (order_id, suborder_id). It defines the overall structure of an order’s fulfillment.
-- 2) dynamodb_test_schema.hevo_oms_prod_lineitems: item-level detail within each suborder (SKU, name, quantity, and unit pricing).
-- Together, they define delivery details, pricing, and order/suborder/sku-tracing details

-- 3) MODELING ()



SELECT
    lineitem_id, suborder_id, order_number, order_status, product_sku, adjusted_quantity, add_on, is_lineitem_strikethrough
FROM analytics.tableau_items_xf
where order_status = 'complete'
order by suborder_id
limit 100;

SELECT
    lineitem_id, suborder_id, order_number, order_status, product_sku, adjusted_quantity, add_on, is_lineitem_strikethrough
FROM analytics.tableau_items_xf
WHERE lineitem_id LIKE '%100010509%';

SELECT
    id, suborder_id, order_id, product_sku, product_name, quantity, add_on, is_lineitem_strikethrough
FROM analytics.oms_items_xf
WHERE suborder_id = '100010509-1';

SELECT __hevo_ref_id, sku, name, quantity, __hevo__ingested_at, originalprice, unitprice 
FROM dynamodb_test_schema.hevo_oms_prod_lineitems
WHERE __hevo_ref_id LIKE '%100010509%';

SELECT
    id, suborder_id, order_number, order_status, quantity, item_count, item_total, subscription_id, product_id, main_product_ids, add_on_product_ids
FROM analytics.oms_suborder_calculations
WHERE suborder_id = '100008985-1';

SELECT id, order_number, order_status, quantity, item_count, item_total, subscription_id, product_id
FROM analytics.oms_suborders_joined
WHERE id = '100008985-1';

-- oms_suborders
SELECT
  id, order_number, order_status, item_total, subscription_id
FROM analytics.oms_suborders
WHERE id = '100008985-1';

--stg_suborders
SELECT order_id, suborder_id, shopify_id, pk, sk_gsi1pk_gsi2sk
FROM analytics.stg_suborders
WHERE pk = 'ORDER#100008985';



-- stg_orders
SELECT order_id, pk, sk_gsi1pk_gsi2sk
FROM analytics.stg_orders
WHERE pk = 'ORDER#100008985';


-- dynamodb_test_schema.hevo_oms_orders
SELECT pk, sk_gsi1pk_gsi2sk, __hevo__ingested_at, __hevo__loaded_at, __hevo__marked_deleted 
FROM dynamodb_test_schema.hevo_oms_orders
WHERE pk = 'ORDER#100008985';

-- Tracing an order from up to downstream
-- Final order trace query for suborder_id '100008985-1' and order_id '100008985'
-- This query joins across key layers from raw ingestion to final reporting