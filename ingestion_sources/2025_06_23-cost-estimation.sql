-- Cost Estimation

-- 1) Stitch
-- amazon_s3_product_catalog.sfcc_product_feed
SELECT
  MIN(_sdc_batched_at) AS first_ingestion,
  MAX(_sdc_batched_at) AS last_ingestion
FROM amazon_s3_product_catalog.sfcc_product_feed;

-- We turned off the pipeline on June 11, 2025, but the last _sdc_batched_at value is from November 14, 2024
-- so there is no input for the cost estimation from this source

SELECT
  DATE_TRUNC('day', _sdc_batched_at) AS load_day,
  COUNT(*) AS rows_loaded
FROM amazon_s3_product_catalog.sfcc_product_feed
WHERE _sdc_batched_at >= DATEADD(day, -30, CURRENT_DATE)  -- or use your actual date range
  AND _sdc_batched_at < DATEADD(day, -12, CURRENT_DATE)   -- to cut off at shutdown
GROUP BY 1
ORDER BY 1;



