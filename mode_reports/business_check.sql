-- Mode Report: Business Check Query
-- Creates a new report at: https://app.mode.com/instawork/reports/new
--
-- Parameters (add these in Mode's form builder):
--   company_id: text, default ''
--   business_names: text, default ''
--   addresses: text, default ''
--   store_ids: text, default ''
--
-- Input format: pipe-delimited (|||) lists
-- Example: "Price Chopper - #199|||Price Chopper - #94"

{% form %}
company_id:
  type: text
  default: ''
business_names:
  type: text
  default: ''
addresses:
  type: text
  default: ''
store_ids:
  type: text
  default: ''
{% endform %}

-- Generate numbers 1–1000
WITH numbers AS (
  SELECT ROW_NUMBER() OVER (ORDER BY 1) AS n
  FROM (
    SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
  ) t1
  CROSS JOIN (
    SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
  ) t2
  CROSS JOIN (
    SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
  ) t3
),

-- Split concatenated inputs into individual rows
input_businesses AS (
  SELECT
    n AS input_order,
    TRIM(SPLIT_PART('{{ business_names }}'::VARCHAR, '|||', n::INT)) AS search_name
  FROM numbers
  WHERE n <= REGEXP_COUNT('{{ business_names }}'::VARCHAR, '\\|\\|\\|') + 1
    AND TRIM(SPLIT_PART('{{ business_names }}'::VARCHAR, '|||', n::INT)) != ''
),

input_addresses AS (
  SELECT
    n AS input_order,
    TRIM(SPLIT_PART('{{ addresses }}'::VARCHAR, '|||', n::INT)) AS search_address
  FROM numbers
  WHERE n <= REGEXP_COUNT('{{ addresses }}'::VARCHAR, '\\|\\|\\|') + 1
    AND TRIM(SPLIT_PART('{{ addresses }}'::VARCHAR, '|||', n::INT)) != ''
),

input_store_ids AS (
  SELECT
    n AS input_order,
    TRIM(SPLIT_PART('{{ store_ids }}'::VARCHAR, '|||', n::INT)) AS search_store_id
  FROM numbers
  WHERE n <= REGEXP_COUNT('{{ store_ids }}'::VARCHAR, '\\|\\|\\|') + 1
),

combined_inputs AS (
  SELECT
    n.input_order,
    n.search_name,
    a.search_address,
    COALESCE(NULLIF(TRIM(s.search_store_id), ''), '') AS search_store_id
  FROM input_businesses n
  LEFT JOIN input_addresses a ON n.input_order = a.input_order
  LEFT JOIN input_store_ids s ON n.input_order = s.input_order
),

normalized_inputs AS (
  SELECT
    ci.*,
    TRIM(SPLIT_PART(ci.search_name, ' - #', 1)) AS brand_name,
    LOWER(REPLACE(REPLACE(REPLACE(REPLACE(ci.search_name, '-', ''), '#', ''), ' ', ''), '.', '')) AS norm_input_name,
    LOWER(REPLACE(REPLACE(REPLACE(REPLACE(ci.search_address, '-', ''), '#', ''), ' ', ''), '.', '')) AS norm_input_addr,
    LOWER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(SPLIT_PART(ci.search_name, ' - #', 1)), '-', ''), '#', ''), ' ', ''), '.', '')) AS norm_brand_name
  FROM combined_inputs ci
),

matches AS (
  SELECT
    ci.input_order,
    ci.search_name AS input_business_name,
    ci.search_address AS input_address,
    ci.search_store_id AS input_store_id,
    b.id AS business_id,
    b.name AS existing_business_name,
    b.company_id,
    b.place_id,
    p.full_address AS existing_address,
    p.regionmapping_id,
    CASE
      WHEN b.name = ci.search_name THEN 'exact_name_match'
      WHEN LOWER(b.name) = LOWER(ci.search_name) THEN 'case_insensitive_name_match'
      WHEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(b.name, '-', ''), '#', ''), ' ', ''), '.', '')) = ci.norm_input_name THEN 'normalized_name_match'
      WHEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(b.name, '-', ''), '#', ''), ' ', ''), '.', '')) LIKE ci.norm_brand_name || '%'
           AND ci.search_store_id != ''
           AND (b.name ILIKE '% ' || ci.search_store_id || ' %'
                OR b.name ILIKE '%#' || ci.search_store_id || ' %'
                OR b.name ILIKE '% ' || ci.search_store_id || '#%'
                OR b.name ILIKE '%#' || ci.search_store_id || '#%'
                OR b.name ILIKE '%- ' || ci.search_store_id || ' %'
                OR b.name ILIKE '% ' || ci.search_store_id || '-%'
                OR b.name LIKE '%#' || ci.search_store_id)
      THEN 'brand_and_store_id_match'
      WHEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(p.full_address, '-', ''), '#', ''), ' ', ''), '.', '')) = ci.norm_input_addr THEN 'address_match'
      ELSE 'no_match'
    END AS match_type,
    ROW_NUMBER() OVER (
      PARTITION BY ci.input_order
      ORDER BY
        CASE WHEN p.full_address ILIKE '%' || SPLIT_PART(ci.search_address, ',', 2) || '%' THEN 0 ELSE 1 END,
        CASE
          WHEN b.name = ci.search_name THEN 1
          WHEN LOWER(b.name) = LOWER(ci.search_name) THEN 2
          WHEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(b.name, '-', ''), '#', ''), ' ', ''), '.', '')) = ci.norm_input_name THEN 3
          WHEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(b.name, '-', ''), '#', ''), ' ', ''), '.', '')) LIKE ci.norm_brand_name || '%'
               AND ci.search_store_id != ''
               AND (b.name ILIKE '%#' || ci.search_store_id || '%'
                    OR b.name ILIKE '% ' || ci.search_store_id || ' %')
          THEN 4
          WHEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(p.full_address, '-', ''), '#', ''), ' ', ''), '.', '')) = ci.norm_input_addr THEN 5
          ELSE 6
        END
    ) AS match_rank
  FROM normalized_inputs ci
  LEFT JOIN iw_backend_db.business b
    ON b.company_id::VARCHAR = '{{ company_id }}'::VARCHAR
   AND (
        b.name = ci.search_name
     OR LOWER(b.name) = LOWER(ci.search_name)
     OR LOWER(REPLACE(REPLACE(REPLACE(REPLACE(b.name, '-', ''), '#', ''), ' ', ''), '.', '')) = ci.norm_input_name
     OR
        (LOWER(REPLACE(REPLACE(REPLACE(REPLACE(b.name, '-', ''), '#', ''), ' ', ''), '.', '')) LIKE ci.norm_brand_name || '%'
         AND ci.search_store_id != ''
         AND (b.name ILIKE '% ' || ci.search_store_id || ' %'
              OR b.name ILIKE '%#' || ci.search_store_id || ' %'
              OR b.name ILIKE '% ' || ci.search_store_id || '#%'
              OR b.name ILIKE '%#' || ci.search_store_id || '#%'
              OR b.name LIKE '%#' || ci.search_store_id))
   )
  LEFT JOIN iw_backend_db.places_place p
    ON p.id = b.place_id
)

SELECT
  ci.input_order,
  ci.search_name AS input_business_name,
  ci.search_address AS input_address,
  ci.search_store_id AS input_store_id,
  CASE WHEN m.business_id IS NOT NULL THEN 'EXISTS' ELSE 'NEW' END AS status,
  m.business_id,
  m.existing_business_name,
  m.existing_address,
  COALESCE(m.company_id::VARCHAR, '{{ company_id }}'::VARCHAR) AS company_id,
  m.place_id,
  m.regionmapping_id,
  COALESCE(m.match_type, 'no_match') AS match_type
FROM combined_inputs ci
LEFT JOIN matches m
  ON ci.input_order = m.input_order
 AND m.match_rank = 1
ORDER BY ci.input_order
