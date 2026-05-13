-- Mode Report Query 5: Worker Lookup
-- Resolves requested-worker names to live worker IDs from Redshift.
-- Replaces the stale roster.json snapshot for the primary lookup path
-- (Christian's export had to be refreshed manually; this is live).
-- Lives inside the existing report (ac9b652e687f).
-- Query token: ef1f48328d23

{% form %}
company_id:
  type: text
  default: ''
names:
  type: text
  default: ''
{% endform %}

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

input_names AS (
  SELECT
    n AS input_order,
    LOWER(TRIM(SPLIT_PART('{{ names }}'::VARCHAR, '|||', n::INT))) AS search_name
  FROM numbers
  WHERE n <= REGEXP_COUNT('{{ names }}'::VARCHAR, '\\|\\|\\|') + 1
    AND TRIM(SPLIT_PART('{{ names }}'::VARCHAR, '|||', n::INT)) != ''
    AND '{{ names }}' != ''
),

candidates AS (
  SELECT
    in_names.input_order,
    in_names.search_name AS input_name,
    up.id AS worker_id,
    COALESCE(
      NULLIF(TRIM(COALESCE(up.given_name, '') || ' ' || COALESCE(up.family_name, '')), ''),
      up.name
    ) AS matched_name,
    ROW_NUMBER() OVER (
      PARTITION BY in_names.input_order
      ORDER BY up.id
    ) AS rn
  FROM input_names in_names
  INNER JOIN iw_backend_db.backend_userprofile up
    ON LOWER(TRIM(COALESCE(
         NULLIF(TRIM(COALESCE(up.given_name, '') || ' ' || COALESCE(up.family_name, '')), ''),
         up.name
       ))) = in_names.search_name
)

SELECT
  in_names.input_order,
  in_names.search_name AS input_name,
  c.worker_id,
  c.matched_name
FROM input_names in_names
LEFT JOIN candidates c ON c.input_order = in_names.input_order AND c.rn = 1
WHERE '{{ names }}' != ''
ORDER BY in_names.input_order
