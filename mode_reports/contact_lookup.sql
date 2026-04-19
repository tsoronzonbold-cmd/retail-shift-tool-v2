-- Mode Report: Contact Lookup
-- Creates a new report at: https://app.mode.com/instawork/reports/new
--
-- Parameters (add these in Mode's form builder):
--   company_id: text, default ''
--   business_ids: text, default ''
--   names: text, default ''
--   emails: text, default ''
--   phone_numbers: text, default ''
--
-- Input format: pipe-delimited (|||) lists

{% form %}
company_id:
  type: text
  default: ''
business_ids:
  type: text
  default: ''
names:
  type: text
  default: ''
emails:
  type: text
  default: ''
phone_numbers:
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

input_business_ids AS (
  SELECT
    n AS input_order,
    TRIM(SPLIT_PART('{{ business_ids }}'::VARCHAR, '|||', n::INT)) AS business_id
  FROM numbers
  WHERE n <= REGEXP_COUNT('{{ business_ids }}'::VARCHAR, '\\|\\|\\|') + 1
    AND TRIM(SPLIT_PART('{{ business_ids }}'::VARCHAR, '|||', n::INT)) != ''
    AND '{{ business_ids }}' != ''
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

input_emails AS (
  SELECT
    n AS input_order,
    LOWER(TRIM(SPLIT_PART('{{ emails }}'::VARCHAR, '|||', n::INT))) AS search_email
  FROM numbers
  WHERE n <= REGEXP_COUNT('{{ emails }}'::VARCHAR, '\\|\\|\\|') + 1
    AND TRIM(SPLIT_PART('{{ emails }}'::VARCHAR, '|||', n::INT)) != ''
    AND '{{ emails }}' != ''
),

input_phone_numbers AS (
  SELECT
    n AS input_order,
    REGEXP_REPLACE(TRIM(SPLIT_PART('{{ phone_numbers }}'::VARCHAR, '|||', n::INT)), '[^0-9]', '') AS search_phone
  FROM numbers
  WHERE n <= REGEXP_COUNT('{{ phone_numbers }}'::VARCHAR, '\\|\\|\\|') + 1
    AND TRIM(SPLIT_PART('{{ phone_numbers }}'::VARCHAR, '|||', n::INT)) != ''
    AND '{{ phone_numbers }}' != ''
),

contacts_via_businessemployee AS (
  SELECT DISTINCT
    up.id AS contact_id,
    COALESCE(
      NULLIF(TRIM(COALESCE(up.given_name, '') || ' ' || COALESCE(up.family_name, '')), ''),
      up.name
    ) AS contact_name,
    up.phonenum AS phone_number,
    up.email,
    be.business_id
  FROM
    iw_backend_db.backend_userprofile up
    INNER JOIN iw_backend_db.backend_companyuser cu ON cu.cuser_id = up.id
    INNER JOIN iw_backend_db.backend_businessemployee be ON be.employee_id = cu.id
  WHERE
    cu.company_id::VARCHAR = '{{ company_id }}'::VARCHAR
),

contacts_via_gig_template AS (
  SELECT DISTINCT
    up.id AS contact_id,
    COALESCE(
      NULLIF(TRIM(COALESCE(up.given_name, '') || ' ' || COALESCE(up.family_name, '')), ''),
      up.name
    ) AS contact_name,
    up.phonenum AS phone_number,
    up.email,
    gt.business_id
  FROM
    iw_backend_db.backend_userprofile up
    INNER JOIN iw_backend_db.backend_gigtemplate gt ON (
      gt.contact_id = up.id
      OR up.id IN (
        SELECT gtc.userprofile_id
        FROM iw_backend_db.backend_gigtemplate_contacts gtc
        WHERE gtc.gigtemplate_id = gt.id
      )
    )
  WHERE
    gt.company_id::VARCHAR = '{{ company_id }}'::VARCHAR
    AND gt.business_id IS NOT NULL
),

base_contacts AS (
  SELECT contact_id, contact_name, phone_number, email, business_id FROM contacts_via_businessemployee
  UNION
  SELECT contact_id, contact_name, phone_number, email, business_id FROM contacts_via_gig_template
),

gig_contact_shifts AS (
  SELECT
    bc.contact_id,
    MAX(COALESCE(s.actual_starts_at, sg.starts_at, s.created_at)) AS shift_date
  FROM
    base_contacts bc
    INNER JOIN iw_backend_db.backend_gigtemplate gt ON (
      gt.business_id = bc.business_id
      AND (
        gt.contact_id = bc.contact_id
        OR bc.contact_id IN (
          SELECT gtc.userprofile_id
          FROM iw_backend_db.backend_gigtemplate_contacts gtc
          WHERE gtc.gigtemplate_id = gt.id
        )
      )
    )
    INNER JOIN iw_backend_db.backend_shiftgroup sg ON sg.gig_id = gt.id
    INNER JOIN iw_backend_db.backend_shift s ON s.shift_group_id = sg.id
  WHERE
    gt.company_id::VARCHAR = '{{ company_id }}'::VARCHAR
  GROUP BY
    bc.contact_id
),

contact_business_agg AS (
  SELECT
    contact_id,
    LISTAGG(business_id::VARCHAR, '|||') WITHIN GROUP (ORDER BY business_id::VARCHAR) AS business_ids
  FROM
    (SELECT DISTINCT contact_id, business_id FROM base_contacts)
  GROUP BY
    contact_id
),

contact_info AS (
  SELECT DISTINCT
    contact_id,
    contact_name,
    phone_number,
    email
  FROM
    base_contacts
),

aggregated_contacts AS (
  SELECT
    cba.contact_id,
    ci.contact_name,
    ci.phone_number,
    ci.email,
    cba.business_ids
  FROM
    contact_business_agg cba
    INNER JOIN contact_info ci ON ci.contact_id = cba.contact_id
),

filtered_contacts AS (
  SELECT
    ac.contact_id,
    ac.contact_name,
    ac.phone_number,
    ac.email,
    ac.business_ids
  FROM
    aggregated_contacts ac
  WHERE
    (
      '{{ names }}' = ''
      AND '{{ emails }}' = ''
      AND '{{ phone_numbers }}' = ''
      AND '{{ business_ids }}' = ''
    )
    OR (
      (
        '{{ names }}' != ''
        AND EXISTS (
          SELECT 1
          FROM input_names in_names
          WHERE LOWER(ac.contact_name) LIKE '%' || in_names.search_name || '%'
        )
      )
      OR (
        '{{ emails }}' != ''
        AND EXISTS (
          SELECT 1
          FROM input_emails in_emails
          WHERE LOWER(ac.email) LIKE '%' || in_emails.search_email || '%'
        )
      )
      OR (
        '{{ phone_numbers }}' != ''
        AND EXISTS (
          SELECT 1
          FROM input_phone_numbers in_phones
          WHERE REGEXP_REPLACE(ac.phone_number, '[^0-9]', '') LIKE '%' || in_phones.search_phone || '%'
        )
      )
      OR (
        '{{ business_ids }}' != ''
        AND EXISTS (
          SELECT 1
          FROM input_business_ids ibi
          WHERE ac.business_ids LIKE '%' || ibi.business_id || '%'
        )
      )
    )
)

SELECT
  fc.contact_id,
  fc.contact_name,
  fc.phone_number,
  fc.email,
  fc.business_ids AS business_id,
  msd.shift_date AS most_recent_shift_date
FROM
  filtered_contacts fc
  LEFT JOIN gig_contact_shifts msd ON msd.contact_id = fc.contact_id
ORDER BY
  msd.shift_date DESC NULLS LAST,
  fc.contact_id
