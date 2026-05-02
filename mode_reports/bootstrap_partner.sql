-- Mode Report Query 4: Bootstrap Partner
-- Returns company name + most-recent gigtemplate defaults for one company_id.
-- Replaces the two raw redshift_client.execute_query() calls used during
-- auto-bootstrap (app.py /upload) and the /bootstrap-partner route.
-- Lives inside the existing report (ac9b652e687f).

{% form %}
company_id:
  type: text
  default: '0'
{% endform %}

SELECT
    c.id          AS company_id,
    c.name        AS company_name,
    t.contact_id,
    t.created_by_id,
    t.position_fk_id,
    t.position_tiering_id,
    t.has_parking,
    t.instructions,
    t.custom_attire_requirements
FROM iw_backend_db.backend_company c
LEFT JOIN (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY created_at DESC) AS rn
    FROM iw_backend_db.backend_gigtemplate
    WHERE company_id = {{ parameters.company_id }}::int
      AND (contact_id IS NOT NULL OR instructions IS NOT NULL)
) t ON t.company_id = c.id AND t.rn = 1
WHERE c.id = {{ parameters.company_id }}::int
LIMIT 1
