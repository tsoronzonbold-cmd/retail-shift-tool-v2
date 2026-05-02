-- Mode Report Query 3: Companies List
-- Powers the partner search dropdown on the upload page.
-- Lives inside the existing report (ac9b652e687f).
-- Query token: eca03db2f4ec
--
-- Uses the same syntax pattern as business_check.sql / contact_lookup.sql:
-- bare {{ name }} (no parameters. prefix), single-quoted, with a plain SQL
-- OR-clause to handle empty input instead of a Liquid {% if %} block.

{% form %}
search:
  type: text
  default: ''
{% endform %}

SELECT id, name
FROM iw_backend_db.backend_company
WHERE name IS NOT NULL
  AND name <> ''
  AND (
    '{{ search }}' = ''
    OR LOWER(name) LIKE '%' || LOWER('{{ search }}') || '%'
  )
ORDER BY name ASC
LIMIT 1000
