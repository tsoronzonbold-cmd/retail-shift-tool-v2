-- Mode Report Query 3: Companies List
-- Powers the partner search dropdown on the upload page.
-- Replaces redshift_client.get_companies().
-- Lives inside the existing report (ac9b652e687f).

{% form %}
search:
  type: text
  default: ''
{% endform %}

SELECT
    id,
    name
FROM iw_backend_db.backend_company
WHERE deleted_at IS NULL
  AND name IS NOT NULL
  AND name <> ''
  {% if parameters.search %}
  AND LOWER(name) LIKE '%' || LOWER({{ parameters.search }}) || '%'
  {% endif %}
ORDER BY name ASC
LIMIT 1000
