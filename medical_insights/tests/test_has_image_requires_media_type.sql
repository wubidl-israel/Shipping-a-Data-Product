-- Fails if has_image = true but media_type is null
-- Returns rows that violate the rule: has_image = true → media_type must not be null

SELECT *
FROM {{ ref('fct_messages') }}
WHERE has_image = true AND media_type IS NULL

