SELECT
 {{ dbt_utils.star(from=ref('people')) }},
  split_part(email, '@', 2) as email_domain
FROM {{ ref('people') }}