SELECT
 {{ dbt_utils.star(from=ref('people'), except=['email']) }},
  email as email_alias
FROM {{ ref('people') }}