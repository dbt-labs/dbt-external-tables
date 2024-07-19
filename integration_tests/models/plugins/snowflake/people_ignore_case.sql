SELECT
 {{ dbt_utils.star(from=ref('people'), except=['email']) }},
  email as Email
FROM {{ ref('people') }}