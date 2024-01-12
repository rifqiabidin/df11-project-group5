{{ config(
    materialized='table',
    partition_by={
      "field": "join_yearmonth",
      "data_type": "date",
    },
    cluster_by = "age_group_id",
)}}

SELECT * FROM {{ ref('fact_bank_marketing') }}
