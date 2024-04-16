{{
    config(
        alias="assistencial_procedimento",
    )
}}

with
    source as (
        select * from {{ ref('raw_sheets_assistencial_procedimento') }}
    )

select * from source
