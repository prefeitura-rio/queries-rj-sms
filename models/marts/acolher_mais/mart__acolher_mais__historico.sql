{{
    config(
        alias="historico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}
with
    episodios as (
        select
            cpf,
            exit_datetime,
            location,
            cids,
            clinical_motivation,
            clinical_outcome,
            cast(cpf as int64) as cpf_particao
        from {{ ref('mart_historico_clinico_app__episodio') }}
        where 
        cast(cast(exit_datetime as datetime) as date) >= date_add(current_date(), interval -1 year)
    )
select *
from episodios