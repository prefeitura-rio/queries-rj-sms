{{
    config(
        enabled=true,
        materialized='table',
        schema="brutos_sisreg_api_log",
        alias="logs",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}


with 
    logs as (
        select 
            run_id,
            datetime(as_of) as run_inicio,
            datetime(validation_date) as run_fim,
            environment as run_ambiente,
            bq_table,
            bq_dataset,
            data_inicial,
            data_final,
            completed,
            ano_particao,
            mes_particao,
            parse_date('%Y-%m-%d', data_particao) as data_particao
        from {{ source("brutos_sisreg_api_log_staging", "marcacoes") }}

        UNION ALL 

        select 
            run_id,
            datetime(as_of) as run_inicio,
            datetime(validation_date) as run_fim,
            environment as run_ambiente,
            bq_table,
            bq_dataset,
            data_inicial,
            data_final,
            completed,
            ano_particao,
            mes_particao,
            parse_date('%Y-%m-%d', data_particao) as data_particao
        from {{ source("brutos_sisreg_api_log_staging", "solicitacoes") }}
    )

select * from logs
where data_particao is not null
