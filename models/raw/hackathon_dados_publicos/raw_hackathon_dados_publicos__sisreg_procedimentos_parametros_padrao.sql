{{
    config(
        alias="sisreg_procedimentos_parametros_padrao",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}


with source as (
      select * from {{ source('hackathon_dados_publicos_staging', 'sisreg_procedimentos_parametros_padrao') }}
),
renamed as (
    select
        id_procedimento,
        nome,
        safe_cast(qtd_consulta_por_hora as int64) as qtd_consulta_por_hora,
        proporcao_de_consulta

    from source
)
select * from renamed