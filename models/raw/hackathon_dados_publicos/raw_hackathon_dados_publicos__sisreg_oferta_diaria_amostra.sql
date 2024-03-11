{{
    config(
        alias="sisreg_oferta_diaria_amostra",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}

with source as (
      select * from {{ source('hackathon_dados_publicos_staging', 'sisreg_oferta_diaria_amostra') }}
),
renamed as (
    select
        unidade as estabelecimento_nome,
        cnes_unidade as id_estabelecimento,
        procedimento as prodecimento_nome,
        cod_procedimento as id_procedimento,
        data_vaga,
        dia_semana,
        hora_vaga,
        nome_profissional as profissional_nome,
        tipo,
        safe_cast(qtd_vagas as int64) as qtd_vagas,
        data

    from source
)
select * from renamed
