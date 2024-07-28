{{
    config(
        alias="contrato_terceiros",
    )
}}

with source as (
      select * from {{ source('osinfo', 'contrato_terceiros') }}
),
renamed as (
    select
        {{ adapter.quote("id_contrato_terceiro") }},
        {{ adapter.quote("cod_organizacao") }},
        {{ adapter.quote("id_instrumento_contratual") }},
        {{ adapter.quote("valor_mes") }},
        {{ adapter.quote("contrato_mes_inicio") }},
        {{ adapter.quote("contrato_ano_inicio") }},
        {{ adapter.quote("contrato_mes_fim") }},
        {{ adapter.quote("contrato_ano_fim") }},
        {{ adapter.quote("cod_unidade") }},
        {{ adapter.quote("referencia_ano_ass_contrato") }},
        {{ adapter.quote("vigencia") }},
        {{ adapter.quote("cnpj") }},
        {{ adapter.quote("razao_social") }},
        {{ adapter.quote("servico") }},
        {{ adapter.quote("referencia_mes_receita") }},
        {{ adapter.quote("flg_imagem") }},
        {{ adapter.quote("imagem_contrato") }}

    from source
)
select * from renamed
  