{{
    config(
        alias="equipamento",
        schema= "brutos_cnes_ftp"
    )
}}

with source as (
      select * from {{ source('br_ms_cnes', 'equipamento') }}
),
renamed as (
    select
        {{ adapter.quote("ano") }},
        {{ adapter.quote("mes") }},
        {{ adapter.quote("sigla_uf") }},
        {{ adapter.quote("id_municipio") }},
        {{ adapter.quote("id_estabelecimento_cnes") }},
        {{ adapter.quote("id_equipamento") }},
        {{ adapter.quote("tipo_equipamento") }},
        {{ adapter.quote("quantidade_equipamentos") }},
        {{ adapter.quote("quantidade_equipamentos_ativos") }},
        {{ adapter.quote("indicador_equipamento_disponivel_sus") }},
        {{ adapter.quote("indicador_equipamento_indisponivel_sus") }}

    from source
)
select * from renamed
  