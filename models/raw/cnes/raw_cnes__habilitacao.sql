with source as (
      select * from {{ source('br_ms_cnes', 'habilitacao') }}
),
renamed as (
    select
        {{ adapter.quote("ano") }},
        {{ adapter.quote("mes") }},
        {{ adapter.quote("sigla_uf") }},
        {{ adapter.quote("id_municipio") }},
        {{ adapter.quote("id_estabelecimento_cnes") }},
        {{ adapter.quote("quantidade_leitos") }},
        {{ adapter.quote("ano_competencia_inicial") }},
        {{ adapter.quote("mes_competencia_inicial") }},
        {{ adapter.quote("ano_competencia_final") }},
        {{ adapter.quote("mes_competencia_final") }},
        {{ adapter.quote("tipo_habilitacao") }},
        {{ adapter.quote("nivel_habilitacao") }},
        {{ adapter.quote("data_portaria") }},
        {{ adapter.quote("ano_portaria") }},
        {{ adapter.quote("mes_portaria") }}

    from source
)
select * from renamed
  