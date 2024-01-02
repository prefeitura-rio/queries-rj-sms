with source as (
      select * from {{ source('br_ms_cnes', 'leito') }}
),
renamed as (
    select
        {{ adapter.quote("ano") }},
        {{ adapter.quote("mes") }},
        {{ adapter.quote("sigla_uf") }},
        {{ adapter.quote("id_estabelecimento_cnes") }},
        {{ adapter.quote("tipo_especialidade_leito") }},
        {{ adapter.quote("tipo_leito") }},
        {{ adapter.quote("quantidade_total") }},
        {{ adapter.quote("quantidade_contratado") }},
        {{ adapter.quote("quantidade_sus") }}

    from source
)
select * from renamed
  