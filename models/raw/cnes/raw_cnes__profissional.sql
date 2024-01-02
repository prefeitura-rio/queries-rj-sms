with source as (
      select * from {{ source('br_ms_cnes', 'profissional') }}
),
renamed as (
    select
        {{ adapter.quote("ano") }},
        {{ adapter.quote("mes") }},
        {{ adapter.quote("sigla_uf") }},
        {{ adapter.quote("id_estabelecimento_cnes") }},
        {{ adapter.quote("id_municipio_6_residencia") }},
        {{ adapter.quote("nome") }},
        {{ adapter.quote("id_vinculo") }},
        {{ adapter.quote("id_registro_conselho") }},
        {{ adapter.quote("id_conselho") }},
        {{ adapter.quote("cartao_nacional_saude") }},
        {{ adapter.quote("cbo_2002") }},
        {{ adapter.quote("indicador_estabelecimento_terceiro") }},
        {{ adapter.quote("indicador_vinculo_contratado_sus") }},
        {{ adapter.quote("indicador_vinculo_autonomo_sus") }},
        {{ adapter.quote("indicador_vinculo_outros") }},
        {{ adapter.quote("indicador_atende_sus") }},
        {{ adapter.quote("indicador_atende_nao_sus") }},
        {{ adapter.quote("carga_horaria_outros") }},
        {{ adapter.quote("carga_horaria_hospitalar") }},
        {{ adapter.quote("carga_horaria_ambulatorial") }}

    from source
)
select * from renamed
  