with source as (
      select * from {{ source('brutos_sheets_staging', 'material_remume') }}
),
renamed as (
    select
        {{ adapter.quote("codigo") }},
        {{ adapter.quote("denominacao_generica") }},
        {{ adapter.quote("concentracao") }},
        {{ adapter.quote("forma_farmaceutica") }},
        {{ adapter.quote("apresentacao") }},
        {{ adapter.quote("grupo") }},
        {{ adapter.quote("disponibilidade") }},
        {{ adapter.quote("disponivel_cms") }},
        {{ adapter.quote("disponivel_cf") }},
        {{ adapter.quote("disponivel_policlinica") }},
        {{ adapter.quote("disponivel_hospital") }},
        {{ adapter.quote("disponivel_maternidade") }},
        {{ adapter.quote("disponivel_caps") }},
        {{ adapter.quote("disponivel_upa") }},
        {{ adapter.quote("disponivel_cer") }},
        {{ adapter.quote("disponivel_unidades_especificas") }}

    from source
)
select * from renamed
  