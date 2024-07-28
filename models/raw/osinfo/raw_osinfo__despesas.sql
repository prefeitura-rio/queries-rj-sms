{{
    config(
        alias="despesas",
    )
}}


with source as (
      select * from {{ source('osinfo', 'despesas') }}
),
renamed as (
    select
        {{ adapter.quote("id_documento") }},
        {{ adapter.quote("cod_organizacao") }},
        {{ adapter.quote("cod_unidade") }},
        {{ adapter.quote("data_envio") }},
        {{ adapter.quote("id_tipo_documento") }},
        {{ adapter.quote("codigo_fiscal") }},
        {{ adapter.quote("cnpj") }},
        {{ adapter.quote("razao") }},
        {{ adapter.quote("cpf") }},
        {{ adapter.quote("nome") }},
        {{ adapter.quote("num_documento") }},
        {{ adapter.quote("serie") }},
        {{ adapter.quote("descricao") }},
        {{ adapter.quote("data_emissao") }},
        {{ adapter.quote("data_vencimento") }},
        {{ adapter.quote("data_pagamento") }},
        {{ adapter.quote("data_apuracao") }},
        {{ adapter.quote("valor_documento") }},
        {{ adapter.quote("valor_pago") }},
        {{ adapter.quote("id_despesa") }},
        {{ adapter.quote("id_rubrica") }},
        {{ adapter.quote("id_contrato") }},
        {{ adapter.quote("id_conta_bancaria") }},
        {{ adapter.quote("referencia_mes") }},
        {{ adapter.quote("referencia_ano") }},
        {{ adapter.quote("cod_bancario") }},
        {{ adapter.quote("flg_justificativa") }},
        {{ adapter.quote("parcela_mes") }},
        {{ adapter.quote("parcelamento_total") }},
        {{ adapter.quote("id_imagem") }},
        {{ adapter.quote("nf_validada_sigma") }},
        {{ adapter.quote("data_validacao") }}

    from source
)
select * from renamed
  