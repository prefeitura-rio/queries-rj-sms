{{
    config(
        alias="administracao_unidade",
    )
}}

with source as (
      select * from {{ source('osinfo', 'administracao_unidade') }}
),
renamed as (
    select
        {{ adapter.quote("cod_unidade") }},
        {{ adapter.quote("cnes") }},
        {{ adapter.quote("nome_fantasia") }},
        {{ adapter.quote("sigla_tipo") }},
        {{ adapter.quote("sigla_perfil") }},
        {{ adapter.quote("sigla_gestao") }},
        {{ adapter.quote("sigla_tipo_gestao") }},
        {{ adapter.quote("razao_social") }},
        {{ adapter.quote("nome_fantasia_original") }},
        {{ adapter.quote("unidade_abreviado") }},
        {{ adapter.quote("sigla") }},
        {{ adapter.quote("cnpj") }},
        {{ adapter.quote("endereco") }},
        {{ adapter.quote("numero") }},
        {{ adapter.quote("complemento") }},
        {{ adapter.quote("bairro") }},
        {{ adapter.quote("municipio") }},
        {{ adapter.quote("cod_municipio") }},
        {{ adapter.quote("uf") }},
        {{ adapter.quote("cep") }},
        {{ adapter.quote("referencia") }},
        {{ adapter.quote("telefone_ddd") }},
        {{ adapter.quote("telefone_1") }},
        {{ adapter.quote("telefone_1_ramal") }},
        {{ adapter.quote("telefone_2") }},
        {{ adapter.quote("telefone_2_ramal") }},
        {{ adapter.quote("fax") }},
        {{ adapter.quote("telefone_sms_consulta") }},
        {{ adapter.quote("telefone_sms_exame") }},
        {{ adapter.quote("email") }},
        {{ adapter.quote("data_ultima_atualizacao") }},
        {{ adapter.quote("data_avaliacao") }}

    from source
)
select * from renamed
  