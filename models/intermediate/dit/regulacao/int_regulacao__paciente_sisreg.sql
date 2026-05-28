{{
  config(
    schema="intermediario_regulacao",
    alias="paciente_sisreg",
    materialized="table",
    partition_by={
      "field": "cns_particao",
      "data_type": "int64",
      "range": {"start": 0, "end": 1000000000000000, "interval": 333333333334},
    },
  )
}}


with
  source as (
    select
      paciente_cpf as cpf,
      paciente_cns as cns,
      {{ proper_br("paciente_nome") }} as nome,
      paciente_sexo as sexo,
      paciente_nascimento_data as nascimento_data,
      paciente_nascimento_uf as nascimento_uf,
      {{ clean_cidade("paciente_nascimento_municipio") }} as nascimento_municipio,

      paciente_residencia_uf as residencia_uf,
      {{ clean_cidade("paciente_residencia_municipio") }} as residencia_municipio,
      {{ clean_bairro("paciente_residencia_bairro") }} as residencia_bairro,

      {{ proper_br("paciente_residencia_logradouro") }} as residencia_logradouro,
      {{ proper_br("paciente_residencia_endereco") }} as residencia_endereco,
      {{ proper_br("paciente_residencia_numero") }} as residencia_numero,
      {{ proper_br("paciente_residencia_complemento") }} as residencia_complemento,
      paciente_residencia_cep as residencia_cep,

      array(
        select
          {{ padronize_telefone("t") }}
        from unnest(
          split(paciente_telefones, ",")
        ) as t
        where {{ padronize_telefone("t") }} is not null
      ) as telefone,

      {{ proper_br("paciente_nome_mae") }} as mae_nome,

      _extracted_at,
      safe_cast(paciente_cns as int64) as cns_particao
    from {{ ref("raw_sisreg_api_v2__solicitacoes") }}
    qualify row_number() over (
      partition by paciente_cns
      order by _extracted_at desc nulls last
    ) = 1
  )

select *
from source
