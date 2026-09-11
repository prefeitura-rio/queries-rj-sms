{{
    config(
        alias="elegiveis"
    )
}}

with 

cadastros_por_unidade_backup as (
  select
    cpf as paciente_cpf,
    id_cnes as unidade_cnes,
    ine_equipe as equipe_ine,
    date(data_nascimento) as data_nascimento,
    sexo,
    raca_cor,
    ifnull(territorio_social or vulnerabilidade_social,false) as situacao_vulnerabilidade,

    struct(
      tipo_logradouro,
      logradouro,
      cep,
      bairro
    ) as endereco,

    updated_at as updated_at
  from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
  where
    ine_equipe in (
      '0000302872', -- BOA VISTA
      '0002172496', -- CACHOREIRA
      '0000302864' -- FLORESTA
    )
    and cpf is not null
)

select *
from cadastros_por_unidade_backup