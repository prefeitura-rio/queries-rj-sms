{{
    config(
        alias="pacientes"
    )
}}

with 

cadastros_por_unidade as (
  select
    cpf as paciente_id,
    ine_equipe as equipe_id,
    
    date_diff(date(current_date()), date(data_nascimento), year) as idade,
    sexo,
    raca_cor,
    upper(bairro) as endereco_bairro,

    updated_at as updated_at
  from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
  where
    id_cnes = '2280787'
    and cpf is not null
    and ine_equipe is not null
),

cadastros as (
  select *
  from cadastros_por_unidade
  qualify row_number() over (
    partition by paciente_id
    order by updated_at desc
  ) = 1
)

select
  {{ anonimize('paciente_id', "'projeto_wac'") }} as paciente_id,
  {{ anonimize('equipe_id', "'projeto_wac'") }} as equipe_id,

  idade,
  sexo,
  raca_cor,

  endereco_bairro

from cadastros 