{{
    config(
        alias="elegiveis"
    )
}}

with 

cadastros_por_unidade as (
  select
    cpf as paciente_id,
    id_cnes as unidade_id,
    ine_equipe as equipe_id,

    case
      when date_diff(date(current_date()), date(data_nascimento), year) < 2 then '0-2'
      when date_diff(date(current_date()), date(data_nascimento), year) < 6 then '3-6'
      when date_diff(date(current_date()), date(data_nascimento), year) < 13 then '7-13'
      when date_diff(date(current_date()), date(data_nascimento), year) < 18 then '14-18'
      when date_diff(date(current_date()), date(data_nascimento), year) < 40 then '19-40'
      when date_diff(date(current_date()), date(data_nascimento), year) < 65 then '41-65'
      else '66+'
    end as faixa_etaria,

    sexo,
    raca_cor,
    nacionalidade,
    escolaridade,
    territorio_social,
    vulnerabilidade_social,

    updated_at as updated_at
  from {{ ref('raw_prontuario_vitacare_historico__cadastro') }}
  where
    ap = '22'
    and cpf is not null
    and ine_equipe is not null
),

enderecos_por_pessoa as (
  select 
    cpf as paciente_id,
    latitude as endereco_latitude,
    longitude as endereco_longitude,
    score as endereco_score
  from {{source('brutos_hackathon_anthropic','localizacao')}}
  qualify row_number() over (
    partition by paciente_id
    order by endereco_score desc
  ) = 1
),

cadastros as (
  select *
  from cadastros_por_unidade
  qualify row_number() over (
    partition by paciente_id
    order by updated_at desc
  ) = 1
),

cadastros_com_endereco as (
  select 
    cadastros.*,
    enderecos_por_pessoa.endereco_latitude,
    enderecos_por_pessoa.endereco_longitude,
    enderecos_por_pessoa.endereco_score
  from cadastros inner join enderecos_por_pessoa using (paciente_id)
)

select
  {{ anonimize('paciente_id', "'hackathon_anthropic'") }} as paciente_id,
  {{ anonimize('equipe_id', "'hackathon_anthropic'") }} as equipe_id,
  {{ anonimize('unidade_id', "'hackathon_anthropic'") }} as unidade_id,

  paciente_id as cpf,
  equipe_id as equipe,
  unidade_id as unidade,

  faixa_etaria,
  sexo,
  raca_cor,
  nacionalidade,
  escolaridade,
  territorio_social,
  vulnerabilidade_social,

  endereco_latitude,
  endereco_longitude,
  endereco_score

from cadastros_com_endereco