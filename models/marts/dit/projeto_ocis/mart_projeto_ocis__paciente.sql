{{
  config(
    alias="paciente",
    materialized="table",
    schema="projeto_ocis",
    meta={"owner": "avellar"}
  )
}}

-- Queremos uma tabela com 1 entrada para cada paciente
-- que apareça ou no Sarah ou no MediLab

with sarah_atendimento as (
  select distinct
    if(
      {{ validate_cpf("paciente_cpf") }},
      paciente_cpf,
      cast(null as string)
    ) as cpf,
    if(
      {{ validate_cns("paciente_cns") }},
      paciente_cns,
      cast(null as string)
    ) as cns,
    {{
      dbt_utils.generate_surrogate_key([
        "upper(regexp_replace(normalize(paciente_nome, NFD), r'[^\p{Letter}]', ''))",
        "paciente_data_nascimento"
      ])
    }} as nome_nasc_id,
    upper(paciente_nome) as nome,
    paciente_data_nascimento as data_nascimento
  from {{ ref("raw_prontuario_sarah__atendimento") }}
  where paciente_data_nascimento is not null
    and paciente_data_nascimento >= '1926-01-01'
),

medilab_exames as (
  select distinct
    if(
      {{ validate_cpf("paciente_cpf") }},
      paciente_cpf,
      cast(null as string)
    ) as cpf,
    if(
      {{ validate_cns("paciente_cns") }},
      paciente_cns,
      cast(null as string)
    ) as cns,
    {{
      dbt_utils.generate_surrogate_key([
        "upper(regexp_replace(normalize(paciente_nome, NFD), r'[^\p{Letter}]', ''))",
        "paciente_data_nascimento"
      ])
    }} as nome_nasc_id,
    upper(paciente_nome) as nome,
    paciente_data_nascimento as data_nascimento
  from {{ ref("raw_medilab__exames") }}
),


cpf_existe as (
  select
    cpf,
    array_concat(
      array_agg(s.cns ignore nulls),
      array_agg(m.cns ignore nulls)
    ) as cns,
    coalesce(s.nome_nasc_id, m.nome_nasc_id) as nome_nasc_id,
    coalesce(s.nome, m.nome) as nome,
    coalesce(s.data_nascimento, m.data_nascimento) as data_nascimento
  from sarah_atendimento as s
  full outer join medilab_exames as m
    using (cpf)
  where cpf is not null
  group by cpf, nome_nasc_id, nome, data_nascimento
),

cns_existe as (
  select
    cast(null as string) as cpf,
    cns,
    coalesce(s.nome_nasc_id, m.nome_nasc_id) as nome_nasc_id,
    coalesce(s.nome, m.nome) as nome,
    coalesce(s.data_nascimento, m.data_nascimento) as data_nascimento
  from sarah_atendimento as s
  full outer join medilab_exames as m
    using (cns)
  where
    s.cpf is null
    and cns is not null
),

mapeamento_cpf_cns as (
  select
    cpf,
    valor_cns
  from {{ ref("mart_historico_clinico__paciente") }},
    unnest(cns) as valor_cns
  where {{ validate_cpf("cpf") }}
),

cpf_preenchido as (
  select
    mp.cpf,
    array_agg(distinct orig.cns) as cns,
    orig.nome_nasc_id,
    orig.nome,
    orig.data_nascimento
  from cns_existe as orig
  left join mapeamento_cpf_cns as mp
    on (orig.cns = mp.valor_cns)
  group by cpf, nome_nasc_id, nome, data_nascimento
),


todos as (
  select * from (
    select *, "cpf" as fonte from cpf_existe
    union all
    select *, "cns" as fonte from cpf_preenchido
  )
  qualify row_number() over (
    partition by nome_nasc_id, cpf
    order by cpf asc nulls last
  ) = 1
)

select
  cpf,
  -- Deduplica lista de CNS
  array(
    select distinct c
    from unnest(cns) as c
  ) as cns,
  nome_nasc_id,
  nome,
  data_nascimento,
  fonte
from todos
