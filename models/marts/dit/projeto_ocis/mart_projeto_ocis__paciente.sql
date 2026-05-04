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
      {{ validate_cpf("safe_cast(paciente_cpf as string)") }},
      cast(paciente_cpf as string),
      cast(null as string)
    ) as cpf,
    if(
      {{ validate_cns("safe_cast(paciente_cns as string)") }},
      cast(paciente_cns as string),
      cast(null as string)
    ) as cns,
    {{
      dbt_utils.generate_surrogate_key([
        "upper(regexp_replace(normalize(paciente_nome, NFD), r'[^\p{Letter}]', ''))",
        "paciente_data_nascimento"
      ])
    }} as paciente_id,
    upper(paciente_nome) as nome,
    paciente_data_nascimento as data_nascimento,
    "sarah" as prontuario
  from {{ ref("raw_prontuario_sarah__atendimento") }}
  where (paciente_nome is not null)
    and (paciente_data_nascimento is not null)
),

medilab_exames as (
  select distinct
    if(
      {{ validate_cpf("safe_cast(paciente_cpf as string)") }},
      cast(paciente_cpf as string),
      cast(null as string)
    ) as cpf,
    if(
      {{ validate_cns("safe_cast(paciente_cns as string)") }},
      cast(paciente_cns as string),
      cast(null as string)
    ) as cns,
    {{
      dbt_utils.generate_surrogate_key([
        "upper(regexp_replace(normalize(paciente_nome, NFD), r'[^\p{Letter}]', ''))",
        "paciente_data_nascimento"
      ])
    }} as paciente_id,
    upper(paciente_nome) as nome,
    paciente_data_nascimento as data_nascimento,
    "medilab" as prontuario
  from {{ ref("raw_medilab__exames") }}
  where (paciente_nome is not null)
    and (paciente_data_nascimento is not null)
  -- TODO: Múltiplos pacientes distintos parecem estar vindo
  -- com mesmo CNS no MediLab; algo a ser investigado mais a fundo
),

-- Primeiro, pegamos todos que já possuem CPF preenchido
cpf_existe as (
  select
    cpf,
    array_concat(
      array_agg(s.cns ignore nulls),
      array_agg(m.cns ignore nulls)
    ) as cns,

    coalesce(s.nome, m.nome) as nome,
    coalesce(s.data_nascimento, m.data_nascimento) as data_nascimento,
    coalesce(s.paciente_id, m.paciente_id) as paciente_id,
    if(
      s.prontuario is not null and m.prontuario is not null,
      "ambos",
      coalesce(s.prontuario, m.prontuario)
    ) as prontuario
  from sarah_atendimento as s
  full outer join medilab_exames as m
    using (cpf)
  where cpf is not null
  group by cpf, paciente_id, nome, data_nascimento, prontuario
),

-- Em seguida, pegamos os sem CPF mas com CNS preenchido
cns_existe as (
  select
    cast(null as string) as cpf,
    cns,

    coalesce(s.nome, m.nome) as nome,
    coalesce(s.data_nascimento, m.data_nascimento) as data_nascimento,
    coalesce(s.paciente_id, m.paciente_id) as paciente_id,
    if(
      s.prontuario is not null and m.prontuario is not null,
      "ambos",
      coalesce(s.prontuario, m.prontuario)
    ) as prontuario
  from sarah_atendimento as s
  full outer join medilab_exames as m
    using (cns)
  where
    s.cpf is null
    and cns is not null
),

-- Tentamos descobrir o CPF desses, se possível
mapeamento_cpf_cns as (
  select
    cpf,
    valor_cns
  from {{ ref("mart_historico_clinico__paciente") }},
    unnest(cns) as valor_cns
),

cns_com_cpf as (
  select
    -- Puxamos o CPF de saude_historico_clinico
    any_value(mp.cpf),
    -- Agrupa possíveis múltiplos CNS's da pessoa
    array_agg(distinct orig.cns) as cns,

    orig.nome,
    orig.data_nascimento,
    orig.paciente_id,
    any_value(orig.prontuario)
  from cns_existe as orig
  left join mapeamento_cpf_cns as mp
    on (orig.cns = mp.valor_cns)
  group by paciente_id, nome, data_nascimento
),


todos_com_cpf as (
  select * from (
    select *, "cpf" as fonte from cpf_existe
    union all
    select *, "cns" as fonte from cns_com_cpf
  )
  where cpf is not null
  qualify row_number() over (
    partition by paciente_id, cpf
    order by cpf asc
  ) = 1
),

dados_completos as (
  select
    cpf,
    -- Deduplica lista de CNS
    array(
      select distinct c
      from unnest(t.cns) as c
    ) as cns,
    upper(p.dados.nome) as nome_oficial,
    t.nome as nome_original,
    p.dados.data_nascimento as data_nascimento_oficial,
    t.data_nascimento as data_nascimento_original,
    p.dados.raca,
    p.dados.genero,
    upper(p.dados.mae_nome) as mae_nome,

    t.paciente_id,
    t.prontuario,
    t.fonte
  from todos_com_cpf as t
  left join {{ ref("mart_historico_clinico__paciente") }} as p
    using (cpf)
)

select *
from dados_completos
