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

with sarah_pacientes as (
  select

    json_value(data, "$.paciente.cpf") as paciente_cpf,
    json_value(data, "$.paciente.cns") as paciente_cns,
    json_value(data, "$.paciente.nome") as paciente_nome,
    date(
      json_value(data, "$.paciente.data_nascimento")
    ) as paciente_data_nascimento,

  from {{ source("brutos_prontuario_sarah_api_staging", "atendimento_continuo") }}
  where source_id != "string"
  qualify row_number() over (partition by source_id order by datalake_loaded_at desc) = 1
),
sarah_pacientes_tratados as (
  select
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
  from sarah_pacientes
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
  from sarah_pacientes_tratados as s
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
  from sarah_pacientes_tratados as s
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
    t.paciente_id as id_paciente,

    t.cpf as cpf_paciente,
    -- Deduplica lista de CNS
    array(
      select distinct c
      from unnest(t.cns) as c
    ) as cns_paciente,
    t.nome as nm_paciente,
    upper(b.mae_nome) as nm_mae_paciente,
    if(
      b.sexo = "não informado",
      null,
      b.sexo
    ) as sexo_paciente,
    t.data_nascimento as data_nascimento_paciente,
    concat("raca-fake-", floor(5*rand())) as ds_raca_paciente,

    (b.nome is not null) as _rf_cpf_existe,
    if(
      b.nome is not null,
      upper(regexp_replace(normalize(b.nome, NFD), r'[^\p{Letter}]', ''))
      = upper(regexp_replace(normalize(t.nome, NFD), r'[^\p{Letter}]', '')),
      null
    ) as _rf_cpf_mesmo_nome,
    if(
      b.nome is not null,
      b.nascimento_data = t.data_nascimento,
      null
    ) as _rf_cpf_mesmo_nascimento,

    t.prontuario as _prontuario_fonte,
    t.fonte as _campo_fonte
  from todos_com_cpf as t
  left join {{ ref("raw_bcadastro__cpf") }} as b
    on (cast(t.cpf as INT64) = b.cpf_particao)
)

select *
from dados_completos
