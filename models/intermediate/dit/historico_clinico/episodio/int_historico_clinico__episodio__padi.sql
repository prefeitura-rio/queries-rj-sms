{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_padi",
        materialized="table",
        unique_key=["id_hci"],
        cluster_by=["id_hci"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with

/*
 Como não há uma documentação descrevendo o caminho do paciente dentro do prontuário
 utilizei a tabela `atendimento_domiciliar_pacientes` como base do episódio assistencial
 por conter campos relevantes e que já aparecem em outras tabelas mais "nichadas" mas também
 por ter mais registro que as outras tabelas até o momento da criação deste modelo (17/09/2026).
 Pode ser que futuramente, com a aquisição do histórico do paciente, isso mude.
*/
pacientes as (
  select
    id_hci,
    id_atendimento,
    paciente_cpf,
    paciente_nome,
    registro_data,
    admissao_data,
    alta_data,
    cids,
    servico_tipo,
    origem_tipo,
    unidade_nome,
    avd_profissional_nome,
    avd_profissional_cargo,
    extracted_at
  from {{ref('raw_prontuario_sarah_padi__atendimento_domiciliar_pacientes')}}
  --from `rj-sms.brutos_prontuario_sarah_padi.atendimento_domiciliar_pacientes` 
),

  -- =============================
  -- Desfecho do atendimento
  -- =============================
saidas as (
  select 
    id_registro as id_atendimento,
    motivo,
    registro_alta
 -- from `rj-sms.brutos_prontuario_sarah_padi.atendimento_domiciliar_saidas`
  from {{ ref('raw_prontuario_sarah_padi__atendimento_domiciliar_saidas') }}
),

  -- =============================
  -- Procedimentos Realizados
  -- =============================
procedimentos_realizados as (
  select
    id_atendimento, 
    array_to_string(array_agg(procedimento), "\n") as procedimentos
  --from `rj-sms.brutos_prontuario_sarah_padi.procedimentos_realizados`
  from {{ ref('raw_prontuario_sarah_padi__procedimentos_realizados') }}
  group by id_atendimento
),

  -- =============================
  -- Condições
  -- =============================
cid_array as (
  select
    id_atendimento,
    split(cids, ";") as cids
  from pacientes
  where cids is not null
  union all

),

cid_explode as (
  select
    id_atendimento,
    rpad(cid, 4, "0") as cid
  from cid_array, unnest(cids) as cid
),

cid_detalhes as (
  select 
  -- Gambiarra porque dim_condicao não tá padronizado
    rpad(id,4, "0") as cid, 
    descricao
 -- from `rj-sms.saude_dados_mestres.condicao_cid10`
  from {{ ref('dim_condicao_cid10') }}

),

condicoes_agregado as (
  select
    id_atendimento,
    array_agg(
      struct(ce.cid, descricao, cast(null as string) as situacao, cast(null as string) as data_diagnostico)
    ) as condicoes
  from cid_explode as ce
  left join cid_detalhes cd using (cid)
  where ce.cid != "0000"
  group by id_atendimento
),
 
  -- =============================
  -- Profissional 
  -- =============================
profissional as (
  select distinct
    id_atendimento,
    struct (
      cpf_profissional as cpf,
      cns,
      nome,
      cbo[OFFSET(0)].cbo as especialidade
    ) as profissional_saude_responsavel
  --from `rj-sms.brutos_prontuario_sarah_padi.procedimentos_realizados` pr
  from {{ ref('raw_prontuario_sarah_padi__procedimentos_realizados') }}
  left join {{ ref('dim_profissional_saude') }} ps on ps.cpf = pr.cpf_profissional
  where pr.cargo in (
    "MÉDICO CLÍNICO GERAL",
    "MÉDICO PEDIATRA",
    "ENFERMEIRO",
    "TÉCNICO DE ENFERMAGEM",
    "FISIOTERAPEUTA"
    )
),

  -- =============================
  -- Estabelecimento
  -- =============================
paciente_estabelecimento as (
  select
    id_atendimento,
    unidade_nome,
    case unidade_nome
      when "SMS PADI LOURENCO JORGE AP 40" then "7063679"
      when "SMS PADI SOUZA AGUIAR AP 10" then  "8205590"
      when "SMS PADI MIGUEL COUTO AP 21" then "6694330"
      when "SMS PADI FRANCISCO DA SILVA TELLES AP 33" then "7110340"
      when "SMS PADI SALGADO FILHO AP 32" then "6694101"
      when "SMS PADI ROCHA FARIA AP 52" then "2976706"
      when "SMS PADI PEDRO II AP 53" then "7110324"
      when "SMS PADI PAULINO WERNECK AP 31" then "4092104"
      when "SMS PADI ALBERT SCHWEITZER AP 51" then "4337557"
    end as id_cnes
  from pacientes
),

dim_estabelecimento as (
  select
    id_cnes,
    nome_acentuado,
    tipo_sms
  from rj-sms.saude_dados_mestres.estabelecimento
),

estabelecimentos as (
  select 
    id_atendimento,
    id_cnes,
    struct(
      id_cnes,
      nome_acentuado as estabelecimento,
      tipo_sms as estabelecimento_tipo
    ) as estabelecimento
  from paciente_estabelecimento
  join dim_estabelecimento using(id_cnes)
)


select 
  id_hci,
  id_atendimento,
-- id_hci                                      
  paciente_cpf,                           
-- tipo
  "Programa de Atenção Domiciliar à Pessoa Idosa" as tipo,

-- subtipo
  initcap(servico_tipo) as subtipo

-- entrada_data
  coalesce(admissao_data, registro_data) as entrada_data,

-- entrada_datahora
  datetime(coalesce(admissao_data, registro_data)) as entrada_datahora,

-- saida_datahora
  coalesce(registro_alta, alta_data) as saida_datahora,

-- procedimentos_realizados     
  pr.procedimentos as procedimentos_realizados,    
             
-- motivo_atendimento  
  case 
    when avd_modalidade is not null and linha_cuidado is not null 
      then concat(avd_modalidade, " - ", linha_cuidado)
    when avd_modalidade is not null and linha_cuidado is null
      then avd_modalidade
    when avd_modalidade is null and linha_cuidado is not null
      then linha_cuidado
    else null
  end as motivo_atendimento,

-- desfecho_atendimento
  s.motivo as desfecho_atendimento,

-- condicoes
  condicoes,

-- estabelecimento
  estabelecimento,

-- profissional
  struct(
    cast(null as string) as id,
    cast(null as string) as cpf,
    cast(null as string) as cns,
    avd_profissional_nome as nome,
    avd_profissional_cargo as especialidade
  ) profissional_saude_responsavel,

-- prontuario
  struct(
    concat(id_cnes,'.', id_atendimento) as id_prontuario_global,
    id_atendimento as id_prontuario_local,
    'padi' as fornecedor
  ) as prontuario,

-- metadados
  struct (
      cast(extracted_at as datetime) as imported_at,
      cast(null as datetime) as updated_at,
      cast(current_timestamp('America/Sao_Paulo') as datetime) as processed_at
  ) as metadados,
  cast(paciente_cpf as int64) as cpf_particao,
  cast( as date) as data_particao
from pacientes
left join condicoes_agregado ca using (id_atendimento)
left join procedimentos_realizados pr using(id_atendimento)
left join saidas s using(id_atendimento)
left join estabelecimentos using(id_atendimento)
left join profissional p using(id_atendimento)
where 
  paciente_cpf is not null
  and coalesce(registro_alta, alta_data) is not null
