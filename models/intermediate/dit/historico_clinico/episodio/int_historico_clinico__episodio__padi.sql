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
 por conter campos relevantes e que já aparecem em outras tabelas mais "nichadas". Também
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
    avd_modalidade,
    linha_cuidado,
    servico_tipo,
    origem_tipo,
    unidade_nome,
    avd_profissional_nome,
    avd_profissional_cargo,
    extracted_at
  from {{ref('raw_prontuario_sarah_padi__atendimento_domiciliar_pacientes')}}
),

  -- =============================
  -- Desfecho do atendimento
  -- =============================
saidas as (
  select 
    id_registro as id_atendimento,
    motivo,
    registro_alta
  from {{ ref('raw_prontuario_sarah_padi__atendimento_domiciliar_saidas') }}
),

  -- =============================
  -- Procedimentos Realizados
  -- =============================
procedimentos_realizados as (
  select
    id_atendimento, 
    array_to_string(array_agg(procedimento), "\n") as procedimentos
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
  from {{ ref('dim_condicao_cid10') }}

),

condicoes_agregado as (
  select
    id_atendimento,
    array_agg(
      struct(
        ce.cid, 
        descricao, 
        cast(null as string) as situacao, 
        cast(null as string) as data_diagnostico)
    ) as condicoes
  from cid_explode as ce
  left join cid_detalhes cd using (cid)
  where ce.cid != "0000"
  group by id_atendimento
),
 
  -- =============================
  -- Profissional 
  -- =============================
profissional_pr AS (
  select
    id_atendimento,
    procedimento,
    id_profissional,
    cpf_profissional,
    cargo,
    -- Um atendimento pode ter múltiplos procedimentos e múltiplos profissionais
    -- Foi necessária estabelecer uma ordem de prioridade para o HCI
    row_number() over (
      partition by id_atendimento 
      order by 
        case upper(cargo)
          when 'MÉDICO CLÍNICO GERAL' then 1
          when 'MÉDICO PEDIATRA' then 2
          when 'ENFERMEIRO' then 3
          when 'FISIOTERAPEUTA' then 4
          when 'NUTRICIONISTA' then 5
          when 'PSICOLOGO' then 6
          when 'TÉCNICO DE ENFERMAGEM' then 7
          when 'FONOAUDIOLOGO' then 8
          when 'ASSISTENTE SOCIAL' then 9
          when 'TERAPEUTA OCUPACIONAL' then 10 
          ELSE 4
        end asc
    ) as rn
  from {{ ref('raw_prontuario_sarah_padi__procedimentos_realizados') }}
),

profissional as (
  select distinct
    id_atendimento,
    cast(id_profissional as string) as id,
    cpf_profissional as cpf,
    cns,
    {{ proper_br('nome') }} as nome,
    cbo[OFFSET(0)].cbo as especialidade
  from profissional_pr pr
  left join {{ ref('dim_profissional_saude') }} ps on ps.cpf = pr.cpf_profissional
  where rn = 1
),

  -- =============================
  -- Estabelecimento
  -- =============================
paciente_estabelecimento as (
  select
    id_atendimento,
    unidade_nome,
    -- Necessário pois não há id_cnes nas tabelas do prontuário
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
      {{ proper_estabelecimento('nome_acentuado') }} as estabelecimento,
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
  initcap(servico_tipo) as subtipo,

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
  -- Tenta pegar informações de profissionais da tabela procedimentos_realizados (porque contém mais informação)
  -- Se não, pega as informações (nome e cargo) da tabela do cadastro do paciente
  struct(
    coalesce(p.id, cast(null as string)) as id,
    coalesce(p.cpf, cast(null as string)) as cpf,
    coalesce(p.cns, cast(null as string)) as cns,
    coalesce(p.nome, {{ proper_br('avd_profissional_nome') }}) as nome,
    coalesce(p.especialidade, {{proper_br('avd_profissional_cargo')}}) as especialidade
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
      cast(current_timestamp() as datetime) as processed_at
  ) as metadados,
  cast(paciente_cpf as int64) as cpf_particao,
  cast(coalesce(admissao_data, registro_data) as date) as data_particao
from pacientes
left join condicoes_agregado ca using (id_atendimento)
left join procedimentos_realizados pr using(id_atendimento)
left join saidas s using(id_atendimento)
left join estabelecimentos using(id_atendimento)
left join profissional p using(id_atendimento)
