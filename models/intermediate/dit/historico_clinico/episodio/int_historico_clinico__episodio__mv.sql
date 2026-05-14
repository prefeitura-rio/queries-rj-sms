{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_mv",
        materialized="table",
        unique_key=['id_hci'],
        cluster_by=['id_hci'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


with 

atendimento as (
  SELECT 
    id_hci,
    id_atendimento,
    id_cnes,
    atendimento_datahora,
    atendimento_tipo,
    atendimento_especialidade,  
    loaded_at,
    updated_at
    FROM `rj-sms-dev.Herian__brutos_prontuario_mv.bam` 
),


-- BAM
bam as (
  select
    id_atendimento,
    atendimento_datahora,
    paciente_alergia, 
    queixa_principal, 
    queixa_medica,
    historia_doenca_atual,
    pressao_arterial_sistolica, 
    pressao_arterial_diastolica,
    frequencia_cardiaca,
    frequencia_respiratoria,
    temperatura, 
    saturacao_oxigenio, 
    hipotese_diagnostica,
    conduta_proposta,
  from `rj-sms-dev.Herian__brutos_prontuario_mv.bam`
),


-- Anamnese 
anamnese as (
  select 
    id_atendimento,
    queixa_principal as queixa_principal_anamnese,
    plano_terapeutico,
    conduta_proposta,
    pressao_arterial_sistolica,
    pressao_arterial_diastolica,
    frequencia_cardiaca,
    frequencia_respiratoria,
    temperatura,
    saturacao_oxigenio,
    peso,
    superficie_corporal,
    altura,
    imc,
    cid,
  from rj-sms-dev.Herian__brutos_prontuario_mv.anamnese -- TODO:trocar por modelo
),


parecer as (
  select 
    id_atendimento,
    atendimento_datahora as data_diagnostico,
    split(cid, ' ')[0] as id_cid,
  from `rj-sms-dev.Herian__brutos_prontuario_mv.parecer` -- TOD: trocar por modelo
),


-- Alta
alta as (
  select 
    id_atendimento,
    atendimento_datahora,
    alta_medica_datahora,
    profissional_nome,
    cid_principal,
    procedimentos_realizados,
    evolucao_paciente,
    plano_alta_orientacao_enfermagem,
    orientacao_medica,
  from `rj-sms-dev.Herian__brutos_prontuario_mv.alta` -- TODO:trocar por modelo
),

anamnese_alta as (
  select 
    id_atendimento,
    conduta_proposta,
  from anamnese
  where upper(plano_terapeutico) like "%ALTA%"
),

-- Condições (CID)
condicoes as (
  select 
    id_atendimento,
    cast(data_diagnostico as date) as data_diagnostico,
    id_cid,
    descricao 
  from parecer p
  join `rj-sms.saude_dados_mestres.condicao_cid10` c -- TODO: Trocar por dim_condicao
    on c.id = p.id_cid 
  where id_cid is not null
  union all
  select 
    id_atendimento,
    cast(a.atendimento_datahora as date) as data_diagnostico,
    cid_principal as id_cid,
    descricao
  from alta a
  join `rj-sms.saude_dados_mestres.condicao_cid10` c -- TODO: Trocar por dim_condicao
    on c.id = a.cid_principal
  where cid_principal is not null
),

condicoes_agg as(
  select 
    id_atendimento,
    array_agg(
      struct(
        id_cid,
        descricao,
        cast(null as string) as situacao,
        cast(data_diagnostico as string) as data_diagnostico
      )
    ) as condicoes_agregadas
  from condicoes
  group by id_atendimento
)

select 
  id_hci,
  id_atendimento,
  atendimento_tipo as tipo,
  atendimento_especialidade as subtipo,
  atendimento.atendimento_datahora as entrada_datahora,
  alta_medica_datahora as saida_datahora,

  -- Exames Realizados 

  -- Procedimentos Realizados
  upper(procedimentos_realizados),

  -- Medidas
  struct(
    an.altura as altura,
    an.superficie_corporal as circunferencia_abdominal,
    coalesce(b.frequencia_cardiaca, an.frequencia_cardiaca) as frequencia_cardiaca,
    coalesce(b.frequencia_respiratoria, an.frequencia_respiratoria) as frequencia_respiratoria,
    cast(null as float64) as glicemia,
    cast(null as float64) as hemoglobina_glicada,
    cast(null as float64) as imc,
    cast(null as float64) as peso,
    coalesce(
      b.pressao_arterial_sistolica, 
      an.pressao_arterial_sistolica
    ) as pressao_sistolica,
    coalesce(
      b.pressao_arterial_diastolica, 
      an.pressao_arterial_diastolica
    ) as pressao_diastolica,
    cast(null as string) as pulso_ritmo,
    coalesce(
      b.saturacao_oxigenio,
      an.saturacao_oxigenio
    ) as saturacao_oxigenio,
    coalesce(
      b.temperatura,
      an.temperatura
    ) as temperatura
  ) as medidas,

  -- Motivo do Atendimento
  coalesce(
    upper(queixa_principal),
    upper(queixa_medica),
    upper(queixa_principal_anamnese)
  ) as motivo_atendimento,

  -- Desfecho do Atendimento
  coalesce(
    upper(a.orientacao_medica)
    --upper(evolucao_paciente)
    --upper(aa.conduta_proposta)
  ) as desfecho_atendimento,

  -- Obito Indicador 
  cast(null as boolean) as obito_indicador, 

  -- Condicoes
  c.condicoes_agregadas as condicoes,

  -- Prescricoes 
  -- Medicamentos Administrados

  -- Estabelecimento
  struct(
    id_cnes,
    e.nome_acentuado as nome,
    e.tipo_sms as estabelecimento_tipo
  ) as estabelecimento,

  -- Profissional
  struct(
    cast(null as string) as id,
    cast(null as string) as cpf,
    cast(null as string) as cns,
    profissional_nome as nome,
    cast(null as string) as especialidade
  ) as profissional_saude_responsavel,


  -- Prontuario
  struct(
    concat(id_cnes, '.', id_atendimento) as id_prontuario_global,
    id_atendimento as id_prontuario_local,
    'mv' as fornecedor
  ) as prontuario,

  -- Metadados
  struct(
    updated_at,
    loaded_at as imported_at,
    current_datetime('America/Sao_Paulo') as processed_at
  ) as metadados

from atendimento
left join bam b using(id_atendimento)
left join alta a using(id_atendimento)
left join anamnese an using(id_atendimento)
left join anamnese_alta aa using(id_atendimento)
left join condicoes_agg c using(id_atendimento)
left join rj-sms.saude_dados_mestres.estabelecimento e using (id_cnes) -- TODO:Trocar por dim_estabelecimento