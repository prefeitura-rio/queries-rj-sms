{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_prontuaRio",
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

triagem as (
  select
    gid_boletim,
    pbe.gid_prontuario,
    data_registro as entrada_datahora,
    struct(
        safe_cast(altura as float64) as altura, 
        cast(null as float64) as circunferencia_abdominal,
        safe_cast(frequencia_cardiaca as float64) as frequencia_cardiaca,
        cast(null as float64) as frequencia_respiratoria,
        cast(null as float64) as glicemia,
        cast(null as float64) as hemoglobina_glicada,
        cast(null as float64) as imc,
        safe_cast(peso as float64) as peso,
        case
          when pressao_arterial like '%x%'
            then safe_cast(split(pressao_arterial, 'x')[offset(0)] as float64)
          else cast(null as float64)
        end as pressao_sistolica,
        case 
          when pressao_arterial like '%x%'
            then safe_cast(split(pressao_arterial, 'x')[offset(1)] as float64)
          else cast(null as float64) 
        end as pressao_diastolica,
        cast(null as string) as pulso_ritmo,
        safe_cast(spo2 as float64) as saturacao_oxigenio,
        safe_cast(temperatura as float64) as temperatura
    ) as medidas,
    concat(
      'Queixas:\n',
      queixas,
      '\n\nReceituario do Profissional de Triagem:\n',
      descricao_receituario
      ) as motivo_atendimento,
    t.cnes,
    t.loaded_at
  from {{ ref('raw_prontuario_prontuaRio__triagem') }} t
  left join {{ ref("raw_prontuario_prontuaRio__prontuario_be") }} pbe using(gid_boletim)

),

------------------------------ EMERGÊNCIA ------------------------------------

-------------------------------
--     CPF/SAIDA_DATAHORA
-------------------------------
  emerg_resumo as (
    select 
      gid_boletim,
      paciente_cpf,
      datetime(alta_data, alta_hora) as saida_datahora,
    from {{ ref("raw_prontuario_prontuaRio__emergencia_resumo") }}
  ),

-------------------------------
--  DESFECHO DO ATENDIMENTO
-------------------------------
  emerg_ultima_evolucao AS (
    select
      gid_boletim,
      cpf_profissional as cpf,
      nome_profissional,
      upper(descricao) AS desfecho_atendimento,
    from {{ ref("raw_prontuario_prontuaRio__evolucao") }}
    qualify row_number() over(partition by gid_boletim order by evolucao_data desc) = 1
  ),

-------------------------------
--       PROFISSIONAIS
-------------------------------
  dim_profissionais as (
    select 
      id_profissional_sus as id,
      cpf,
      cns,
      nome,
      cbo.cbo as especialidade
    from {{ ref("dim_profissional_saude") }} ps,
    unnest(ps.cbo) as cbo
  ),

  emerg_profissionais_enriquecido as (
    select 
      gid_boletim,
      id,
      cpf,
      cns,
      nome_profissional as nome,
      especialidade
    from emerg_ultima_evolucao
    left join dim_profissionais using(cpf) 
  ),

  emerg_profissionais_agg as (
    select 
      gid_boletim,
      struct(
        id,
        cpf,
        cns,
        {{ proper_br("nome") }} as nome,
        especialidade
      ) profissional_saude_responsavel
    from emerg_profissionais_enriquecido
  ),

-------------------------------
--        CONDICOES
-------------------------------
  emerg_cid as (
    select distinct
      gid_boletim,
      codigo_cid10_1 as cid,
      descricao as nome,
      internacao_data
    from {{ ref("raw_prontuario_prontuaRio__emergencia_resumo") }} er
    inner join {{ ref("dim_condicao_cid10") }} ci on ci.id = codigo_cid10_1
    where codigo_cid10_1 is not null
    union all
    select distinct
      gid_boletim,
      categoria_cid10_1 as cid,
      ci.categoria.descricao as nome,
      internacao_data
    from {{ ref("raw_prontuario_prontuaRio__emergencia_resumo") }} er
    inner join {{ ref("dim_condicao_cid10") }} ci on ci.categoria.id = er.categoria_cid10_1
    where categoria_cid10_1 is not null
    union all
    select distinct 
      gid_boletim,
      codigo_cid10_2 as cid,
      descricao as nome,
      internacao_data
    from {{ ref("raw_prontuario_prontuaRio__emergencia_resumo") }} er
    inner join {{ ref("dim_condicao_cid10") }} ci on ci.id = codigo_cid10_2
    where codigo_cid10_2 is not null
  ),

  emerg_cid_agg as (
    select 
      gid_boletim,
      array_agg(
        struct(
          cid as id,
          nome as descricao,
          cast(null as string) as situacao,
          cast(internacao_data as string) as data_diagnostico
        )
      ) as condicoes,
    from emerg_cid
    group by gid_boletim
  ),

-------------------------------
--  MEDICAMENTO ADMINISTRADOS
-------------------------------

paciente as (
  select 
    gid_boletim,
    gid_prontuario,
    gid_paciente
  from triagem t
  inner join {{ref("raw_prontuario_prontuaRio__paciente")}} p
    on t.gid_boletim = p.gid_registro
  ),

emerg_prescricao as (
  select distinct
  gid_boletim,
  gid_paciente,
  medicamento_nome,
  dose,
  intervalo,
  via,
  atendimento_data
from paciente p
inner join {{ ref("raw_prontuario_prontuaRio__atendimento") }} using(gid_paciente)
inner join {{ ref("raw_prontuario_prontuaRio__prescricao") }} pr using(gid_atendimento)
inner join {{ ref("raw_prontuario_prontuaRio__medicamento") }} m  using(gid_prescricao)
),


emerg_prescricao_agg as (
  select 
    gid_boletim,
    gid_paciente,
    array_agg(
      struct(
        medicamento_nome as nome,
        safe_cast(dose as int64) as quantidade,
        cast(null as string) as unidade_medida,
        intervalo as uso,
        via as via_administracao,
        safe_cast(atendimento_data as timestamp) as prescricao_data
      )
    ) as medicamentos_administrados
  from emerg_prescricao 
  group by gid_boletim, gid_paciente
),


-------------------------------
--      ESTABELECIMENTO
-------------------------------
  emerg_estabelecimento as (
    select 
      gid_boletim,
      struct (
        cnes,
        {{ proper_estabelecimento("nome_acentuado") }} as nome,
        tipo_sms as estabelecimento_tipo
      ) as estabelecimento 
    from {{ ref("raw_prontuario_prontuaRio__triagem") }} t
    inner join{{ ref("dim_estabelecimento") }}es on es.id_cnes = t.cnes
  ),

  
------------------------------ INTERNACAO ------------------------------------

-------------------------------
--    PACIENTE CADASTRO
-------------------------------
inter_cadastro as (
  select distinct
    gid_registro as gid_prontuario, 
    paciente_cpf
  from {{ref("raw_prontuario_prontuaRio__internacao_cadastro")}}
),


-------------------------------
--    DESFECHO DO ATENDIMENTO
-------------------------------
 inter_desfecho as (
  select
    gid_prontuario,
    cpf_profissional as cpf,
    nome_profissional,
    upper(descricao) AS desfecho_atendimento
  from {{ ref("raw_prontuario_prontuaRio__evolucao") }}
  qualify row_number() over(partition by gid_prontuario order by evolucao_data desc) = 1
),

inter_saida as (
  select distinct
    gid_prontuario,
    registro_data as saida_datahora
  from {{ ref("raw_prontuario_prontuaRio__evolucao") }}
),

-------------------------------
--        CONDICOES
-------------------------------
  inter_cid as (
    select distinct
      gid_prontuario,
      codigo_cid10 as id,
      descricao,
      internacao_data,
    from {{ ref("raw_prontuario_prontuaRio__internacao_alta") }} ia
    inner join {{ ref("dim_condicao_cid10") }} c10 on c10.id = ia.codigo_cid10
    where length(codigo_cid10) = 4
    union all
    select distinct
      gid_prontuario,
      codigo_cid10_secundario as id,
      descricao,
      internacao_data,
    from {{ ref("raw_prontuario_prontuaRio__internacao_alta") }} ia
    inner join {{ ref("dim_condicao_cid10") }} c10 on c10.id = ia.codigo_cid10_secundario
    where length(codigo_cid10_secundario) = 4  and codigo_cid10_secundario != codigo_cid10
    union all
    select distinct
      gid_prontuario,
      codigo_cid10 as id,
      categoria.descricao as descricao,
      internacao_data,
    from {{ ref("raw_prontuario_prontuaRio__internacao_alta") }} ia
    inner join {{ ref("dim_condicao_cid10") }} c10 on c10.categoria.id = ia.codigo_cid10_secundario 
    where length(codigo_cid10) = 3
    union all
    select distinct
      gid_prontuario,
      codigo_cid10_secundario as id,
      categoria.descricao as descricao,
      internacao_data,
    from {{ ref("raw_prontuario_prontuaRio__internacao_alta") }} ia
    inner join {{ ref("dim_condicao_cid10") }} c10 on c10.categoria.id = ia.codigo_cid10_secundario
    where length(codigo_cid10_secundario) = 3  and codigo_cid10_secundario != codigo_cid10
  ),

  inter_cid_agg as (
    select 
      gid_prontuario,
      array_agg(
        struct(
          id,
          descricao,
          cast(null as string) as situacao,
          cast(internacao_data as string) as data_diagnostico
        )
      ) as condicoes
    from inter_cid
    group by gid_prontuario
  ),

-------------------------------
--       PROFISSIONAIS
-------------------------------
  inter_profissional as (
    select 
      gid_prontuario,
      medico_alta_cpf as cpf
    from {{ref("raw_prontuario_prontuaRio__internacao_alta")}}
  ),

  inter_profissionais_enriquecido as (
    select distinct
      gid_prontuario,
      id,
      cpf,
      cns,
      nome,
      especialidade
    from inter_profissional ep
    left join dim_profissionais using(cpf) 
  ),

  inter_profissionais_agg as (
    select 
      gid_prontuario,
      struct(
        id,
        cpf,
        cns,
        {{  proper_br("nome") }} as nome, 
        especialidade
      ) profissional_saude_responsavel
    from inter_profissionais_enriquecido
  ),

-------------------------------
--   MEDICAMENTO ADMINISTRADOS
-------------------------------

inter_prescricao as (
  select distinct
  gid_prontuario,
  gid_paciente,
  medicamento_nome,
  dose,
  intervalo,
  via,
  atendimento_data
from paciente p
inner join {{ ref("raw_prontuario_prontuaRio__atendimento") }} using(gid_paciente)
inner join {{ ref("raw_prontuario_prontuaRio__prescricao") }} pr using(gid_atendimento)
inner join {{ ref("raw_prontuario_prontuaRio__medicamento") }} m  using(gid_prescricao)
),


inter_prescricao_agg as (
  select 
    gid_prontuario,
    gid_paciente,
    array_agg(
      struct(
        medicamento_nome as nome,
        safe_cast(dose as int64) as quantidade,
        cast(null as string) as unidade_medida,
        intervalo as uso,
        via as via_administracao,
        safe_cast(atendimento_data as timestamp) as prescricao_data
      )
    ) as medicamentos_administrados
  from inter_prescricao 
  group by gid_prontuario, gid_paciente
),


-------------------------------
--      ESTABELECIMENTO
-------------------------------
  inter_estabelecimento as (
    select 
      gid_prontuario,
      struct(
        t.cnes,
        {{ proper_estabelecimento("nome_acentuado") }} as nome,
        tipo_sms as estabelecimento_tipo
      ) as estabelecimento
    from triagem as t
    inner join {{ ref("dim_estabelecimento") }} es on es.id_cnes = t.cnes
  ),


------------------------------ MERGE ------------------------------------

merge_ as (
    select 
      paciente_cpf as cpf,
      pr.gid_paciente as gid_paciente,
      'Consulta' as tipo,
      'Emergência' as subtipo,
      t.entrada_datahora,
      r.saida_datahora,
      t.motivo_atendimento,
      u.desfecho_atendimento,
      t.medidas,
      c.condicoes,
      pr.medicamentos_administrados,
      e.estabelecimento,
      pa.profissional_saude_responsavel,
      struct(
        gid_boletim as id_prontuario_global,
        split(gid_boletim, '.')[offset(1)] as id_prontuario_local,
        'prontuaRio'as fornecedor
      ) as prontuario,
      loaded_at
    from triagem t
    left join emerg_resumo r using(gid_boletim)
    left join emerg_ultima_evolucao u using(gid_boletim)
    left join emerg_cid_agg c using(gid_boletim)
    left join emerg_estabelecimento e using(gid_boletim)
    left join emerg_profissionais_agg pa using(gid_boletim)
    left join emerg_prescricao_agg pr using(gid_boletim)
    where t.gid_prontuario is null
  union all
    select 
      ic.paciente_cpf as cpf,
      pr.gid_paciente as gid_paciente,
      'Consulta' as tipo,
      'Internação' as subtipo,
      t.entrada_datahora,
      s.saida_datahora,
      motivo_atendimento,
      id.desfecho_atendimento,
      t.medidas,
      ca.condicoes,
      pr.medicamentos_administrados,
      e.estabelecimento,
      p.profissional_saude_responsavel,
      struct(
        t.gid_prontuario as id_prontuario_global,
        split(gid_prontuario, '.')[offset(1)] as id_prontuario_local,
        'prontuaRio' as fornecedor
      ) as prontuario,
      loaded_at

    from triagem t 
    left join inter_cid_agg ca using(gid_prontuario)
    left join inter_desfecho id using(gid_prontuario)
    left join inter_saida s using(gid_prontuario)
    left join inter_profissionais_agg p using(gid_prontuario)
    left join inter_estabelecimento e using(gid_prontuario)
    left join inter_cadastro ic using(gid_prontuario)
    left join inter_prescricao_agg pr using(gid_prontuario)
),


final as (
  select 
      {{
        dbt_utils.generate_surrogate_key(
              [
                  "prontuario.id_prontuario_global",
                  "subtipo",
                  "cpf"
              ]
          )
      }} as id_hci,

      *,
      
      struct(
          cast(loaded_at as datetime) as imported_at,
          cast (null as datetime) as updated_at,
          current_datetime('America/Sao_Paulo') as processed_at
      ) as metadados,
      safe_cast(cpf as int64) as cpf_particao,
      safe_cast(entrada_datahora as date) as data_particao
  from merge_
)


select distinct * from final