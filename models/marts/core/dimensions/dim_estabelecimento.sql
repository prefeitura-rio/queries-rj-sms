{{ config(materialized='table', alias='estabelecimento', tags=['daily']) }}

with source as (
  select * from {{ ref('dim_estabelecimento_sus_rio_historico') }}
),

  -- Estabelecimentos com atendimento odontologico
estabelecimentos_odonto as (
  select distinct
    id_cnes
    from {{ ref('raw_prontuario_vitacare_historico__saudebucal') }}
  ),


current_year as (
  select max(ano_competencia) as ano_atual from source
),

current_period as (
  select 
    cast(current_year.ano_atual as int64) as ano_atual,
    cast(max(mes_competencia) as int64) as competencia_atual
  from source
  join current_year on source.ano_competencia = current_year.ano_atual
  group by current_year.ano_atual
),

final as (
  select
      -- primary key
      id_unidade,

      -- foreign keys
      id_cnes,
      safe_cast(id_tipo_unidade as string) as id_tipo_unidade,
      safe_cast(id_ap as string) as area_programatica,
      cnpj_mantenedora,

      -- common fields
      ativa,
      tipo_sms_agrupado,
      tipo, -- trocar para tipo_cnes?
      tipo_sms,
      tipo_sms_simplificado,
      tipo_disponibilidade,
      nome_limpo,
      {{add_accents_estabelecimento('nome_limpo')}} as nome_acentuado,
      nome_sigla,
      nome_complemento,
      nome_fantasia,
      responsavel_sms,
      administracao,
      prontuario_tem,
      prontuario_versao,
      prontuario_estoque_tem_dado,
      prontuario_estoque_motivo_sem_dado,
      prontuario_episodio_tem_dado,
      endereco_bairro,
      endereco_logradouro,
      endereco_numero,
      endereco_complemento,
      endereco_cep,
      endereco_latitude,
      endereco_longitude,
      telefone,
      email,
      facebook,
      instagram,
      twitter,
      aberto_sempre,
      turno_atendimento,
      diretor_clinico_cpf,
      diretor_clinico_conselho,
      if(
        id_cnes in (select id_cnes from estabelecimentos_odonto),
        true,
        false
      ) as atendimento_odonto_indicador,

      -- metadata
      data_atualizao_registro,
      usuario_atualizador_registro,
      data_particao,
      data_carga,
      data_snapshot

  from
      source
      join current_period on source.ano_competencia = current_period.ano_atual and source.mes_competencia = current_period.competencia_atual
  where
      estabelecimento_sms_indicador = 1

  order by
      ativa desc,
      id_tipo_unidade asc,
      area_programatica asc,
      endereco_bairro asc,
      nome_fantasia asc
)

select * from final