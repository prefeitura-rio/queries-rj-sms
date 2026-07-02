{{
  config(
    schema="regulacao",
    alias="solicitacao",
    materialized="table",
    partition_by={
      "field": "cpf_particao",
      "data_type": "int64",
      "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    },
  )
}}

with
  solicitacao_sisreg as (
    select
      * except (_run_id),

      -- Marcação
      struct(
        cast(null as string) as id,
        cast(null as datetime) as datahora,
        cast(null as datetime) as aprovacao_datahora,
        cast(null as date) as confirmacao_data,
        cast(null as string) as flag_paciente_avisado,
        cast(null as string) as flag_executada,
        cast(null as string) as flag_falta_registrada
      ) as marcacao,
      -- Execução
      struct(
        cast(null as string) as profissional_crm,
        cast(null as string) as profissional_cpf,
        cast(null as string) as profissional_nome,
        cast(null as string) as unidade_id_cnes,
        cast(null as string) as unidade_nome,
        cast(null as string) as unidade_telefone,
        cast(null as string) as unidade_cep,
        cast(null as string) as unidade_municipio,
        cast(null as string) as unidade_bairro,
        cast(null as string) as unidade_logradouro,
        cast(null as string) as unidade_numero,
        cast(null as string) as unidade_complemento
      ) as execucao,

      "sisreg" as fonte,
      2 as rank
    from {{ ref("int_regulacao__solicitacao_sisreg") }}
  ),
  marcacao_sisreg as (
    select
      * except(marcacao, execucao, _run_id),

      marcacao,
      execucao,

      "sisreg" as fonte,
      1 as rank
    from {{ ref("int_regulacao__marcacao_sisreg") }}
  ),

  joined as (
    select
      if(
        {{ validate_cpf("paciente_cpf") }},
        paciente_cpf,
        cast(null as string)
      ) as paciente_cpf,
      if(
        {{ validate_cns("paciente_cns") }},
        paciente_cns,
        cast(null as string)
      ) as paciente_cns,
      solicitacao,
      cancelamento,
      procedimento,
      solicitante,
      regulador,
      laudo,
      marcacao,
      execucao,
      fonte,
      _extracted_at
    from (
      select * from solicitacao_sisreg
      union all
      select * from marcacao_sisreg
    )
  ),

  aggregated as (
    select
      paciente_cpf,
      paciente_cns,
      solicitacao,
      cancelamento,
      procedimento,
      solicitante,
      regulador,
      array_agg(
        laudo ignore nulls
        order by laudo.datahora_observacao
      ) as laudo,
      array_agg(
        marcacao ignore nulls
        order by marcacao.datahora
      ) as marcacao,
      array_agg(
        execucao ignore nulls
        -- TODO: vai me dar dor de cabeça
      ) as execucao,
      fonte,
      max(_extracted_at) as _extracted_at
    from joined
    group by
      paciente_cpf,
      paciente_cns,
      solicitacao,
      cancelamento,
      procedimento,
      solicitante,
      regulador,
      fonte
  ),

  particionado as (
    select
      * except (laudo, marcacao, execucao, fonte, _extracted_at),
      array(
        select distinct s
        from unnest(laudo) as s
        where (
          s.situacao is not null
          or s.datahora_observacao is not null
        )
      ) as laudo,
      array(
        select distinct s
        from unnest(marcacao) as s
        where s.id is not null
      ) as marcacao,
      array(
        select distinct s
        from unnest(execucao) as s
        where coalesce(
          s.profissional_cpf,
          s.unidade_id_cnes
        ) is not null
      ) as execucao,
      fonte,
      _extracted_at,
      cast(paciente_cpf as int64) as cpf_particao
    from aggregated
  )

select *
from particionado
