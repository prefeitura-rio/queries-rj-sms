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

      -- desfaz struct()s porque vamos precisar fazer GROUP BY
      solicitacao.id as solicitacao_id,
      solicitacao.solicitacao_datahora as solicitacao_solicitacao_datahora,
      solicitacao.atualizacao_datahora as solicitacao_atualizacao_datahora,
      solicitacao.detalhe_tipo as solicitacao_detalhe_tipo,
      solicitacao.detalhe_status as solicitacao_detalhe_status,
      solicitacao.detalhe_responsavel as solicitacao_detalhe_responsavel,
      solicitacao.visualizada_regulador as solicitacao_visualizada_regulador,
      solicitacao.tipo_vaga as solicitacao_tipo_vaga,
      solicitacao.classificacao_risco as solicitacao_classificacao_risco,
      solicitacao.data_desejada as solicitacao_data_desejada,
      solicitacao.unidade_desejada_id_cnes as solicitacao_unidade_desejada_id_cnes,
      solicitacao.unidade_desejada_nome as solicitacao_unidade_desejada_nome,

      cancelamento.datahora as cancelamento_datahora,
      cancelamento.justificativa as cancelamento_justificativa,
      cancelamento.perfil as cancelamento_perfil,

      procedimento.grupo_codigo as procedimento_grupo_codigo,
      procedimento.grupo_nome as procedimento_grupo_nome,
      procedimento.id as procedimento_id,
      procedimento.descricao as procedimento_descricao,
      procedimento.sigtap_id as procedimento_sigtap_id,
      procedimento.sigtap_descricao as procedimento_sigtap_descricao,

      solicitante.uf_sigla as solicitante_uf_sigla,
      solicitante.central_nome as solicitante_central_nome,
      solicitante.unidade_id_cnes as solicitante_unidade_id_cnes,
      solicitante.unidade_nome as solicitante_unidade_nome,
      solicitante.profissional_cpf as solicitante_profissional_cpf,
      solicitante.profissional_nome as solicitante_profissional_nome,

      regulador.uf_regulador_codigo_ibge as regulador_uf_regulador_codigo_ibge,
      regulador.uf_regulador_sigla as regulador_uf_regulador_sigla,
      regulador.central_reguladora_codigo as regulador_central_reguladora_codigo,
      regulador.central_reguladora_nome as regulador_central_reguladora_nome,

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

      solicitacao_id,
      -- datahora mais antiga pra esse ID
      MIN(solicitacao_solicitacao_datahora) as solicitacao_datahora,
      -- atualização mais recente pra esse ID
      MAX(solicitacao_atualizacao_datahora) as atualizacao_datahora,
      -- MAX() usado aqui como COALESCE(), pra ignorar nulos
      MAX(solicitacao_detalhe_tipo) as detalhe_tipo,
      MAX(solicitacao_detalhe_status) as detalhe_status,
      MAX(solicitacao_detalhe_responsavel) as detalhe_responsavel,
      MAX(solicitacao_visualizada_regulador) as visualizada_regulador,
      MAX(solicitacao_tipo_vaga) as tipo_vaga,
      MAX(solicitacao_classificacao_risco) as classificacao_risco,
      -- se tivermos múltiplas datas desejadas, é possível que a primeira
      -- já tenha passado; então MAX() aqui faz sentido também
      MAX(solicitacao_data_desejada) as data_desejada,
      -- MAX() de novo como COALESCE()
      MAX(solicitacao_unidade_desejada_id_cnes) as unidade_desejada_id_cnes,
      MAX(solicitacao_unidade_desejada_nome) as unidade_desejada_nome,

      cancelamento_datahora,
      -- MAX() de novo como COALESCE()
      MAX(cancelamento_justificativa) as cancelamento_justificativa,
      MAX(cancelamento_perfil) as cancelamento_perfil,

      -- creio que as chances de termos 2 entradas de solicitações
      -- com um mesmo ID mas solicitando procedimentos distintos seja nula,
      -- então aqui não corremos risco de 'desincronizar' ID e descrição
      MAX(procedimento_grupo_codigo) as procedimento_grupo_codigo,
      MAX(procedimento_grupo_nome) as procedimento_grupo_nome,
      MAX(procedimento_id) as procedimento_id,
      MAX(procedimento_descricao) as procedimento_descricao,
      procedimento_sigtap_id,
      MAX(procedimento_sigtap_descricao) as procedimento_sigtap_descricao,

      -- similarmente, se temos 2 entradas de solicitações com mesmo ID,
      -- espera-se que elas tenham mesmo valor pra solicitante
      MAX(solicitante_uf_sigla) as solicitante_uf_sigla,
      MAX(solicitante_central_nome) as solicitante_central_nome,
      solicitante_unidade_id_cnes as solicitante_unidade_id_cnes,
      MAX(solicitante_unidade_nome) as solicitante_unidade_nome,
      MAX(solicitante_profissional_cpf) as solicitante_profissional_cpf,
      MAX(solicitante_profissional_nome) as solicitante_profissional_nome,


      MAX(regulador_uf_regulador_codigo_ibge) as regulador_uf_regulador_codigo_ibge,
      MAX(regulador_uf_regulador_sigla) as regulador_uf_regulador_sigla,
      MAX(regulador_central_reguladora_codigo) as regulador_central_reguladora_codigo,
      MAX(regulador_central_reguladora_nome) as regulador_central_reguladora_nome,

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
      solicitacao_id,
      cancelamento_datahora,
      procedimento_sigtap_id,
      solicitante_unidade_id_cnes,
      fonte
  ),

  particionado as (
    select
      paciente_cpf,
      paciente_cns,

      -- refaz struct()s agora que já está agrupado 😵‍💫
      struct(
        solicitacao_id as id,
        solicitacao_datahora,
        atualizacao_datahora,
        detalhe_tipo,
        detalhe_status,
        detalhe_responsavel,
        visualizada_regulador,
        tipo_vaga,
        classificacao_risco,
        data_desejada,
        unidade_desejada_id_cnes,
        unidade_desejada_nome
      ) as solicitacao,

      struct(
        cancelamento_datahora as datahora,
        cancelamento_justificativa as justificativa,
        cancelamento_perfil as perfil
      ) as cancelamento,

      struct(
        procedimento_grupo_codigo as grupo_codigo,
        procedimento_grupo_nome as grupo_nome,
        procedimento_id as id,
        procedimento_descricao as descricao,
        procedimento_sigtap_id as sigtap_id,
        procedimento_sigtap_descricao as sigtap_descricao
      ) as procedimento,

      struct(
        solicitante_uf_sigla as uf_sigla,
        solicitante_central_nome as central_nome,
        solicitante_unidade_id_cnes as unidade_id_cnes,
        solicitante_unidade_nome as unidade_nome,
        solicitante_profissional_cpf as profissional_cpf,
        solicitante_profissional_nome as profissional_nome
      ) as solicitante,

      struct(
        regulador_uf_regulador_codigo_ibge as uf_regulador_codigo_ibge,
        regulador_uf_regulador_sigla as uf_regulador_sigla,
        regulador_central_reguladora_codigo as central_reguladora_codigo,
        regulador_central_reguladora_nome as central_reguladora_nome
      ) as regulador,

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
