{{
  config(
    schema="intermediario_regulacao",
    alias="solicitacao_sisreg",
    materialized="table",
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "month"
    },
  )
}}


with
  source as (
    select
      paciente_cpf,
      paciente_cns,

      ----------------
      -- Solicitação
      struct(
        solicitacao_id as id,
        datetime(
          cast(solicitacao_datahora as timestamp),
          "America/Sao_Paulo"
        ) as solicitacao_datahora,
        datetime(atualizacao_datahora, "America/Sao_Paulo") as atualizacao_datahora,

        -- ex. "SOLICITAÇÃO / DEVOLVIDA / REGULADOR"
        trim(split(solicitacao_status, "/")[safe_offset(0)]) as detalhe_tipo,
        trim(split(solicitacao_status, "/")[safe_offset(1)]) as detalhe_status,
        trim(split(solicitacao_status, "/")[safe_offset(2)]) as detalhe_responsavel,

        solicitacao_visualizada_regulador as visualizada_regulador,
        tipo_vaga_solicitada as tipo_vaga,
        classificacao_risco,
        data_desejada,
        unidade_desejada_id_cnes,
        {{ add_accents_estabelecimento("unidade_desejada_nome") }} as unidade_desejada_nome
      ) as solicitacao,

      ----------------
      -- Cancelamento
      struct(
        datetime(cancelamento_datahora, "America/Sao_Paulo") as datahora,
        cast(null as string) as justificativa,
        -- perfil_cancelamento_codigo,
        perfil_cancelamento_nome as perfil  -- solicitante, regulador/autorizador, ...
        -- operador_cancelamento_nome
      ) as cancelamento,

      ----------------
      -- Procedimento
      struct(
        procedimento_grupo_codigo as grupo_codigo, -- Código de grupo sempre termina em '000'
        regexp_replace(
          procedimento_grupo_nome,
          r"^GRUPO\s*-\s*",
          ""
        ) as grupo_nome, -- Nome sempre começa com "GRUPO - "
        procedimento_id as id,
        trim(procedimento_descricao) as descricao,
        procedimento_sigtap_id as sigtap_id,
        trim(procedimento_sigtap_descricao) as sigtap_descricao
      ) as procedimento,

      ----------------
      -- Solicitante
      struct(
        -- uf_solicitante_codigo,
        uf_solicitante_sigla as uf_sigla,
        -- central_solicitante_id_cnes,
        -- central_solicitante_codigo_ibge,
        -- central_solicitante_nome_cnes,
        central_solicitante_nome as central_nome,

        unidade_solicitante_id_cnes as unidade_id_cnes,
        {{ add_accents_estabelecimento("unidade_solicitante_nome") }} as unidade_nome,

        profissional_solicitante_cpf as profissional_cpf,
        {{ proper_br("medico_solicitante_nome") }} as profissional_nome

        -- operador_solicitante_nome
      ) as solicitante,

      ----------------
      -- Regulador
      struct(
        uf_regulador_codigo_ibge,  -- sempre '33'
        uf_regulador_sigla,  -- sempre 'RJ'
        central_reguladora_codigo_ibge as central_reguladora_codigo,  -- sempre '330455', é filtro na extração
        central_reguladora_nome  -- sempre 'RIO DE JANEIRO'
      ) as regulador,

      ----------------
      -- Laudo
      struct(
        laudo_operador_id_cnes as operador_id_cnes,
        {{ add_accents_estabelecimento("laudo_operador_unidade_nome") }} as operador_unidade,

        cid_id,
        cid_descricao,

        laudo_perfil_tipo as perfil_tipo,
        laudo_descricao_tipo as descricao_tipo,
        laudo_situacao as situacao,
        laudo_observacao as observacao,
        datetime(
          laudo_datahora_observacao,
          "America/Sao_Paulo"
        ) as datahora_observacao
      ) as laudo,

      tipo_externo as tipo,
      _run_id,
      datetime(
        _extracted_at,
        "America/Sao_Paulo"
      ) as _extracted_at,
      data_particao

    from {{ ref("raw_sisreg_api_v2__solicitacao_ambulatorial") }}

    union all

    select
      paciente_cpf,
      paciente_cns,

      ----------------
      -- Solicitação
      struct(
        solicitacao_id as id,
        cast(
          concat(
            split(solicitacao_data, "T")[0],
            " ",
            solicitacao_hora
          ) as datetime
        ) as solicitacao_datahora,
        datetime(atualizacao_datahora, "America/Sao_Paulo") as atualizacao_datahora,

        cast(null as string) as detalhe_tipo,
        upper(trim(solicitacao_status)) as detalhe_status,
        cast(null as string) as detalhe_responsavel,

        cast(null as string) as visualizada_regulador,
        cast(null as string) as tipo_vaga,
        classificacao_risco,
        data_desejada,
        unidade_desejada_id_cnes,
        {{ add_accents_estabelecimento("unidade_desejada_nome") }} as unidade_desejada_nome
      ) as solicitacao,

      ----------------
      -- Cancelamento
      struct(
        cast(null as datetime) as datahora,
        cast(null as string) as justificativa,
        cast(null as string) as perfil
      ) as cancelamento,

      ----------------
      -- Procedimento
      struct(
        cast(null as string) as grupo_codigo,
        cast(null as string) as grupo_nome,
        cast(null as string) as id,
        cast(null as string) as descricao,
        procedimento_sigtap_id as sigtap_id,
        trim(procedimento_sigtap_descricao) as sigtap_descricao
      ) as procedimento,

      ----------------
      -- Solicitante
      struct(
        -- uf_solicitante_codigo,
        uf_solicitante_sigla as uf_sigla,
        central_solicitante_nome as central_nome,

        unidade_solicitante_id_cnes as unidade_id_cnes,
        {{ add_accents_estabelecimento("unidade_solicitante_nome") }} as unidade_nome,

        medico_solicitante_cpf as profissional_cpf,
        {{ proper_br("medico_solicitante_nome") }} as profissional_nome

        -- operador_solicitante_nome
      ) as solicitante,

      ----------------
      -- Regulador
      struct(
        uf_regulador_codigo_ibge,
        uf_regulador_sigla,
        central_reguladora_codigo_ibge as central_reguladora_codigo,
        central_reguladora_nome
      ) as regulador,

      ----------------
      -- Laudo
      struct(
        cast(null as string) as operador_id_cnes,
        cast(null as string) as operador_unidade,

        cast(null as string) as cid_id,
        cast(null as string) as cid_descricao,

        cast(null as string) as perfil_tipo,
        cast(null as string) as descricao_tipo,
        cast(null as string) as situacao,
        cast(null as string) as observacao,
        cast(null as datetime) as datahora_observacao
      ) as laudo,

      tipo_externo as tipo,
      _run_id,
      datetime(
        _extracted_at,
        "America/Sao_Paulo"
      ) as _extracted_at,
      data_particao

    from {{ ref("raw_sisreg_api_v2__solicitacao_hospitalar") }}
  )

select *
from source
