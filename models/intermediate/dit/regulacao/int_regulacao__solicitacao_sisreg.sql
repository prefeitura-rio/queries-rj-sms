{{
  config(
    schema="intermediario_regulacao",
    alias="solicitacao_sisreg",
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "month"
    },
    meta={"owner": "avellar", "team": "cit"},
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

      -- [1] Todo campo de data/hora do Sisreg vem no formato '2026-01-01T00:00:00.000Z',
      --     mas após investigações, creio que esse 'Z' seja mentira!! A hora já vem em
      --     UTC-3, mas marcada como UTC+0. Então aqui convertemos de string pra TIMESTAMP,
      --     depois pra DATETIME sem passar um fuso, e ele efetivamente só corta o Z fora.
        datetime(timestamp(solicitacao_datahora)) as solicitacao_datahora,
        datetime(timestamp(atualizacao_datahora)) as atualizacao_datahora,

        -- ex. "SOLICITAÇÃO / DEVOLVIDA / REGULADOR"
        trim(split(solicitacao_status, "/")[safe_offset(0)]) as detalhe_tipo,
        trim(split(solicitacao_status, "/")[safe_offset(1)]) as detalhe_status,
        trim(split(solicitacao_status, "/")[safe_offset(2)]) as detalhe_responsavel,

        solicitacao_visualizada_regulador as visualizada_regulador,
        tipo_vaga_solicitada as tipo_vaga,
        classificacao_risco,
        date(data_desejada) as data_desejada,
        unidade_desejada_id_cnes,
        {{ add_accents_estabelecimento("unidade_desejada_nome") }} as unidade_desejada_nome
      ) as solicitacao,

      ----------------
      -- Cancelamento
      struct(
        -- [2] ...EXCETO em data_cancelamento! Nesse caso, mesmo tendo o 'Z' de UTC+0,
        --     eles parecem estar processando o fuso duas vezes. Então um cancelamento
        --     às 10:00 vai ser convertido 1x (07:00 UTC-3), depois de novo (04:00 UTC-6),
        --     e no final recebemos uma data_cancelamento "04:00:00Z".
        --     Calma que piora!! Porque a conversão não só "subtrai 3", mas sim converte
        --     fuso, durante horário de verão (durma em paz), vulgo UTC-2, o horário é
        --     convertido 2x subtraindo 2 e não 3, então o resultado é UTC-4 e não UTC-6!
        --     Pra resolver, calculamos o offset ao UTC dessa data/hora pra descobrir se
        --     era horário de verão ou não, e somamos esse offset de volta na hora pra
        --     desfazer a conversão duplicada.
        -- Exs.:
        --  -> "2026-06-03T13:21:36.962Z"  (que deveria ser 16:21)
        --     # Calcula offset
        --     datetime_diff(
        --       datetime(timestamp(cancelamento_datahora), "America/Sao_Paulo"),  # 10:21
        --       datetime(timestamp(cancelamento_datahora), "UTC"),                # 13:21
        --       hour
        --     )
        --     # Offset é -3; adiciona -(-3) = +3 à hora em UTC
        --     datetime_add(
        --       datetime(timestamp(cancelamento_datahora), "UTC"),                # 13:21
        --       interval -[OFFSET] hour
        --     )
        --     => 2026-06-03T16:21:36.962
        --  -> "2017-11-15T08:27:40.307Z" => 2017-11-15T10:27:40.307
        datetime_add(
          datetime(timestamp(cancelamento_datahora), "UTC"),
          interval -(
            datetime_diff(
              datetime(timestamp(cancelamento_datahora), "America/Sao_Paulo"),
              datetime(timestamp(cancelamento_datahora), "UTC"),
              hour
            )
          ) hour
        ) as datahora,
        cast(null as string) as justificativa,
        -- perfil_cancelamento_codigo,
        {{ proper_br("perfil_cancelamento_nome") }} as perfil  -- solicitante, regulador/autorizador, ...
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
        -- Vide [1]
        datetime(timestamp(laudo_datahora_observacao)) as datahora_observacao
      ) as laudo,

      tipo_externo as tipo,
      _run_id,

      -- [3] _extract_at é timestamp nossa, então precisa receber fuso sim
      datetime(
        _extracted_at,
        "America/Sao_Paulo"
      ) as _extracted_at,
      data_particao

    from {{ ref("raw_sisreg_api_v2__solicitacao_ambulatorial") }}
    {% if is_incremental() %}
      -- Só partições dos últimos 13 meses; extração é último ano
      where data_particao >= date_sub(
        current_date("America/Sao_Paulo"),
        interval 13 month
      )
    {% endif %}

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
        datetime(timestamp(atualizacao_datahora)) as atualizacao_datahora,

        cast(null as string) as detalhe_tipo,
        upper(trim(solicitacao_status)) as detalhe_status,
        cast(null as string) as detalhe_responsavel,

        cast(null as string) as visualizada_regulador,
        cast(null as string) as tipo_vaga,
        classificacao_risco,
        date(data_desejada) as data_desejada,
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

      -- Vide [3]
      datetime(
        _extracted_at,
        "America/Sao_Paulo"
      ) as _extracted_at,
      data_particao

    from {{ ref("raw_sisreg_api_v2__solicitacao_hospitalar") }}
    {% if is_incremental() %}
      -- Só partições dos últimos 13 meses; extração é último ano
      where data_particao >= date_sub(
        current_date("America/Sao_Paulo"),
        interval 13 month
      )
    {% endif %}
  )

select *
from source
