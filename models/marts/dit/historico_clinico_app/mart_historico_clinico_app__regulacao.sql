{{
  config(
    schema="app_historico_clinico",
    alias="regulacao",
    materialized="table",
    tags=["sisreg_v2"],
    partition_by={
      "field": "cpf_particao",
      "data_type": "int64",
      "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    },
  )
}}

with
  source as (
    select
      s.solicitacao.id as solicitacao_id,
      s.solicitacao.solicitacao_datahora,
      s.solicitacao.atualizacao_datahora,

      s.solicitacao.detalhe_tipo,
      case
        when s.solicitacao.detalhe_status in (
          "CANCELADO", "CANCELADA"
        )
          then "CANCELADO"
        else upper(s.solicitacao.detalhe_status)
      end as detalhe_status,
      s.solicitacao.detalhe_responsavel,
      s.solicitacao.classificacao_risco,

      s.solicitacao.data_desejada,
      {{
        estabelecimento_remove_apendices(
          proper_estabelecimento("s.solicitacao.unidade_desejada_nome")
        )
      }} as unidade_desejada,

      case
        when
          regexp_contains(
            coalesce(s.procedimento.descricao, s.procedimento.sigtap_descricao),
            r"(?i)ACOMPANHAMENTO\s*E\s*AVALIAÇÃO\s*DOMICILIAR\s*DE\s*PACIENTE\s*SUBMETIDO\s*À\s*VENTILAÇÃO\s*MECANICA"
          )
          then "Avaliação domiciliar de paciente em ventilação mecânica"
        else
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    {{ proper_br("coalesce(s.procedimento.descricao, s.procedimento.sigtap_descricao)") }},
                    r"(?i)\bou\b",  -- proper_br transforma em "Ou" mesmo no meio de frase
                    "ou"
                  ),
                  r"(?i)\b(RADIOGRAFIA|RAIO[\-\s]*X)\b",
                  "RX"
                ),
                r"(?i)\bRESSON[AÂ]NCIA\s*MAGN[EÉ]TICA\b",
                "RM"
              ),
              r"(?i)\bTOMOGRAFIA\s*COMPUTADORIZADA\b",
              "TC"
            ),
            r"(?i)ACIDENTE\s*VASCULAR\s*CEREBRAL\s*-?\s*AVC",
            "AVC"
          )
      end as procedimento_descricao,

      {{
        estabelecimento_remove_apendices(
          proper_estabelecimento("s.solicitante.unidade_nome")
        )
      }} as unidade_solicitante,
      s.solicitante.profissional_nome as profissional_solicitante,

      array(
        select as struct
          *
        from (
          select as struct
            * except (operador_id_cnes, operador_unidade),
            {{
              estabelecimento_remove_apendices(
                proper_estabelecimento("l.operador_unidade")
              )
            }} as operador_unidade,
          from unnest(s.laudo) as l

          union all

          select as struct
            cast(null as string) as cid_id,
            cast(null as string) as cid_descricao,
            s.cancelamento.perfil as perfil_tipo,
            "Justificativa" as descricao_tipo,
            "CANCELADO" as situacao,
            s.cancelamento.justificativa as observacao,
            s.cancelamento.datahora as datahora_observacao,
            cast(null as string) as operador_unidade
          -- Queremos um UNION ALL condicional a `cancelamento.datahora IS NOT NULL`;
          -- mas "Query without FROM clause cannot have a WHERE clause";
          -- então fazemos um `FROM` de mentirinha lendo um array de 1 entrada
          from unnest([1])
          where s.cancelamento.datahora is not null
        )
        order by datahora_observacao asc
      ) as laudo,

      s.marcacao,

      array(
        select as struct
          e.profissional_nome,
          {{
            estabelecimento_remove_apendices(
              proper_estabelecimento("e.unidade_nome")
            )
          }} as unidade_nome,
          {{ proper_br("e.unidade_municipio") }} as unidade_municipio,
          {{ proper_br("e.unidade_bairro") }} as unidade_bairro,
        from unnest(s.execucao) as e
      ) as execucao,

      s.fonte,
      s.cpf_particao
    from {{ ref("mart_regulacao__solicitacao") }} as s
    where s.cpf_particao is not null
  ),

  suspeita_hiv as (
    select *
    from source
    -- `laudo` é um array de structs, então pra filtrar a linha inteira
    -- caso um elemento dentro dele seja 'proibido', usamos exists(), que
    -- retorna true se receber 1 ou mais linhas, e false caso contrário
    where not exists(
      select 1
      from unnest(laudo) as ld
      where 1=2  -- Queremos qualquer match, então `false OR ... OR ...`
      {% for field in [
        "procedimento_descricao",
        "ld.cid_id",
        "ld.observacao",
      ] %}
      or ifnull(
        regexp_contains(
          lower({{ field }}),
          -- B20-B24 são CIDs de HIV
          -- PVHIV - Pessoa Vivendo com HIV
          -- CD4 - Linfócito referência em testes de HIV
          -- TARV - Terapia Antirretroviral
          -- Tenofovir - Nome de medicamento antirretroviral
          r"\b(b2[0-4][\s\-\.]*[0-9]?|(pv)?hiv|aids|imuno[\s\-]*defici[eê]ncia|cd4|tarv|tenofovir)\b"
        ),
        false
      )
      {% endfor %}
    )
  )

select *
from suspeita_hiv
