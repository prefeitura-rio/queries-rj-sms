{{
  config(
    schema="app_historico_clinico",
    alias="regulacao",
    materialized="table",
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
      s.cancelamento.datahora as cancelamento_datahora,

      s.solicitacao.detalhe_tipo,
      s.solicitacao.detalhe_status,
      s.solicitacao.detalhe_responsavel,

      s.solicitacao.data_desejada,
      {{
        estabelecimento_remove_apendices(
          proper_estabelecimento("s.solicitacao.unidade_desejada_nome")
        )
      }} as unidade_desejada,

      s.procedimento.sigtap_id,
      case 
        when
          regexp_contains(
            s.procedimento.sigtap_descricao,
            R"(?i)ACOMPANHAMENTO\s*E\s*AVALIAÇÃO\s*DOMICILIAR\s*DE\s*PACIENTE\s*SUBMETIDO\s*À\s*VENTILAÇÃO\s*MECANICA"
          )
          then "Avaliação domiciliar de paciente em ventilação mecânica"
        else
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  {{ proper_br("s.procedimento.sigtap_descricao") }},
                  R"(?i)\b(RADIOGRAFIA|RAIO[\-\s]*X)\b",
                  "RX"
                ),
                R"(?i)\bRESSON[AÂ]NCIA\s*MAGN[EÉ]TICA\b",
                "RM"
              ),
              R"(?i)\bTOMOGRAFIA\s*COMPUTADORIZADA\b",
              "TC"
            ),
            R"(?i)ACIDENTE\s*VASCULAR\s*CEREBRAL\s*-?\s*AVC",
            "AVC"
          )
      end as sigtap_descricao,

      {{
        estabelecimento_remove_apendices(
          proper_estabelecimento("s.solicitante.unidade_nome")
        )
      }} as unidade_solicitante,
      s.solicitante.profissional_nome as profissional_solicitante,

      array(
        select as struct
          * except (operador_unidade),
          {{
            estabelecimento_remove_apendices(
              proper_estabelecimento("l.operador_unidade")
            )
          }} as operador_unidade,
        from unnest(s.laudo) as l
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
  )

select *
from source
