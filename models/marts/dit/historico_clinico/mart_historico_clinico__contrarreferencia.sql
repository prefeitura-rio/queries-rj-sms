{{
    config(
        schema="saude_historico_clinico",
        alias="contrarreferencia",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with source as (
  select

    source_id,
    id_hci,

    struct(
      {{ proper_br("paciente_nome") }} as nome,
      paciente_nome_social as nome_social,
      paciente_cpf as cpf,
      paciente_cns as cns,
      safe_cast(paciente_data_nascimento as date) as data_nascimento,
      paciente_telefone as telefone,

      paciente_uf_naturalidade as uf_naturalidade,
      paciente_municipio_naturalidade as municipio_naturalidade,
      paciente_uf_residencia as uf_residencia,
      paciente_municipio_residencia as municipio_residencia
    ) as paciente,

    struct(
      id_cnes as id_cnes,
      unidade_nome as nome,
      unidade_uf as uf,
      unidade_municipio as municipio
    ) as estabelecimento,

    struct(
      {{ proper_br("profissional_nome") }} as nome,
      profissional_cpf as cpf,
      profissional_cns as cns,
      profissional_cargo as cargo
    ) as profissional,

    struct(
      id_documento,
      contrarreferencia_numero as numero,
      safe_cast(contrarreferencia_datahora as datetime) as datahora
    ) as contrarreferencia,

    struct(
      case
        when motivo = "Não foi registrada."
          then null
        else motivo
      end as motivo,
      impressao,
      resultados,
      conduta,
      conduta_seguimento as seguimento,
      resumo,
      encaminhamento
    ) as avaliacao,

    struct(
      cid_principal as cid,
      diagnostico_principal as descricao
    ) as diagnostico,

    flag_problema,
    flag_motivo_coincide,

    data_particao,
    safe_cast(paciente_cpf as int64) as cpf_particao

  from {{ ref("int_historico_clinico__contrarreferencia") }}
),

  flagged as (
    select
      source_id,
      id_hci,

      avaliacao.resumo,
      REGEXP_CONTAINS(avaliacao.resumo, r"\[HDA\]") as flag_tem_hda,
      REGEXP_CONTAINS(avaliacao.resumo, r"\[MEDICAMENTOS EM USO\]") as flag_tem_med,
      REGEXP_CONTAINS(avaliacao.resumo, r"\[HIPÓTESE DIAGNÓSTICA\]") as flag_tem_hip,
    from source
  ),

  sections as (
    select
      source_id,
      id_hci,

      -- /!\ A seleção inteira aqui presume a ordem de seções
      --     [HDA] -> [MEDICAMENTOS...] -> [HIPÓTESE...]
      case
        -- Se não temos nenhuma seção de interesse
        when not flag_tem_hda and not flag_tem_med and not flag_tem_hip
          then null
        -- Se só não temos [HDA], é possível que o médico tenha apagado do início
        -- Vamos considerar que o início do texto é HDA
        when not flag_tem_hda and flag_tem_med
          then trim(REGEXP_EXTRACT(
            resumo,
            r"^([^$]+)\[MEDICAMENTOS EM USO\]"
          ))
        when not flag_tem_hda and flag_tem_hip
          then trim(REGEXP_EXTRACT(
            resumo,
            r"^([^$]+)\[HIPÓTESE DIAGNÓSTICA\]"
          ))

        -- Caso contrário, se temos [HDA]...
        when flag_tem_med
          then trim(REGEXP_EXTRACT(
            resumo,
            r"\[HDA\]([^$]+)\[MEDICAMENTOS EM USO\]"
          ))
        when flag_tem_hip
          then trim(REGEXP_EXTRACT(
            resumo,
            r"^\[HDA\]([^$]+)\[HIPÓTESE DIAGNÓSTICA\]"
          ))

        -- Se chegamos aqui, só temos [HDA]; devolve o resumo inteiro
        else trim(REGEXP_REPLACE(resumo, r"\[HDA\]", ""))
      end as hda,

      case
        -- Se não temos medicamentos
        when not flag_tem_med
          then null
        -- Se temos hipótese
        when flag_tem_med
          then trim(REGEXP_EXTRACT(
            resumo,
            r"\[MEDICAMENTOS EM USO\]([^$]+)\[HIPÓTESE DIAGNÓSTICA\]"
          ))
        -- Senão, retorna tudo que vier depois
        else trim(REGEXP_EXTRACT(
            resumo,
            r"\[MEDICAMENTOS EM USO\]([^$]+)"
          ))
      end as meds,

      case
        -- Se não temos hipótese
        when not flag_tem_hip
          then null
        -- Senão, retorna tudo que vier depois
        else trim(REGEXP_EXTRACT(
          resumo,
          r"\[HIPÓTESE DIAGNÓSTICA\]([^$]+)"
        ))
      end as hipotese_diagnostica

    from flagged
  ),

  sections_cleaned as (
    select
      * except (hda, meds, hipotese_diagnostica),

      -- Como extraímos o campo de dentro de um texto maior,
      -- essas colunas agora podem ser só "-"; então damos
      -- uma última limpada
      case
        when REGEXP_CONTAINS(
            lower(hda),
            r"^[\-\.\s;,/=_\?\]'´`x]+$"
          )
          then null
        when lower(hda) in (
          "sem hda"
        )
          then null
        else REGEXP_REPLACE(
          {{ process_null("hda") }},
          r"\n{3,}",
          "\n\n"
        )
      end as hda,

      case
        -- Às vezes é repetição de HDA
        when REGEXP_REPLACE(NORMALIZE(upper(hda), NFD), r"[^A-Z]", "")
          = REGEXP_REPLACE(NORMALIZE(upper(meds), NFD), r"[^A-Z]", "")
          then null

        when REGEXP_CONTAINS(
            lower(meds),
            r"^[\-\.\s;,/=_\?\]'´`x]+$"
          )
          then null

        when REGEXP_REPLACE(NORMALIZE(upper(meds), NFD), r"[^A-Z]", "") in (
          "", "N", "NAO", "NAOSABE",
          "NAOUSAATUALMENTE",
          "NADA", "NEGA", "NEGO", "NEGAUSO",
          "NEGAUSODIARIODEMEDICACAO",
          "NEGAUSOREGULARDEMEDICACOES",
          "SEM", "NDD",
          "ACIMA", "VIDEACIMA", "JADESCRITO",
          "ESQUECEU",
          "NENHUM", "NENHUMA",
          "NENHM", "NEHUM", "NEHUMA"
        )
          then null

        else REGEXP_REPLACE(
          {{ process_null("meds") }},
          r"\n{3,}",
          "\n\n"
        )
      end as meds,

      case
        when REGEXP_REPLACE(NORMALIZE(upper(hda), NFD), r"[^A-Z]", "")
          = REGEXP_REPLACE(NORMALIZE(upper(hipotese_diagnostica), NFD), r"[^A-Z]", "")
          then null

        when REGEXP_CONTAINS(
            lower(hipotese_diagnostica),
            r"^[\-\.\s;,/=_\?\]'´`x]+$"
          )
          then null

        else REGEXP_REPLACE(
          {{ process_null("hipotese_diagnostica") }},
          r"\n{3,}",
          "\n\n"
        )
      end as hipotese_diagnostica

    from sections
  ),

  joined as (
    select

      src.id_hci,

      src.paciente,
      src.estabelecimento,
      src.profissional,
      src.contrarreferencia,

      struct(
        src.avaliacao.motivo,
        src.avaliacao.impressao,
        src.avaliacao.resultados,
        src.avaliacao.conduta,
        src.avaliacao.seguimento,

        src.avaliacao.resumo,
        sec.hda as historia_doenca_atual,
        sec.meds as medicamentos_em_uso,
        sec.hipotese_diagnostica,

        src.avaliacao.encaminhamento
      ) as avaliacao,

      src.diagnostico,

      src.flag_problema,
      src.flag_motivo_coincide,

      src.data_particao,
      src.cpf_particao

    from source as src
    left join sections_cleaned as sec
      using(id_hci, source_id)
  )

select *
from joined
