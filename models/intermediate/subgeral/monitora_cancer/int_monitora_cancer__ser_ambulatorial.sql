-- Eventos de procedimentos de mama extraídos do SER (regulação estadual ambulatorial)
{% set data_inicio_monitoramento = "2021-01-01" %}

select
-- pk
    "SER" as sistema_origem,
    "REGULACAO" as sistema_tipo,
    id_solicitacao as id_sistema_origem,

-- paciente
    paciente_cns,
    cast(NULL as int) as paciente_cpf_sisreg,

-- unidades
    id_cnes_unidade_origem,
    id_cnes_unidade_executante,

-- qualificacao
    cid,
    cast(NULL as string) as evento_status,
    coalesce(procedimento_regulado, procedimento_solicitado) as procedimento,

-- datas
    data_solicitacao,
    data_agendamento as data_autorizacao,
    data_execucao,

-- resultados siscan (não aplicável)
    cast(NULL as date) as data_exame_resultado,
    cast(NULL as string) as mama_esquerda_resultado,
    cast(NULL as string) as mama_direita_resultado,

-- indicadores
    false as criterio_suspeita,
    case
        when
        procedimento_solicitado in (
            "AMBULATÓRIO 1ª VEZ - MASTOLOGIA (ONCOLOGIA)"
        )
        or
        procedimento_regulado in (
            "AMBULATÓRIO 1ª VEZ - MASTOLOGIA (ONCOLOGIA)"
        )
        then true
        else false
    end as criterio_diagnostico

from {{ ref("raw_ser_metabase__ambulatorial") }}
where 1 = 1
    and data_solicitacao >= "{{ data_inicio_monitoramento }}"
    and (
        procedimento_solicitado in (
            "AMBULATÓRIO 1ª VEZ - MASTOLOGIA (ONCOLOGIA)",
            "RESSONÂNCIA MAGNÉTICA DE MAMA",
            "BIÓPSIA DE MAMA GUIADA POR USG",
            "AMBULATÓRIO 1ª VEZ EM CIRURGIA PLÁSTICA REPARADORA - MAMA (ONCOLOGIA)",
            "MAMOGRAFIA BILATERAL",
            "BIÓPSIA DE MAMA POR ULTRASSONOGRAFIA",
            "BIÓPSIA GUIADA POR MAMOGRAFIA",
            "CORE BIOPSIA DE MAMA",
            "RESSONÂNCIA MAGNÉTICA DE MAMA-ONCOLOGIA",
            "ULTRA-SONOGRAFIA DOPPLER DE MAMAS",
            "ULTRASSONOGRAFIA DE MAMA (FEMININA E MASCULINA)",
            "MASTOLOGIA (RETORNO)",
            "CREG BL AMBULATÓRIO 1ª VEZ - MASTOLOGIA",
            "MAMOGRAFIA - BILATERAL",
            "BIOPSIA DE MAMA POR PAAF",
            "AMBULATÓRIO 1ª VEZ - MASTOLOGIA",
            "AMBULATÓRIO 1ª VEZ EM MASTOLOGIA - LESÃO IMPALPÁVEL (ONCOLOGIA)",
            "CONSULTA EM MASTOLOGIA",
            "MAMOGRAFIA DE RASTREIO",
            "ULTRASSONOGRAFIA - MAMAS",
            "BIÓPSIA DE MAMA POR ESTEREOTAXIA / MAMOTOMIA",
            "CONSULTA EM GINECOLOGIA - MASTOLOGIA",
            "BIÓPSIA DE MAMA - LESÃO PALPÁVEL",
            "ULTRASSONOGRAFIA DE MAMA COM DOPPLER",
            "CORE BIÓPSIA DE MAMA",
            "PROCEDIMENTOS DIAGNÓSTICOS GUIADOS POR USG (MAMA) (DESATIVADO)",
            "ULTRASSONOGRAFIA DE MAMAS BILATERAL",
            "BIÓPSIA DE MAMA POR ULTRASSONOGRAFIA"
        ) or
        procedimento_regulado in (
            "AMBULATÓRIO 1ª VEZ - MASTOLOGIA (ONCOLOGIA)",
            "RESSONÂNCIA MAGNÉTICA DE MAMA",
            "BIÓPSIA DE MAMA GUIADA POR USG",
            "AMBULATÓRIO 1ª VEZ EM CIRURGIA PLÁSTICA REPARADORA - MAMA (ONCOLOGIA)",
            "MAMOGRAFIA BILATERAL",
            "BIÓPSIA DE MAMA POR ULTRASSONOGRAFIA",
            "BIÓPSIA GUIADA POR MAMOGRAFIA",
            "CORE BIOPSIA DE MAMA",
            "RESSONÂNCIA MAGNÉTICA DE MAMA-ONCOLOGIA",
            "ULTRA-SONOGRAFIA DOPPLER DE MAMAS",
            "ULTRASSONOGRAFIA DE MAMA (FEMININA E MASCULINA)",
            "MASTOLOGIA (RETORNO)",
            "CREG BL AMBULATÓRIO 1ª VEZ - MASTOLOGIA",
            "MAMOGRAFIA - BILATERAL",
            "BIOPSIA DE MAMA POR PAAF",
            "AMBULATÓRIO 1ª VEZ - MASTOLOGIA",
            "AMBULATÓRIO 1ª VEZ EM MASTOLOGIA - LESÃO IMPALPÁVEL (ONCOLOGIA)",
            "CONSULTA EM MASTOLOGIA",
            "MAMOGRAFIA DE RASTREIO",
            "ULTRASSONOGRAFIA - MAMAS",
            "BIÓPSIA DE MAMA POR ESTEREOTAXIA / MAMOTOMIA",
            "CONSULTA EM GINECOLOGIA - MASTOLOGIA",
            "BIÓPSIA DE MAMA - LESÃO PALPÁVEL",
            "ULTRASSONOGRAFIA DE MAMA COM DOPPLER",
            "CORE BIÓPSIA DE MAMA",
            "PROCEDIMENTOS DIAGNÓSTICOS GUIADOS POR USG (MAMA) (DESATIVADO)",
            "ULTRASSONOGRAFIA DE MAMAS BILATERAL",
            "BIÓPSIA DE MAMA POR ULTRASSONOGRAFIA"
        )
    )
