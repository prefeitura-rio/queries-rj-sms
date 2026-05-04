-- Eventos de procedimentos de mama extraídos do SISREG (regulação ambulatorial)
{% set data_inicio_monitoramento = "2021-01-01" %}

select
-- pk
    "SISREG" as sistema_origem,
    "REGULACAO" as sistema_tipo,
    safe_cast(id_solicitacao as int) as id_sistema_origem,

-- paciente
    paciente_cns,
    safe_cast(paciente_cpf as int) as paciente_cpf_sisreg,

-- unidades
    id_cnes_unidade_solicitante as id_cnes_unidade_origem,
    id_cnes_unidade_executante,

-- qualificacao
    cid_solicitacao as cid,
    solicitacao_status as evento_status,
    procedimento,

-- datas
    safe_cast(data_solicitacao as date) as data_solicitacao,
    safe_cast(data_autorizacao as date) as data_autorizacao,
    safe_cast(data_execucao as date) as data_execucao,

-- resultados siscan (não aplicável)
    cast(NULL as date) as data_exame_resultado,
    cast(NULL as string) as mama_esquerda_resultado,
    cast(NULL as string) as mama_direita_resultado,

-- indicadores
    case
        when procedimento in (
            "MAMOGRAFIA  DIAGNOSTICA",
            "BIÓPSIA DE MAMA - LESÃO PALPÁVEL",
            "BIOPSIA DE MAMA GUIADA POR USG",
            "BIOPSIA DE MAMA POR ESTEREOTAXIA",
            "ULTRASSONOGRAFIA MAMARIA BILATERAL PARA ORIENTAR BIOPSIA DE MAMA"
        ) then true
        else false
    end as criterio_suspeita,
    false as criterio_diagnostico

from {{ ref("mart_sisreg__solicitacoes") }}
where 1 = 1
    and data_solicitacao >= "{{ data_inicio_monitoramento }}"
    and procedimento in (
        "MAMOGRAFIA BILATERAL",
        "MAMOGRAFIA  DIAGNOSTICA",
        "CONSULTA EM MASTOLOGIA",
        "CONSULTA EM GINECOLOGIA - MASTOLOGIA",
        "CONSULTA EM CIRURGIA PLASTICA - REPARADORA - MAMA",
        "BIÓPSIA DE MAMA - LESÃO PALPÁVEL",
        "BIOPSIA DE MAMA GUIADA POR USG",
        "BIOPSIA DE MAMA POR ESTEREOTAXIA",
        "ULTRASSONOGRAFIA DE MAMAS BILATERAL",
        "ULTRA-SONOGRAFIA DE MAMAS  BILATERAL",
        "ULTRA-SONOGRAFIA  DE MAMAS (BILATERAL) - PEDIATRICA",
        "ULTRA-SONOGRAFIA DOPPLER DE MAMAS",
        "ULTRASSONOGRAFIA MAMARIA BILATERAL PARA ORIENTAR BIOPSIA DE MAMA",
        "RESSONANCIA MAGNETICA DE MAMA (BILATERAL)",
        "RESSONANCIA MAGNETICA DE MAMA ESQUERDA",
        "RESSONANCIA MAGNETICA DE MAMA DIREITA"
    )
