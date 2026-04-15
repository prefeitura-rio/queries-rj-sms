-- Eventos de procedimentos de mama extraídos do SER (regulação estadual internações)
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
    procedimento,

-- datas
    data_solicitacao,
    data_reserva as data_autorizacao,
    data_internacao_inicio as data_execucao,

-- resultados siscan (não aplicável)
    cast(NULL as date) as data_exame_resultado,
    cast(NULL as string) as mama_esquerda_resultado,
    cast(NULL as string) as mama_direita_resultado,

-- indicadores
    false as criterio_suspeita,
    false as criterio_diagnostico

from {{ ref("raw_ser_metabase__internacoes") }}
where 1 = 1
    and data_solicitacao >= "{{ data_inicio_monitoramento }}"
    and procedimento in (
        "DRENAGEM DE ABSCESSO DE MAMA",
        "SEGMENTECTOMIA/QUADRANTECTOMIA/SETORECTOMIA DE MAMA EM ONCOLOGIA",
        "MASTOIDECTOMIA RADICAL",
        "MASTOIDECTOMIA SUBTOTAL",
        "RESSECÇAO DE LESAO NAO PALPÁVEL DE MAMA COM MARCAÇAO EM ONCOLOGIA (POR MAMA)",
        "MAMOPLASTIA PÓS-CIRURGIA BARIÁTRICA"
    )
