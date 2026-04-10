-- Laudos radiológicos (mamografia) extraídos do SISCAN
{% set data_inicio_monitoramento = "2021-01-01" %}

select
-- pk
    "SISCAN" as sistema_origem,
    "EXAME" as sistema_tipo,
    safe_cast(protocolo_id as int) as id_sistema_origem,

-- paciente
    paciente_cns,
    cast(NULL as int) as paciente_cpf_sisreg,

-- unidades
    unidade_solicitante_id_cnes as id_cnes_unidade_origem,
    unidade_prestadora_id_cnes as id_cnes_unidade_executante,

-- qualificacao
    cast(NULL as string) as cid,
    cast(NULL as string) as evento_status,
    case
        when mamografia_tipo = "Rastreamento" then "RESULTADO MAMOGRAFIA DE RASTREIO"
        when mamografia_tipo = "Diagnóstica" then "RESULTADO MAMOGRAFIA DIAGNOSTICA"
        else null
    end as procedimento,

-- datas
    data_solicitacao,
    cast(NULL as date) as data_autorizacao,
    data_realizacao as data_execucao,

-- resultados siscan
    data_liberacao_resultado as data_exame_resultado,
    mama_esquerda_classif_radiologica as mama_esquerda_resultado,
    mama_direita_classif_radiologica as mama_direita_resultado,

-- indicadores
    case
        when
        mama_esquerda_classif_radiologica in (
            "Categoria 4 - achados mamográficos suspeitos",
            "Categoria 5 - achados mamográficos altamente suspeitos"
        )
        or
        mama_direita_classif_radiologica in (
            "Categoria 4 - achados mamográficos suspeitos",
            "Categoria 5 - achados mamográficos altamente suspeitos"
        )
        then true
        else false
    end as criterio_suspeita,

    case
        when
        mama_esquerda_classif_radiologica = "Categoria 6 - achados mamográficos"
        or
        mama_direita_classif_radiologica = "Categoria 6 - achados mamográficos"
        then true
        else false
    end as criterio_diagnostico

from {{ ref("raw_siscan_web__laudos") }}
where 1 = 1
    and data_solicitacao >= "{{ data_inicio_monitoramento }}"
