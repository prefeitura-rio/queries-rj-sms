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
    end as criterio_diagnostico,

-- apenas para SER/SISREG
    cast(NULL as int64) as atraso_solicitacao_autorizacao,
    cast(NULL as int64) as atraso_autorizacao_execucao,
    cast(NULL as int64) as atraso_regulacao,

-- risco (float64): derivado de três fontes do laudo SISCAN, combinadas pelo MAIOR risco.
-- 1) Categoria BI-RADS de cada mama:
--    Cat 1/2 = 1.0 (sem achados / benignos); Cat 0/3 = 2.0 (avaliação adicional / provavelmente benignos);
--    Cat 4 = 3.0 (achados suspeitos); Cat 5/6 = 4.0 (altamente suspeitos / com diagnóstico).
-- 2) Tipo de rastreamento (mamografia_rastreamento_tipo):
--    "População de risco elevado (história familiar)" = 4.0;
--    "Paciente já tratado de câncer de mama" = 3.0;
--    "População alvo" = 1.0; demais = NULL.
-- NULL se nenhuma das fontes casar com o mapeamento.
    (
        select max(r) from unnest([
            case safe_cast(regexp_extract(mama_esquerda_classif_radiologica, r'Categoria (\d+)') as int64)
                when 1 then 1.0 when 2 then 1.0
                when 0 then 2.0 when 3 then 2.0
                when 4 then 3.0
                when 5 then 4.0 when 6 then 4.0
                else null
            end,
            case safe_cast(regexp_extract(mama_direita_classif_radiologica, r'Categoria (\d+)') as int64)
                when 1 then 1.0 when 2 then 1.0
                when 0 then 2.0 when 3 then 2.0
                when 4 then 3.0
                when 5 then 4.0 when 6 then 4.0
                else null
            end,
            case mamografia_rastreamento_tipo
                when 'População de risco elevado (história familiar)' then 4.0
                when 'Paciente já tratado de câncer de mama' then 3.0
                when 'População alvo' then 1.0
                else null
            end
        ]) as r
    ) as risco

from {{ ref("raw_siscan_web__laudos") }}
where 1 = 1
    and data_solicitacao >= "{{ data_inicio_monitoramento }}"
