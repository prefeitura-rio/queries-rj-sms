-- Critério 1 (cross-evento) — gatilho: mamografia SISCAN Categoria 0/4/5
-- (suspeita). Desfecho esperado: ultrassonografia ou biópsia no SISREG.
-- Enquanto não houver desfecho >= data do gatilho, o critério fica ativo.
-- Folga: 10 dias. Emite a relação canônica bruta de 8 colunas.
{% set criterio_1_intervalo = 10 %}
{% set criterio_1_peso = monitora_cancer_pesos_clinicos()[0] %}

with
    -- Dedup por (cpf, data_trigger) com max(risco): múltiplos eventos SISCAN
    -- no mesmo dia colapsam em um único gatilho, mantendo o pior risco.
    criterio_1_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger,
            max(risco) as risco_evento_gatilho
        from {{ ref("int_monitora_cancer__eventos_run_atual") }}
        where fonte = 'SISCAN'
            and procedimento in (
                'RESULTADO MAMOGRAFIA DE RASTREIO',
                'RESULTADO MAMOGRAFIA DIAGNOSTICA'
            )
            and (
                starts_with(coalesce(mama_esquerda_resultado, ''), 'Categoria 0')
                or starts_with(coalesce(mama_esquerda_resultado, ''), 'Categoria 4')
                or starts_with(coalesce(mama_esquerda_resultado, ''), 'Categoria 5')
                or starts_with(coalesce(mama_direita_resultado, ''), 'Categoria 0')
                or starts_with(coalesce(mama_direita_resultado, ''), 'Categoria 4')
                or starts_with(coalesce(mama_direita_resultado, ''), 'Categoria 5')
            )
        group by cpf_particao, data_referencia_evento
    ),

    criterio_1_desfecho_esperado as (
        -- Match nos rótulos definidos em int_monitora_cancer__parametros_sisreg:
        -- 'USG' é a abreviação usada para ultrassonografia (o seed não usa
        -- 'ULTRA' nem 'ULTRASSONOGRAFIA'). CONTAINS_SUBSTR normaliza case via
        -- NFKC mas preserva diacríticos, por isso 'BIOPSIA' e 'BIÓPSIA' são
        -- testados separadamente para cobrir 'Biópsia' (acentuado) e
        -- 'Biopsia' (sem acento, em 'USG de Mamas - para Biopsia').
        select
            cpf_particao,
            data_expected
        from {{ ref("int_monitora_cancer__eventos_run_atual") }}
        where fonte = 'SISREG'
            and (
                contains_substr(procedimento, 'USG')
                or contains_substr(procedimento, 'BIOPSIA')
                or contains_substr(procedimento, 'BIÓPSIA')
            )
            and data_expected is not null
    )

{{ monitora_cancer_criterio_cross_evento(
    criterio_label='SISCAN_MAMA_CAT_0_4_5__SISREG_ULTRA_OU_BIOPSIA',
    intervalo_urgencia=criterio_1_intervalo,
    peso=criterio_1_peso,
    triggers_cte_name='criterio_1_triggers',
    desfecho_cte_name='criterio_1_desfecho_esperado'
) }}
