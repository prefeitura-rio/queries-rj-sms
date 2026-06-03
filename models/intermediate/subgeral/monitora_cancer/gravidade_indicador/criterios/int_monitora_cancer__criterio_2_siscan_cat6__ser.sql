-- Critério 2 (cross-evento) — gatilho: mamografia SISCAN Categoria 6
-- (diagnóstico). Desfecho esperado: solicitação no SER (entrada na regulação
-- estadual; data_solicitacao). Folga: 5 dias.
-- Emite a relação canônica bruta de 8 colunas.
{% set criterio_2_intervalo = 5 %}
{% set criterio_2_peso = monitora_cancer_pesos_clinicos()[1] %}

with
    criterio_2_triggers as (
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
                starts_with(coalesce(mama_esquerda_resultado, ''), 'Categoria 6')
                or starts_with(coalesce(mama_direita_resultado, ''), 'Categoria 6')
            )
        group by cpf_particao, data_referencia_evento
    ),

    -- Desfecho esperado: uma SOLICITAÇÃO no SER (igual ao critério 7). Uma
    -- solicitação SER, ainda que pendente de autorização/execução, já
    -- satisfaz o desfecho — a paciente entrou na regulação estadual; cópia local para manter cada arquivo
    -- autossuficiente.
    criterio_2_desfecho_esperado as (
        select
            cpf_particao,
            data_solicitacao as data_expected
        from {{ ref("int_monitora_cancer__eventos_run_atual") }}
        where fonte = 'SER'
            and data_solicitacao is not null
    )

{{ monitora_cancer_criterio_cross_evento(
    criterio_label='SISCAN_MAMA_CAT_6__SER',
    intervalo_urgencia=criterio_2_intervalo,
    peso=criterio_2_peso,
    triggers_cte_name='criterio_2_triggers',
    desfecho_cte_name='criterio_2_desfecho_esperado'
) }}
