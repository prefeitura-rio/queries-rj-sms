-- Critério 7 (cross-evento) — gatilho: solicitação SER que falhou
-- (CHEGADA_NAO_CONFIRMADA, CANCELADA). Desfecho esperado: uma NOVA
-- solicitação SER. Comparação ESTRITA (> data do gatilho): só a solicitação
-- posterior à falha desativa. Folga: 10 dias. Relação canônica de 8 colunas.
{% set criterio_7_intervalo = 10 %}
{% set criterio_7_peso = monitora_cancer_pesos_clinicos()[6] %}

with
    criterio_7_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger,
            max(risco) as risco_evento_gatilho
        from {{ ref("int_monitora_cancer__eventos_run_atual") }}
        where fonte = 'SER'
            and evento_status in ('CHEGADA_NAO_CONFIRMADA', 'CANCELADA')
        group by cpf_particao, data_referencia_evento
    ),

    criterio_7_desfecho_esperado as (
        select
            cpf_particao,
            data_solicitacao as data_expected
        from {{ ref("int_monitora_cancer__eventos_run_atual") }}
        where fonte = 'SER'
            and data_solicitacao is not null
    )

{{ monitora_cancer_criterio_cross_evento(
    criterio_label='SER_FALHA__NOVA_SER',
    intervalo_urgencia=criterio_7_intervalo,
    peso=criterio_7_peso,
    triggers_cte_name='criterio_7_triggers',
    desfecho_cte_name='criterio_7_desfecho_esperado',
    desfecho_strict=true
) }}
