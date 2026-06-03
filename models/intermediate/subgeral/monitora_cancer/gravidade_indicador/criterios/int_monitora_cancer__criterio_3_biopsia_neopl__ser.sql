-- Critério 3 (cross-evento) — gatilho: biópsia SISCAN com lesão neoplásica
-- (criterio_diagnostico em laudo histopatológico = lesao_neoplasico is not
-- null, equivalente exato). Desfecho esperado: solicitação no SER (entrada na
-- regulação estadual; data_solicitacao). Folga: 5 dias. Emite a relação
-- canônica bruta de 8 colunas.
{% set criterio_3_intervalo = 5 %}
{% set criterio_3_peso = monitora_cancer_pesos_clinicos()[2] %}

with
    criterio_3_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger,
            max(risco) as risco_evento_gatilho
        from {{ ref("int_monitora_cancer__eventos_run_atual") }}
        where fonte = 'SISCAN'
            and procedimento not in (
                'RESULTADO MAMOGRAFIA DE RASTREIO',
                'RESULTADO MAMOGRAFIA DIAGNOSTICA'
            )
            and criterio_diagnostico = true
        group by cpf_particao, data_referencia_evento
    ),

    -- Desfecho esperado: uma SOLICITAÇÃO no SER (igual ao critério 7). Uma
    -- solicitação SER, ainda que pendente de autorização/execução, já
    -- satisfaz o desfecho — a paciente entrou na regulação estadual; cópia local para manter cada arquivo
    -- autossuficiente.
    criterio_3_desfecho_esperado as (
        select
            cpf_particao,
            data_solicitacao as data_expected
        from {{ ref("int_monitora_cancer__eventos_run_atual") }}
        where fonte = 'SER'
            and data_solicitacao is not null
    )

{{ monitora_cancer_criterio_cross_evento(
    criterio_label='SISCAN_BIOPSIA_NEOPLASICA__SER',
    intervalo_urgencia=criterio_3_intervalo,
    peso=criterio_3_peso,
    triggers_cte_name='criterio_3_triggers',
    desfecho_cte_name='criterio_3_desfecho_esperado'
) }}
