{% macro monitora_cancer_criterio_cross_evento(
    criterio_label,
    intervalo_urgencia,
    peso,
    triggers_cte_name,
    desfecho_cte_name,
    desfecho_strict=false
) %}
        select
            t.cpf_particao,
            '{{ criterio_label }}' as criterio,
            cast(null as string) as etapa,
            t.data_trigger,
            {{ monitora_cancer_dias_atraso('t.data_trigger', intervalo_urgencia) }} as dias_atraso,
            {{ intervalo_urgencia }} as intervalo_urgencia_dias,
            t.risco_evento_gatilho,
            {{ peso }} as peso_criterio
        from {{ triggers_cte_name }} as t
        where not exists (
            select 1 from {{ desfecho_cte_name }} as e
            where t.cpf_particao = e.cpf_particao
                and e.data_expected {{ '>' if desfecho_strict else '>=' }} t.data_trigger
        )
{% endmacro %}


/*
    Instância bruta de um critério CROSS-EVENTO do score de gravidade do
    monitora_cancer (critérios 1, 2, 3, 7).

    Emite a relação canônica bruta de 8 colunas (ANTES da fórmula Eq. 1,
    aplicada depois do UNION no select final de
    int_monitora_cancer__gravidade_instancias):
      cpf_particao, criterio, etapa (sempre NULL em cross-evento),
      data_trigger, dias_atraso, intervalo_urgencia_dias,
      risco_evento_gatilho, peso_criterio.

    Mecanismo de desativação: anti-join NOT EXISTS contra a CTE de desfecho
    esperado — o critério some quando aparece um desfecho com
    data_expected >= data_trigger (ou > data_trigger se desfecho_strict).

    Parâmetros:
      • criterio_label     — string; valor da coluna `criterio`.
      • intervalo_urgencia — int; folga clínica. Divisor do fator de tempo
                             e valor de intervalo_urgencia_dias. Tipicamente
                             a variável Jinja criterio_N_intervalo.
      • peso               — float; peso clínico (criterio_N_peso, lido de
                             monitora_cancer_pesos_clinicos).
      • triggers_cte_name  — string; nome da CTE LOCAL com
                             (cpf_particao, data_trigger, risco_evento_gatilho).
      • desfecho_cte_name  — string; nome da CTE LOCAL com
                             (cpf_particao, data_expected).
      • desfecho_strict    — bool; default false. true ⇒ `>` no anti-join
                             (caso do critério 7); senão `>=`.

    Recebe NOMES de CTEs locais ao modelo (não ref()), expandidos como
    identificadores no FROM/anti-join. dias_atraso usa
    monitora_cancer_dias_atraso (piso 0).
*/
