-- noqa: disable=LT08

-- ════════════════════════════════════════════════════════════════════════════
-- PARÂMETROS GLOBAIS DO FATOR DE RISCO
-- ════════════════════════════════════════════════════════════════════════════
-- Os três entram no cálculo do fator_risco "snapshot" (cache nas colunas
-- fator_risco/gravidade_criterio do select final). Para análise de
-- sensibilidade, recompute em Python a partir de risco_evento_gatilho cru —
-- não use os caches.
--
-- amortecedor_risco: quanto NÃO se confia no risco bruto reportado pelos
-- sistemas externos. A "confiança no risco bruto" é
-- risco_maximo_escala / (risco_maximo_escala + amortecedor_risco). Com o
-- valor atual (1), a confiança é 4/5 = 80% — risco 4 vale 2.5× o risco 1
-- (em vez de 4× sem amortecimento). risco_padrao_quando_nulo: tratamento
-- para risco NULL (mediana 2).
{% set risco_maximo_escala = 4 %}
{% set risco_padrao_quando_nulo = 2 %}
{% set amortecedor_risco = 1 %}

-- ════════════════════════════════════════════════════════════════════════════
-- INSTÂNCIAS DE CRITÉRIO ATIVAS — AGREGADOR (granularidade fina)
-- ════════════════════════════════════════════════════════════════════════════
--
-- Uma linha por (cpf_particao, criterio, etapa, data_trigger) ativa na jornada
-- atual. "Ativa" = gatilho disparou AND desfecho esperado ainda não chegou
-- AND dias_atraso > 0.
--
-- ESTE ARQUIVO É O AGREGADOR. A lógica de gatilho/desfecho/folga de cada
-- critério vive em 1 arquivo por critério em criterios/ (ver
-- criterios/README.md); cada um é ephemeral e emite a mesma relação canônica
-- bruta de 8 colunas:
--   (cpf_particao, criterio, etapa, data_trigger, dias_atraso,
--    intervalo_urgencia_dias, risco_evento_gatilho, peso_criterio)
-- Aqui apenas: (a) UNION ALL dos 7 critérios; (b) aplica a fórmula Eq. (1)
-- expondo fator_tempo, fator_risco, gravidade_criterio; (c) JOIN de gestante;
-- (d) filtro de folga dias_atraso > 0.
--
-- Os eventos da jornada atual (com data_expected) vêm de
-- int_monitora_cancer__eventos_run_atual (ephemeral compartilhado pelos 7
-- critérios e pela CTE gestantes abaixo — o dbt o injeta uma única vez).
--
-- Para o score composto (colapso, agregação, multiplicador de gestante,
-- reescala 0-100 com teto dinâmico), ver int_monitora_cancer__gravidade.sql.

-- Materializado como tabela: (a) compartilhado entre gravidade (score final)
-- e mart_monitora_cancer__gravidade_instancias (consumo analítico), evita
-- recomputação; (b) clusterização por `criterio` acelera filtros analíticos.
{{
    config(
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
        cluster_by=["criterio"],
        on_schema_change="sync_all_columns",
    )
}}

with
    -- UNION das instâncias brutas dos 7 critérios (antes do filtro de folga).
    instancias_brutas as (
        select * from {{ ref("int_monitora_cancer__criterio_1_siscan_cat045__sisreg") }}
        union all
        select * from {{ ref("int_monitora_cancer__criterio_2_siscan_cat6__ser") }}
        union all
        select * from {{ ref("int_monitora_cancer__criterio_3_biopsia_neopl__ser") }}
        union all
        select * from {{ ref("int_monitora_cancer__criterio_4_sisreg_biopsia__intra") }}
        union all
        select * from {{ ref("int_monitora_cancer__criterio_5_ser_pendente") }}
        union all
        select * from {{ ref("int_monitora_cancer__criterio_6_ser_em_fila") }}
        union all
        select * from {{ ref("int_monitora_cancer__criterio_7_ser_falha__nova_ser") }}
    ),

    -- gestante por paciente (broadcasted), para JOIN single-pass.
    gestantes as (
        select distinct
            cpf_particao,
            gestante
        from {{ ref("int_monitora_cancer__eventos_run_atual") }}
    )

select
    i.cpf_particao,
    i.criterio,
    i.etapa,
    i.data_trigger,
    i.dias_atraso,
    i.intervalo_urgencia_dias,
    i.risco_evento_gatilho,
    i.peso_criterio,

    -- Componentes da Eq. (1) — expostos como colunas para audit e para
    -- recomputo simples em Python.
    i.dias_atraso / i.intervalo_urgencia_dias as fator_tempo,

    -- fator_risco com os parâmetros vigentes. Para análise de sensibilidade
    -- (variar amortecedor_risco), recompute em Python a partir de
    -- risco_evento_gatilho cru — não use esta coluna.
    {{ monitora_cancer_fator_risco(
        'i.risco_evento_gatilho',
        amortecedor_risco,
        risco_maximo_escala,
        risco_padrao_quando_nulo
    ) }} as fator_risco,

    -- gravidade_criterio com os parâmetros vigentes (cache).
    {{ monitora_cancer_gravidade_criterio(
        'i.dias_atraso',
        'i.intervalo_urgencia_dias',
        'i.risco_evento_gatilho',
        amortecedor_risco,
        risco_maximo_escala,
        risco_padrao_quando_nulo
    ) }} as gravidade_criterio,

    coalesce(g.gestante, false) as gestante
from instancias_brutas as i
    left join gestantes as g using (cpf_particao)
where i.dias_atraso > 0
