-- noqa: disable=LT08

-- ════════════════════════════════════════════════════════════════════════════
-- PARÂMETROS
-- ════════════════════════════════════════════════════════════════════════════

-- ── Parâmetros por critério ─────────────────────────────────────────────────
-- intervalo_urgencia: folga clinicamente tolerável; também divisor do fator
-- de tempo. peso_criterio: importância relativa do critério (lido da macro
-- monitora_cancer_pesos_clinicos — fonte única do projeto; o valor é
-- emitido na coluna peso_criterio de cada linha e consumido downstream em
-- int_monitora_cancer__gravidade).
{% set pesos_clinicos = monitora_cancer_pesos_clinicos() %}

--{# Critério 1 — SISCAN mama Cat 0/4/5 → SISREG ultra/biópsia #}
{% set criterio_1_intervalo = 10 %}
{% set criterio_1_peso = pesos_clinicos[0] %}

--{# Critério 2 — SISCAN mama Cat 6 → qualquer evento SER #}
{% set criterio_2_intervalo = 5 %}
{% set criterio_2_peso = pesos_clinicos[1] %}

--{# Critério 3 — SISCAN biópsia neoplásica → qualquer evento SER #}
{% set criterio_3_intervalo = 5 %}
{% set criterio_3_peso = pesos_clinicos[2] %}

--{# Critério 4 — SISREG biópsia → progresso intra-evento (2 legs) #}
{% set criterio_4_intervalo = 20 %}
{% set criterio_4_peso = pesos_clinicos[3] %}

--{# Critério 5 — SER status "PENDENTE" → atualização do status #}
{% set criterio_5_intervalo = 10 %}
{% set criterio_5_peso = pesos_clinicos[4] %}

--{# Critério 6 — SER status "EM_FILA" → atualização do status #}
{% set criterio_6_intervalo = 60 %}
{% set criterio_6_peso = pesos_clinicos[5] %}

--{# Critério 7 — SER status de falha → nova solicitação SER #}
{% set criterio_7_intervalo = 10 %}
{% set criterio_7_peso = pesos_clinicos[6] %}

-- ── Parâmetros globais do fator de risco ──────────────────────────────────
-- Os três entram no cálculo do fator_risco "snapshot" (cache nas colunas
-- fator_risco/gravidade_criterio). Para análise de sensibilidade, recompute
-- em Python a partir de risco_evento_gatilho cru — não use os caches.
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
-- INSTÂNCIAS DE CRITÉRIO ATIVAS — granularidade fina
-- ════════════════════════════════════════════════════════════════════════════
--
-- Uma linha por (cpf_particao, criterio, etapa, data_trigger) ativa no run
-- atual. "Ativa" = gatilho disparou AND desfecho esperado ainda não chegou
-- AND dias_atraso > 0. As 7 CTEs criterio_N constroem as instâncias por
-- critério; a CTE final aplica a fórmula Eq. (1) e expõe os componentes em
-- colunas separadas (fator_tempo, fator_risco, gravidade_criterio).
--
-- ESTE É O ÚNICO LUGAR EM QUE A LÓGICA DE GATILHO/DESFECHO/FOLGA POR
-- CRITÉRIO VIVE — int_monitora_cancer__gravidade consome desta tabela e
-- só aplica colapso + agregação + multiplicador de gestante.
--
-- ── Critérios atualmente implementados ────────────────────────────────────
--   1) SISCAN mamografia Cat 0/4/5  →  SISREG ultra/biópsia
--   2) SISCAN mamografia Cat 6      →  qualquer evento SER
--   3) SISCAN biópsia neoplásica    →  qualquer evento SER
--   4) SISREG biópsia               →  progresso intra-evento (2 legs:
--      solicitacao→autorizacao e autorizacao→execucao). Os legs
--      compartilham critério e peso; a coluna `etapa` os distingue.
--   5) SER status "PENDENTE"        →  atualização do status
--   6) SER status "EM_FILA"         →  atualização do status
--   7) SER status de falha          →  nova solicitação SER
--      (CHEGADA_NAO_CONFIRMADA, CANCELADA)
--
-- ── Mecanismos de desativação por padrão ──────────────────────────────────
--   • Cross-evento (1, 2, 3, 7): WHERE NOT EXISTS contra a CTE de desfecho
--     esperado (deduplicada). Quando aparece qualquer desfecho ≥
--     data_trigger — ou > data_trigger no caso de 7 — o critério some do
--     output.
--   • Intra-evento legs (4): filtro direto `data_proxima IS NULL` (a coluna
--     do desfecho do leg) na WHERE.
--   • Intra-evento "stuck" (5, 6): filtro `evento_status IN ('PENDENTE'/...)`
--     já age como desativação — quando o status muda, a linha some.
--   • Folga (todos): filtro final `dias_atraso > 0` remove instâncias cujo
--     gatilho ainda não passou da folga clínica (gravidade_criterio = 0).
--
-- ── Para adicionar um critério ────────────────────────────────────────────
--   1. Definir criterio_N_intervalo e adicionar peso correspondente na
--      macro monitora_cancer_pesos_clinicos.
--   2. Criar a(s) CTE(s) de gatilho/desfecho esperado (com dedup group by
--      cpf+data_trigger e max(risco) se cross-evento) e a CTE criterio_N
--      (anti-join NOT EXISTS para cross-evento, ou WHERE direto para
--      intra), emitindo as colunas: cpf_particao, criterio, etapa,
--      data_trigger, dias_atraso, intervalo_urgencia_dias,
--      risco_evento_gatilho, peso_criterio.
--   3. Incluir criterio_N no UNION ALL de instancias_brutas.
--   4. Atualizar o accepted_values do teste `criterio` em
--      _mart_monitora_cancer__schema.yml.
--
-- Para o score composto (colapso, agregação, multiplicador de gestante,
-- reescala 0-100 com teto dinâmico), ver int_monitora_cancer__gravidade.sql.

-- Materializado como tabela: (a) compartilhado entre gravidade (score final)
-- e mart_monitora_cancer__gravidade_instancias (consumo analítico), evita
-- recomputação; (b) clusterização por `criterio` acelera filtros analíticos
-- (Passos 2/3/4 do roadmap operam por critério).
{{
    config(
        materialized="table",
        schema="projeto_monitora_cancer",
        tags=["daily", "subgeral", "monitora_cancer"],
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
    -- Eventos do RUN ATUAL apenas, com a data_expected pré-calculada.
    eventos_run_atual as (
        select
            cpf_particao,
            fonte,
            procedimento,
            criterio_diagnostico,
            mama_esquerda_resultado,
            mama_direita_resultado,
            evento_status,
            data_solicitacao,
            data_autorizacao,
            data_execucao,
            data_referencia_evento,
            coalesce(data_execucao, data_autorizacao) as data_expected,
            risco,
            gestante
        from {{ ref("int_monitora_cancer__eventos_episodios") }}
        qualify run_id = max(run_id) over (partition by cpf_particao)
    ),

    -- ── Critério 1 — SISCAN mama Cat 0/4/5 → SISREG ultra/biópsia ──
    -- Dedup por (cpf, data_trigger) com max(risco): múltiplos eventos SISCAN
    -- no mesmo dia colapsam em um único gatilho, mantendo o pior risco.
    criterio_1_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger,
            max(risco) as risco_evento_gatilho
        from eventos_run_atual
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
        -- procedimento já vem normalizado por clean_proced_name em fatos
        -- (uppercase, sem diacríticos), então 'BIOPSIA' cobre 'BIÓPSIA' e
        -- 'ULTRA' cobre 'ULTRASSONOGRAFIA' e 'ULTRA-SONOGRAFIA'
        select
            cpf_particao,
            data_expected
        from eventos_run_atual
        where fonte = 'SISREG'
            and (
                contains_substr(procedimento, 'ULTRA')
                or contains_substr(procedimento, 'BIOPSIA')
            )
            and data_expected is not null
    ),

    criterio_1 as (
        select
            t.cpf_particao,
            'SISCAN_MAMA_CAT_0_4_5__SISREG_ULTRA_OU_BIOPSIA' as criterio,
            cast(null as string) as etapa,
            t.data_trigger,
            {{ monitora_cancer_dias_atraso('t.data_trigger', criterio_1_intervalo) }} as dias_atraso,
            {{ criterio_1_intervalo }} as intervalo_urgencia_dias,
            t.risco_evento_gatilho,
            {{ criterio_1_peso }} as peso_criterio
        from criterio_1_triggers as t
        where not exists (
            select 1 from criterio_1_desfecho_esperado as e
            where t.cpf_particao = e.cpf_particao
                and e.data_expected >= t.data_trigger
        )
    ),

    -- Desfechos esperados compartilhados pelos critérios 2 e 3: qualquer SER.
    desfecho_esperado_ser as (
        select
            cpf_particao,
            data_expected
        from eventos_run_atual
        where fonte = 'SER'
            and data_expected is not null
    ),

    -- ── Critério 2 — SISCAN mama Cat 6 → qualquer SER ──
    criterio_2_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger,
            max(risco) as risco_evento_gatilho
        from eventos_run_atual
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

    criterio_2 as (
        select
            t.cpf_particao,
            'SISCAN_MAMA_CAT_6__SER' as criterio,
            cast(null as string) as etapa,
            t.data_trigger,
            {{ monitora_cancer_dias_atraso('t.data_trigger', criterio_2_intervalo) }} as dias_atraso,
            {{ criterio_2_intervalo }} as intervalo_urgencia_dias,
            t.risco_evento_gatilho,
            {{ criterio_2_peso }} as peso_criterio
        from criterio_2_triggers as t
        where not exists (
            select 1 from desfecho_esperado_ser as e
            where t.cpf_particao = e.cpf_particao
                and e.data_expected >= t.data_trigger
        )
    ),

    -- ── Critério 3 — SISCAN biópsia neoplásica → qualquer SER ──
    -- criterio_diagnostico em laudos histopatológicos = (lesao_neoplasico is not null),
    -- então o filtro abaixo é equivalente exato a "biópsia com lesão neoplásica".
    criterio_3_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger,
            max(risco) as risco_evento_gatilho
        from eventos_run_atual
        where fonte = 'SISCAN'
            and procedimento not in (
                'RESULTADO MAMOGRAFIA DE RASTREIO',
                'RESULTADO MAMOGRAFIA DIAGNOSTICA'
            )
            and criterio_diagnostico = true
        group by cpf_particao, data_referencia_evento
    ),

    criterio_3 as (
        select
            t.cpf_particao,
            'SISCAN_BIOPSIA_NEOPLASICA__SER' as criterio,
            cast(null as string) as etapa,
            t.data_trigger,
            {{ monitora_cancer_dias_atraso('t.data_trigger', criterio_3_intervalo) }} as dias_atraso,
            {{ criterio_3_intervalo }} as intervalo_urgencia_dias,
            t.risco_evento_gatilho,
            {{ criterio_3_peso }} as peso_criterio
        from criterio_3_triggers as t
        where not exists (
            select 1 from desfecho_esperado_ser as e
            where t.cpf_particao = e.cpf_particao
                and e.data_expected >= t.data_trigger
        )
    ),

    -- ── Critério 4 — SISREG biópsia → progresso entre datas intra-evento ──
    -- Intra-evento, decomposto em dois "legs" sequenciais que compartilham
    -- critério e peso (distinguidos pela coluna `etapa`):
    --   leg 1 (SOLICITACAO_AUTORIZACAO): data_solicitacao → data_autorizacao
    --   leg 2 (AUTORIZACAO_EXECUCAO):    data_autorizacao → data_execucao
    -- Biópsia no SISREG não tem data_resultado, por isso só dois legs.
    criterio_4 as (
        -- leg 1: solicitacao → autorizacao
        select
            cpf_particao,
            'SISREG_BIOPSIA_PROGRESSO' as criterio,
            'SOLICITACAO_AUTORIZACAO' as etapa,
            data_solicitacao as data_trigger,
            {{ monitora_cancer_dias_atraso('data_solicitacao', criterio_4_intervalo) }} as dias_atraso,
            {{ criterio_4_intervalo }} as intervalo_urgencia_dias,
            risco as risco_evento_gatilho,
            {{ criterio_4_peso }} as peso_criterio
        from eventos_run_atual
        where fonte = 'SISREG'
            and contains_substr(procedimento, 'BIOPSIA')
            and data_solicitacao is not null
            and data_autorizacao is null

        union all

        -- leg 2: autorizacao → execucao
        select
            cpf_particao,
            'SISREG_BIOPSIA_PROGRESSO' as criterio,
            'AUTORIZACAO_EXECUCAO' as etapa,
            data_autorizacao as data_trigger,
            {{ monitora_cancer_dias_atraso('data_autorizacao', criterio_4_intervalo) }} as dias_atraso,
            {{ criterio_4_intervalo }} as intervalo_urgencia_dias,
            risco as risco_evento_gatilho,
            {{ criterio_4_peso }} as peso_criterio
        from eventos_run_atual
        where fonte = 'SISREG'
            and contains_substr(procedimento, 'BIOPSIA')
            and data_autorizacao is not null
            and data_execucao is null
    ),

    -- ── Critério 5 — SER status "PENDENTE" → atualização do status ──
    criterio_5 as (
        select
            cpf_particao,
            'SER_PENDENTE__STATUS_UPDATE' as criterio,
            cast(null as string) as etapa,
            data_solicitacao as data_trigger,
            {{ monitora_cancer_dias_atraso('data_solicitacao', criterio_5_intervalo) }} as dias_atraso,
            {{ criterio_5_intervalo }} as intervalo_urgencia_dias,
            risco as risco_evento_gatilho,
            {{ criterio_5_peso }} as peso_criterio
        from eventos_run_atual
        where fonte = 'SER'
            and evento_status = 'PENDENTE'
            and data_solicitacao is not null
    ),

    -- ── Critério 6 — SER status "EM_FILA" → atualização do status ──
    criterio_6 as (
        select
            cpf_particao,
            'SER_EM_FILA__STATUS_UPDATE' as criterio,
            cast(null as string) as etapa,
            data_solicitacao as data_trigger,
            {{ monitora_cancer_dias_atraso('data_solicitacao', criterio_6_intervalo) }} as dias_atraso,
            {{ criterio_6_intervalo }} as intervalo_urgencia_dias,
            risco as risco_evento_gatilho,
            {{ criterio_6_peso }} as peso_criterio
        from eventos_run_atual
        where fonte = 'SER'
            and evento_status = 'EM_FILA'
            and data_solicitacao is not null
    ),

    -- ── Critério 7 — SER status de falha → nova solicitação SER ──
    criterio_7_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger,
            max(risco) as risco_evento_gatilho
        from eventos_run_atual
        where fonte = 'SER'
            and evento_status in ('CHEGADA_NAO_CONFIRMADA', 'CANCELADA')
        group by cpf_particao, data_referencia_evento
    ),

    desfecho_esperado_ser_nova_solicitacao as (
        select
            cpf_particao,
            data_solicitacao as data_expected
        from eventos_run_atual
        where fonte = 'SER'
            and data_solicitacao is not null
    ),

    criterio_7 as (
        select
            t.cpf_particao,
            'SER_FALHA__NOVA_SER' as criterio,
            cast(null as string) as etapa,
            t.data_trigger,
            {{ monitora_cancer_dias_atraso('t.data_trigger', criterio_7_intervalo) }} as dias_atraso,
            {{ criterio_7_intervalo }} as intervalo_urgencia_dias,
            t.risco_evento_gatilho,
            {{ criterio_7_peso }} as peso_criterio
        from criterio_7_triggers as t
        where not exists (
            select 1 from desfecho_esperado_ser_nova_solicitacao as e
            where t.cpf_particao = e.cpf_particao
                and e.data_expected > t.data_trigger
        )
    ),

    -- UNION de todas as instâncias antes do filtro de folga.
    instancias_brutas as (
        select * from criterio_1
        union all
        select * from criterio_2
        union all
        select * from criterio_3
        union all
        select * from criterio_4
        union all
        select * from criterio_5
        union all
        select * from criterio_6
        union all
        select * from criterio_7
    ),

    -- gestante por paciente (broadcasted), para JOIN single-pass.
    gestantes as (
        select distinct
            cpf_particao,
            gestante
        from eventos_run_atual
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
