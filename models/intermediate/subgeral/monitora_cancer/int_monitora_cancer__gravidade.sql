-- noqa: disable=LT08

-- Parâmetros de cada subscore em dias.
-- Cada subscore usa este valor em DOIS pontos:
--   • threshold subtraído no cálculo de dias_atraso (folga antes de começar a contar atraso).
--   • dias_dobrar_subscore na fórmula do subscore (a cada N dias de atraso,
--     o termo exponencial dobra).
-- Convenção atual: dias_dobrar_subscore = threshold por subscore.
{% set subscore_1_threshold = 10 %}  {# SISCAN mama Cat 0/4/5 → SISREG ultra/biópsia #}
{% set subscore_2_threshold = 5 %}   {# SISCAN mama Cat 6 → qualquer SER #}
{% set subscore_3_threshold = 5 %}   {# SISCAN biópsia neoplásica → qualquer SER #}
{% set subscore_4_threshold = 20 %}  {# SISREG biópsia → progresso intra-evento (2 legs) #}
{% set subscore_5_threshold = 10 %}  {# SER status "PENDENTE" → atualização #}
{% set subscore_6_threshold = 60 %}  {# SER status "EM FILA" → atualização #}
{% set subscore_7_threshold = 10 %}  {# SER falha → nova solicitação SER #}

-- Teto (em dias) aplicado a dias_atraso dentro do termo exponencial do subscore,
-- evitando explosão. Usado em DOIS pontos:
--   • LEAST(dias_atraso, teto_atraso) dentro da macro monitora_cancer_subscore.
--   • Denominador da normalização (subscore_valor máximo possível por trigger),
--     na macro monitora_cancer_subscore_normalizado.
{% set teto_atraso = 90 %}

-- Maior valor possível de risco_trigger no domínio de monitora_cancer.
{% set max_risco = 4 %}

-- Score de gravidade da paciente no monitoramento de câncer de mama.
--
-- Conceito: para cada subscore — caracterizado pela tupla
-- (trigger, expected, threshold, dias_dobrar_subscore, risco_trigger) —
-- o TRIGGER só gera subscore enquanto o EXPECTED ainda NÃO chegou. Quando o
-- desfecho esperado acontece, o trigger é desativado e não contribui mais
-- ao score (não há entrada no breakdown para esse trigger).
--
-- Para triggers ainda ativos, mede-se quantos dias o desfecho está atrasado
-- (contagem desde o trigger até hoje, descontada a folga do threshold):
--
--   dias_atraso = max(0, date_diff(hoje, trigger_date) - threshold)
--
-- O valor BRUTO de cada subscore (parcela que aquele trigger contribui ao
-- score, antes da normalização) é:
--
--   subscore_valor = coalesce(risco_trigger, 1)
--                  * 2 ^ ( min(dias_atraso, teto_atraso) / dias_dobrar_subscore )
--
-- Implementado na macro monitora_cancer_subscore. Componentes:
--   • Fator de risco: coalesce(risco_trigger, 1). risco_trigger é o risco
--     do evento gatilho (1..4, ou NULL quando não mapeado). Multiplica
--     diretamente o subscore; NULL → fator 1 (sem agravador).
--   • dias_dobrar_subscore controla a velocidade de crescimento: a cada
--     `dias_dobrar_subscore` dias de atraso, o termo exponencial dobra. O
--     teto `teto_atraso` evita explosão.
-- Observação: dentro do threshold, dias_atraso=0 → termo exponencial=2^0=1,
-- ou seja, todo trigger ativo contribui com no mínimo 1 × fator de risco.
--
-- Antes da agregação, cada subscore bruto é NORMALIZADO para (0, 1] dividindo
-- pelo seu próprio máximo teórico (max_risco × termo exponencial saturado em
-- teto_atraso) — implementado na macro monitora_cancer_subscore_normalizado:
--
--   subscore_normalizado = subscore_valor
--                        / ( max_risco * 2 ^ ( teto_atraso / dias_dobrar_subscore ) )
--
-- max_risco é o maior risco_trigger possível no domínio (hoje = 4 em todas
-- as fontes que produzem triggers).
--
-- O gravidade_score do paciente é a SOMA dos subscores NORMALIZADOS de todos
-- os triggers ATIVOS, multiplicada por 2 se a paciente for gestante.
--
-- Escopo temporal: apenas eventos do RUN ATUAL (último episódio) entram no
-- cálculo. Eventos de runs antigos (separados por > episodio_gap_dias)
-- representam ciclos de cuidado encerrados e não inflam o score atual.
--
-- Convenções:
--   trigger_date  = data_referencia_evento (= MAX das 4 datas do evento)
--   expected_date = COALESCE(data_execucao, data_autorizacao) — usada apenas
--                   no filtro de desativação (NOT EXISTS / WHERE x IS NULL);
--                   nunca entra no cálculo de dias_atraso (que é sempre
--                   contado de trigger_date até hoje).
--
-- Subscores atuais (valores parametrizados no topo do arquivo;
-- dias_dobrar_subscore = threshold em todos):
--   1) SISCAN mamografia Cat 0/4/5  →  SISREG ultra/biópsia,         threshold subscore_1_threshold
--   2) SISCAN mamografia Cat 6      →  qualquer evento SER,          threshold subscore_2_threshold
--   3) SISCAN biópsia neoplásica    →  qualquer evento SER,          threshold subscore_3_threshold
--   4) SISREG biópsia               →  progresso intra-evento,       threshold subscore_4_threshold
--      (decomposto em dois legs: solicitacao→autorizacao e autorizacao→execucao)
--   5) SER status "PENDENTE"        →  atualização do status,        threshold subscore_5_threshold
--   6) SER status "EM FILA"         →  atualização do status,        threshold subscore_6_threshold
--   7) SER status falha             →  nova solicitação SER,         threshold subscore_7_threshold
--      (CHEGADA NAO CONFIRMADA, CANCELADA)
--
-- Mecanismo de desativação por padrão:
--   • Cross-evento (1, 2, 3, 7): WHERE NOT EXISTS contra a CTE de expected
--     (deduplicada). Quando aparece qualquer expected ≥ trigger_date — ou
--     > trigger_date no caso de 7 — o trigger some do output.
--   • Intra-evento legs (4): filtro direto `data_proxima IS NULL` (a coluna
--     do desfecho do leg) na WHERE.
--   • Intra-evento "stuck" (5, 6): filtro `evento_status IN ('PENDENTE'/...)`
--     já age como desativação — quando o status muda, a linha some.
--
-- Para adicionar um subscore: criar `subscore_N_triggers` (com dedup
-- group by cpf+data_trigger e max(risco) se cross-evento) + `subscore_N`
-- (anti-join NOT EXISTS para cross-evento, ou WHERE direto para intra) e
-- incluir no UNION ALL de `todos_atrasos`. Subscores intra-evento dispensam
-- JOIN/anti-join porque trigger e desfecho vêm da mesma linha.

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

    -- ── Subscore 1: SISCAN mama Cat 0/4/5 → SISREG ultra/biópsia, threshold 10 ──
    -- Dedup por (cpf, data_trigger) com max(risco): múltiplos eventos SISCAN
    -- no mesmo dia colapsam em um único trigger, mantendo o pior risco.
    subscore_1_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger,
            max(risco) as risco_trigger
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

    subscore_1_expected as (
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

    -- Trigger só gera subscore quando o expected ainda NÃO chegou (NOT EXISTS).
    -- dias_atraso é contado de data_trigger até hoje (descontado o threshold).
    subscore_1 as (
        select
            t.cpf_particao,
            'SISCAN_MAMA_CAT_0_4_5__SISREG_ULTRA_OU_BIOPSIA' as subscore,
            t.data_trigger,
            {{ monitora_cancer_dias_atraso('t.data_trigger', subscore_1_threshold) }} as dias_atraso,
            {{ subscore_1_threshold }} as dias_dobrar_subscore,
            t.risco_trigger
        from subscore_1_triggers as t
        where not exists (
            select 1 from subscore_1_expected as e
            where t.cpf_particao = e.cpf_particao
                and e.data_expected >= t.data_trigger
        )
    ),

    -- Expecteds compartilhados pelos subscores 2 e 3: qualquer evento SER.
    expected_ser as (
        select
            cpf_particao,
            data_expected
        from eventos_run_atual
        where fonte = 'SER'
            and data_expected is not null
    ),

    -- ── Subscore 2: SISCAN mama Cat 6 → qualquer SER, threshold 5 ──
    subscore_2_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger,
            max(risco) as risco_trigger
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

    subscore_2 as (
        select
            t.cpf_particao,
            'SISCAN_MAMA_CAT_6__SER' as subscore,
            t.data_trigger,
            {{ monitora_cancer_dias_atraso('t.data_trigger', subscore_2_threshold) }} as dias_atraso,
            {{ subscore_2_threshold }} as dias_dobrar_subscore,
            t.risco_trigger
        from subscore_2_triggers as t
        where not exists (
            select 1 from expected_ser as e
            where t.cpf_particao = e.cpf_particao
                and e.data_expected >= t.data_trigger
        )
    ),

    -- ── Subscore 3: SISCAN biópsia neoplásica → qualquer SER, threshold 5 ──
    -- criterio_diagnostico em laudos histopatológicos = (lesao_neoplasico is not null),
    -- então o filtro abaixo é equivalente exato a "biópsia com lesão neoplásica".
    subscore_3_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger,
            max(risco) as risco_trigger
        from eventos_run_atual
        where fonte = 'SISCAN'
            and procedimento not in (
                'RESULTADO MAMOGRAFIA DE RASTREIO',
                'RESULTADO MAMOGRAFIA DIAGNOSTICA'
            )
            and criterio_diagnostico = true
        group by cpf_particao, data_referencia_evento
    ),

    subscore_3 as (
        select
            t.cpf_particao,
            'SISCAN_BIOPSIA_NEOPLASICA__SER' as subscore,
            t.data_trigger,
            {{ monitora_cancer_dias_atraso('t.data_trigger', subscore_3_threshold) }} as dias_atraso,
            {{ subscore_3_threshold }} as dias_dobrar_subscore,
            t.risco_trigger
        from subscore_3_triggers as t
        where not exists (
            select 1 from expected_ser as e
            where t.cpf_particao = e.cpf_particao
                and e.data_expected >= t.data_trigger
        )
    ),

    -- ── Subscore 4: SISREG biópsia → progresso entre datas intra-evento, threshold 20 ──
    -- Intra-evento, decomposto em dois "legs" sequenciais:
    --   leg 1: data_solicitacao → data_autorizacao
    --   leg 2: data_autorizacao → data_execucao
    -- Cada leg é avaliado independentemente com o mesmo threshold (20 dias)
    -- e produz sua própria parcela em dias_atraso. Se a data final do leg
    -- está NULL, o atraso conta de data_inicial até hoje (cai no COALESCE).
    -- Biópsia no SISREG não tem data_resultado (data_exame_resultado é
    -- hardcoded NULL em int_monitora_cancer__sisreg), por isso só dois legs.
    -- Leg 2 só dispara depois que data_autorizacao foi preenchida — antes
    -- disso, o ônus do atraso está totalmente em leg 1.
    subscore_4 as (
        -- leg 1: solicitacao → autorizacao
        -- Trigger só ativo quando data_autorizacao ainda NÃO chegou.
        select
            cpf_particao,
            'SISREG_BIOPSIA__SOLICITACAO_AUTORIZACAO' as subscore,
            data_solicitacao as data_trigger,
            {{ monitora_cancer_dias_atraso('data_solicitacao', subscore_4_threshold) }} as dias_atraso,
            {{ subscore_4_threshold }} as dias_dobrar_subscore,
            risco as risco_trigger
        from eventos_run_atual
        where fonte = 'SISREG'
            and contains_substr(procedimento, 'BIOPSIA')
            and data_solicitacao is not null
            and data_autorizacao is null

        union all

        -- leg 2: autorizacao → execucao
        -- Trigger só ativo quando data_execucao ainda NÃO chegou.
        select
            cpf_particao,
            'SISREG_BIOPSIA__AUTORIZACAO_EXECUCAO' as subscore,
            data_autorizacao as data_trigger,
            {{ monitora_cancer_dias_atraso('data_autorizacao', subscore_4_threshold) }} as dias_atraso,
            {{ subscore_4_threshold }} as dias_dobrar_subscore,
            risco as risco_trigger
        from eventos_run_atual
        where fonte = 'SISREG'
            and contains_substr(procedimento, 'BIOPSIA')
            and data_autorizacao is not null
            and data_execucao is null
    ),

    -- ── Subscore 5: SER status "PENDENTE" → atualização do status, threshold 10 ──
    -- Intra-evento "stuck": a linha com evento_status = 'PENDENTE' no snapshot
    -- corrente é prova de que a atualização do status ainda não ocorreu — quando
    -- o status muda, a linha sai do filtro e o trigger é automaticamente
    -- desativado (sem precisar de anti-join). dias_atraso conta de
    -- data_solicitacao até hoje.
    subscore_5 as (
        select
            cpf_particao,
            'SER_PENDENTE__STATUS_UPDATE' as subscore,
            data_solicitacao as data_trigger,
            {{ monitora_cancer_dias_atraso('data_solicitacao', subscore_5_threshold) }} as dias_atraso,
            {{ subscore_5_threshold }} as dias_dobrar_subscore,
            risco as risco_trigger
        from eventos_run_atual
        where fonte = 'SER'
            and evento_status = 'PENDENTE'
            and data_solicitacao is not null
    ),

    -- ── Subscore 6: SER status "EM FILA" → atualização do status, threshold 60 ──
    -- Mesma lógica do subscore 5, com filtro de status e threshold distintos.
    subscore_6 as (
        select
            cpf_particao,
            'SER_EM_FILA__STATUS_UPDATE' as subscore,
            data_solicitacao as data_trigger,
            {{ monitora_cancer_dias_atraso('data_solicitacao', subscore_6_threshold) }} as dias_atraso,
            {{ subscore_6_threshold }} as dias_dobrar_subscore,
            risco as risco_trigger
        from eventos_run_atual
        where fonte = 'SER'
            and evento_status = 'EM FILA'
            and data_solicitacao is not null
    ),

    -- ── Subscore 7: SER status de falha → nova solicitação SER, threshold 10 ──
    -- Cross-evento: quando uma SER termina em "CHEGADA NAO CONFIRMADA"
    -- (paciente não compareceu) ou "CANCELADA", espera-se uma NOVA SER em
    -- até 10 dias. data_trigger = data_referencia_evento (última data
    -- conhecida do evento de falha — pode ser data_execucao para chegada
    -- não confirmada ou data_solicitacao para cancelamento). O JOIN com
    -- expected_ser_nova_solicitacao usa data_solicitacao > data_trigger
    -- (estrito) para garantir que o expected é uma OUTRA solicitação,
    -- posterior, e não a própria linha do trigger.
    subscore_7_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger,
            max(risco) as risco_trigger
        from eventos_run_atual
        where fonte = 'SER'
            and evento_status in ('CHEGADA NAO CONFIRMADA', 'CANCELADA')
        group by cpf_particao, data_referencia_evento
    ),

    expected_ser_nova_solicitacao as (
        select
            cpf_particao,
            data_solicitacao as data_expected
        from eventos_run_atual
        where fonte = 'SER'
            and data_solicitacao is not null
    ),

    subscore_7 as (
        select
            t.cpf_particao,
            'SER_FALHA__NOVA_SER' as subscore,
            t.data_trigger,
            {{ monitora_cancer_dias_atraso('t.data_trigger', subscore_7_threshold) }} as dias_atraso,
            {{ subscore_7_threshold }} as dias_dobrar_subscore,
            t.risco_trigger
        from subscore_7_triggers as t
        where not exists (
            select 1 from expected_ser_nova_solicitacao as e
            where t.cpf_particao = e.cpf_particao
                and e.data_expected > t.data_trigger
        )
    ),

    todos_atrasos as (
        select * from subscore_1
        union all
        select * from subscore_2
        union all
        select * from subscore_3
        union all
        select * from subscore_4
        union all
        select * from subscore_5
        union all
        select * from subscore_6
        union all
        select * from subscore_7
    ),

    -- Aplica a fórmula
    --   subscore_valor = coalesce(risco_trigger, 1)
    --                  * 2 ^ (min(dias_atraso, teto_atraso) / dias_dobrar_subscore)
    -- uma única vez por linha. Isolar em CTE evita recomputar a expressão
    -- dentro da normalização, do sum() e do array_agg(struct(...)).
    subscores_brutos as (
        select
            cpf_particao,
            subscore,
            data_trigger,
            dias_atraso,
            dias_dobrar_subscore,
            risco_trigger,
            {{ monitora_cancer_subscore('dias_atraso', 'dias_dobrar_subscore', 'risco_trigger', teto_atraso) }} as subscore_valor
        from todos_atrasos
    ),

    -- Normalização do subscore para (0, 1] (divide pelo máximo teórico que
    -- aquele subscore poderia atingir, dado risco_trigger no topo da escala
    -- e dias_atraso saturado em teto_atraso). É o valor que entra na soma
    -- final do gravidade_score.
    subscores as (
        select
            cpf_particao,
            subscore,
            data_trigger,
            dias_atraso,
            dias_dobrar_subscore,
            risco_trigger,
            subscore_valor,
            {{ monitora_cancer_subscore_normalizado('subscore_valor', 'dias_dobrar_subscore', teto_atraso, max_risco) }} as subscore_valor_normalizado
        from subscores_brutos
    ),

    agregado as (
        select
            cpf_particao,
            sum(subscore_valor_normalizado) as gravidade_score_base,
            array_agg(
                struct(
                    subscore,
                    data_trigger,
                    dias_atraso,
                    dias_dobrar_subscore,
                    risco_trigger,
                    subscore_valor,
                    subscore_valor_normalizado
                )
                order by data_trigger desc, subscore
            ) as gravidade_breakdown
        from subscores
        group by cpf_particao
    ),

    -- gestante já vem propagado em cada linha de eventos_episodios; qualquer
    -- linha do paciente serve. distinct para colapsar.
    gestantes as (
        select distinct
            cpf_particao,
            gestante
        from eventos_run_atual
    )

select
    a.cpf_particao,
    if(
        coalesce(g.gestante, false),
        a.gravidade_score_base * 2,
        a.gravidade_score_base
    ) as gravidade_score,
    a.gravidade_score_base,
    coalesce(g.gestante, false) as gestante,
    a.gravidade_breakdown
from agregado as a
    left join gestantes as g using (cpf_particao)
