-- noqa: disable=LT08

-- Score de gravidade da paciente no monitoramento de câncer de mama.
--
-- Conceito: para cada par (trigger, expected, threshold) — chamado "subscore" —
-- conta-se quantos dias o desfecho esperado atrasou para acontecer após o
-- evento gatilho, descontando a folga do threshold:
--
--   dias_atraso = max(0, date_diff(expected_date_ou_hoje, trigger_date) - threshold)
--
-- O gravidade_score do paciente é a SOMA das parcelas de todos os triggers
-- de todos os subscores, multiplicada por 2 se a paciente for gestante.
--
-- Escopo temporal: apenas eventos do RUN ATUAL (último episódio) entram no
-- cálculo. Eventos de runs antigos (separados por > episodio_gap_dias)
-- representam ciclos de cuidado encerrados e não inflam o score atual.
--
-- Convenções:
--   trigger_date  = data_referencia_evento (= MAX das 4 datas do evento)
--   expected_date = COALESCE(data_execucao, data_autorizacao)
--                   (eventos só com data_solicitacao ainda na fila NÃO contam)
--
-- Subscores atuais:
--   1) SISCAN mamografia Cat 0/4/5  →  SISREG ultra/biópsia,         threshold 10 dias
--   2) SISCAN mamografia Cat 6      →  qualquer evento SER,          threshold  5 dias
--   3) SISCAN biópsia neoplásica    →  qualquer evento SER,          threshold  5 dias
--   4) SISREG biópsia               →  progresso intra-evento,       threshold 20 dias
--      (decomposto em dois legs: solicitacao→autorizacao e autorizacao→execucao)
--   5) SER status "PENDENTE"        →  atualização do status,        threshold 10 dias
--   6) SER status "EM FILA"         →  atualização do status,        threshold 60 dias
--   7) SER status falha             →  nova solicitação SER,         threshold 10 dias
--      (CHEGADA NAO CONFIRMADA, CANCELADA)
--
-- Para adicionar um subscore: criar mais um par de CTEs `subscore_N_triggers`
-- + `subscore_N` no padrão abaixo e incluir no UNION ALL de `todos_atrasos`.
-- Subscores intra-evento (mesmo evento de origem para trigger e expected)
-- são single-row: dispensam JOIN porque trigger e expected vêm da mesma linha.
-- Para subscores em que o expected é "status saiu do valor inicial" e o
-- snapshot só guarda o status corrente, a presença da linha com aquele
-- status é evidência de que a atualização ainda não ocorreu — então
-- data_expected = NULL e o atraso conta de data_solicitacao até hoje.
--
-- Overlaps conhecidos entre subscores (intencionais — são dimensões
-- distintas do mesmo atraso, não double-counting acidental):
--   • Subscore 1 (expected = SISREG biópsia) ↔ Subscore 4 (trigger =
--     SISREG biópsia): uma biópsia atrasada contribui em (1) pelo gap
--     SISCAN→SISREG e em (4) pelos gaps internos do pipeline SISREG.
--     Períodos temporais distintos, contribuições somam.
--   • Subscores 5 e 6 são mutuamente exclusivos por linha (filtros de
--     evento_status disjuntos), assim como 7 vs. 5/6.
--   • Subscore 7 (expected = nova SER) pode usar como expected uma SER
--     que ela própria seja trigger de 5/6. Ex.: SER cancelada → nova SER
--     PENDENTE há 30 dias dispara 7 (cancelada) E 5 (pendente).
--   • Subscores 2/3 (expected requer data_execucao ou data_autorizacao
--     preenchidas) NÃO sobrepõem a 5/6 (que filtram linhas em PENDENTE/
--     EM FILA, com essas datas necessariamente nulas).

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
            gestante
        from {{ ref("int_monitora_cancer__eventos_episodios") }}
        qualify run_id = max(run_id) over (partition by cpf_particao)
    ),

    -- ── Subscore 1: SISCAN mama Cat 0/4/5 → SISREG ultra/biópsia, threshold 10 ──
    subscore_1_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger
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

    subscore_1 as (
        select
            t.cpf_particao,
            'SISCAN_MAMA_CAT_0_4_5__SISREG_ULTRA_OU_BIOPSIA' as subscore,
            t.data_trigger,
            min(e.data_expected) as data_expected,
            greatest(
                0,
                date_diff(
                    coalesce(min(e.data_expected), current_date('America/Sao_Paulo')),
                    t.data_trigger,
                    day
                ) - 10
            ) as dias_atraso
        from subscore_1_triggers as t
            left join subscore_1_expected as e
            on t.cpf_particao = e.cpf_particao
                and e.data_expected >= t.data_trigger
        group by t.cpf_particao, t.data_trigger
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
            data_referencia_evento as data_trigger
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
    ),

    subscore_2 as (
        select
            t.cpf_particao,
            'SISCAN_MAMA_CAT_6__SER' as subscore,
            t.data_trigger,
            min(e.data_expected) as data_expected,
            greatest(
                0,
                date_diff(
                    coalesce(min(e.data_expected), current_date('America/Sao_Paulo')),
                    t.data_trigger,
                    day
                ) - 5
            ) as dias_atraso
        from subscore_2_triggers as t
            left join expected_ser as e
            on t.cpf_particao = e.cpf_particao
                and e.data_expected >= t.data_trigger
        group by t.cpf_particao, t.data_trigger
    ),

    -- ── Subscore 3: SISCAN biópsia neoplásica → qualquer SER, threshold 5 ──
    -- criterio_diagnostico em laudos histopatológicos = (lesao_neoplasico is not null),
    -- então o filtro abaixo é equivalente exato a "biópsia com lesão neoplásica".
    subscore_3_triggers as (
        select
            cpf_particao,
            data_referencia_evento as data_trigger
        from eventos_run_atual
        where fonte = 'SISCAN'
            and procedimento not in (
                'RESULTADO MAMOGRAFIA DE RASTREIO',
                'RESULTADO MAMOGRAFIA DIAGNOSTICA'
            )
            and criterio_diagnostico = true
    ),

    subscore_3 as (
        select
            t.cpf_particao,
            'SISCAN_BIOPSIA_NEOPLASICA__SER' as subscore,
            t.data_trigger,
            min(e.data_expected) as data_expected,
            greatest(
                0,
                date_diff(
                    coalesce(min(e.data_expected), current_date('America/Sao_Paulo')),
                    t.data_trigger,
                    day
                ) - 5
            ) as dias_atraso
        from subscore_3_triggers as t
            left join expected_ser as e
            on t.cpf_particao = e.cpf_particao
                and e.data_expected >= t.data_trigger
        group by t.cpf_particao, t.data_trigger
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
        select
            cpf_particao,
            'SISREG_BIOPSIA__SOLICITACAO_AUTORIZACAO' as subscore,
            data_solicitacao as data_trigger,
            data_autorizacao as data_expected,
            greatest(
                0,
                date_diff(
                    coalesce(data_autorizacao, current_date('America/Sao_Paulo')),
                    data_solicitacao,
                    day
                ) - 20
            ) as dias_atraso
        from eventos_run_atual
        where fonte = 'SISREG'
            and contains_substr(procedimento, 'BIOPSIA')
            and data_solicitacao is not null

        union all

        -- leg 2: autorizacao → execucao
        select
            cpf_particao,
            'SISREG_BIOPSIA__AUTORIZACAO_EXECUCAO' as subscore,
            data_autorizacao as data_trigger,
            data_execucao as data_expected,
            greatest(
                0,
                date_diff(
                    coalesce(data_execucao, current_date('America/Sao_Paulo')),
                    data_autorizacao,
                    day
                ) - 20
            ) as dias_atraso
        from eventos_run_atual
        where fonte = 'SISREG'
            and contains_substr(procedimento, 'BIOPSIA')
            and data_autorizacao is not null
    ),

    -- ── Subscore 5: SER status "PENDENTE" → atualização do status, threshold 10 ──
    -- Intra-evento "stuck": a linha com evento_status = 'PENDENTE' no snapshot
    -- corrente é prova de que a coluna de status ainda não foi atualizada —
    -- logo data_expected é sempre NULL e o atraso conta de data_solicitacao
    -- até hoje (cai no COALESCE de current_date).
    subscore_5 as (
        select
            cpf_particao,
            'SER_PENDENTE__STATUS_UPDATE' as subscore,
            data_solicitacao as data_trigger,
            cast(null as date) as data_expected,
            greatest(
                0,
                date_diff(
                    current_date('America/Sao_Paulo'),
                    data_solicitacao,
                    day
                ) - 10
            ) as dias_atraso
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
            cast(null as date) as data_expected,
            greatest(
                0,
                date_diff(
                    current_date('America/Sao_Paulo'),
                    data_solicitacao,
                    day
                ) - 60
            ) as dias_atraso
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
            data_referencia_evento as data_trigger
        from eventos_run_atual
        where fonte = 'SER'
            and evento_status in ('CHEGADA NAO CONFIRMADA', 'CANCELADA')
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
            min(e.data_expected) as data_expected,
            greatest(
                0,
                date_diff(
                    coalesce(min(e.data_expected), current_date('America/Sao_Paulo')),
                    t.data_trigger,
                    day
                ) - 10
            ) as dias_atraso
        from subscore_7_triggers as t
            left join expected_ser_nova_solicitacao as e
            on t.cpf_particao = e.cpf_particao
                and e.data_expected > t.data_trigger
        group by t.cpf_particao, t.data_trigger
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

    agregado as (
        select
            cpf_particao,
            sum(dias_atraso) as gravidade_score_base,
            array_agg(
                struct(
                    subscore,
                    data_trigger,
                    data_expected,
                    dias_atraso
                )
                order by data_trigger desc, subscore
            ) as gravidade_breakdown
        from todos_atrasos
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
