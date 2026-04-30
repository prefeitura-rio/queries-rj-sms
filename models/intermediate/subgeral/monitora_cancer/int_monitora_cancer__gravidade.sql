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
--   1) SISCAN mamografia Cat 0/4/5  →  SISREG ultra/biópsia,  threshold 10 dias
--   2) SISCAN mamografia Cat 6      →  qualquer evento SER,   threshold  5 dias
--   3) SISCAN biópsia neoplásica    →  qualquer evento SER,   threshold  5 dias
--
-- Para adicionar um subscore: criar mais um par de CTEs `subscore_N_triggers`
-- + `subscore_N` no padrão abaixo e incluir no UNION ALL de `todos_atrasos`.
-- Subscores intra-evento (mesmo evento de origem para trigger e expected)
-- usam evento_id como chave de JOIN em vez de cpf_particao + faixa de data.

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

    todos_atrasos as (
        select * from subscore_1
        union all
        select * from subscore_2
        union all
        select * from subscore_3
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
