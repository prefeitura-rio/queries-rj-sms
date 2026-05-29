-- noqa: disable=LT08

-- ════════════════════════════════════════════════════════════════════════════
-- PARÂMETROS
-- ════════════════════════════════════════════════════════════════════════════
--
-- Parâmetros do score composto (etapas de agregação + apresentação). Os
-- parâmetros das INSTÂNCIAS (intervalo_urgencia, fator de risco amortecido)
-- vivem em int_monitora_cancer__gravidade_instancias. Os pesos clínicos
-- são fonte única na macro monitora_cancer_pesos_clinicos, aplicados em
-- int_monitora_cancer__gravidade_instancias (coluna peso_criterio) e
-- consumidos aqui via essa coluna.

-- peso_carga_total: peso do termo_soma na Eq. 3. 0.5 ⇒ 2 critérios
-- medianos equivalem a 1 muito grave (pacientes com múltiplas pendências
-- sobem materialmente na fila). Decisão clínica.
{% set peso_carga_total = 0.5 %}

-- multiplicador_gestante: α em (1 + α · 1_{gestante}) aplicado ao
-- termo_max na Eq. 3. 1.0 ⇒ gestante dobra o termo_max. Efeito
-- proporcional à gravidade do critério (não offset fixo); gestante
-- sem critério ativo continua em score = 0.
{% set multiplicador_gestante = 1.0 %}

-- (Sem parâmetro teto_score_apresentacao.) O teto da reescala 0-100 é
-- computado DINAMICAMENTE como o p95 da distribuição de gravidade_total
-- do run atual — ver CTE teto_apresentacao abaixo. Dispensa
-- recalibração manual após mudança de pesos/parâmetros.

-- Sem guard de invariante de ordem: gestante-sem-critério tem termo_max
-- = 0 e portanto gravidade_total = 0; a invariante "gestante sem critério
-- não passa não-gestante com critério" vale por construção.

-- ════════════════════════════════════════════════════════════════════════════
-- SCORE DE GRAVIDADE — monitoramento de câncer de mama
-- ════════════════════════════════════════════════════════════════════════════
--
-- Ordena pacientes na linha de cuidado por urgência de contato. Quanto mais
-- pendências, mais graves (atraso × risco × peso clínico), mais alto o
-- score. Gestante recebe multiplicador sobre o termo_max. Saída
-- esperada de consumo:
--   ORDER BY gravidade_total DESC, gestante DESC
-- O gravidade_total_0_100 é só para apresentação (satura no topo).
--
-- ESTRUTURA EM CAMADAS
-- ─────────────────────
-- int_monitora_cancer__gravidade_instancias   (table)
--   ▶ uma linha por (cpf, criterio, etapa, data_trigger) ativa
--   ▶ contém: dias_atraso, intervalo_urgencia, risco_evento_gatilho,
--             peso_criterio, fator_tempo, fator_risco, gravidade_criterio,
--             gestante
--   ▶ filtra dias_atraso > 0 (folga descartada)
--   ▶ contém TODAS as instâncias (antes do colapso MAX por critério) —
--     base correta para análise de sensibilidade
--      │
--      ▼
-- int_monitora_cancer__gravidade   (este arquivo, ephemeral)
--   ▶ aplica colapso MAX por critério (Etapa 3)
--   ▶ agrega max + soma por paciente (Etapa 5)
--   ▶ aplica score composto + multiplicador de gestante (Etapa 6)
--   ▶ reescala 0-100 com clip dinâmico (Etapa 7)
--      │
--      ▼
-- mart_monitora_cancer__gravidade  (table)
--
-- ETAPA 3 — colapso POR CRITÉRIO (CTE gravidade_max_por_criterio)
-- Um mesmo critério pode disparar várias vezes para a mesma paciente.
-- Mantém-se, por (paciente, critério), apenas a instância de MAIOR
-- gravidade_criterio — a pendência mais urgente. Spec §4.5: evita
-- contagem dupla.
--
-- ETAPA 4 — contribuição ponderada (por critério) — Eq. (2)
--   contribuicao_criterio = peso_criterio · gravidade_criterio
--
-- ETAPA 5 — agregação por paciente
--   gravidade_termo_max  = max_c (peso_criterio · gravidade_criterio)
--                          domina o score (não-compensatório, Handbook §6.13)
--   gravidade_termo_soma = sum_c (peso_criterio · gravidade_criterio)
--                          bônus por carga (linear-aditivo, Handbook §6.10)
--
-- ETAPA 6 — score composto da paciente — Eq. (3) [multiplicativa]
--   gravidade_total = gravidade_termo_max · (1 + α · 1_{gestante})
--                   + peso_carga_total · gravidade_termo_soma
--   onde α = multiplicador_gestante (1.0 ⇒ gestante DOBRA o termo_max)
--
-- ETAPA 7 — reescala 0-100 com clip (apresentação)
--   teto = p95(gravidade_total)  -- dinâmico, recomputado a cada run
--   gravidade_total_0_100 = 100 · min(gravidade_total / teto, 1)
--
-- POPULAÇÃO INCLUÍDA: pacientes com pelo menos um critério ativo OU que
-- são gestantes. Gestantes-sem-critério-ativo entram no mart com
-- gravidade_total = 0 (visibilidade da coorte, sem score).
--
-- Para os critérios, mecanismos de desativação e fórmula da gravidade por
-- critério, ver int_monitora_cancer__gravidade_instancias.sql.
--
-- REFERÊNCIA METODOLÓGICA
--   Nardo, M., Saisana, M., Saltelli, A., Tarantola, S., Hoffman, A.,
--   Giovannini, E. (2008). Handbook on Constructing Composite Indicators:
--   Methodology and User Guide. OECD/JRC.

with
    -- Etapa 3 — colapso MAX por (cpf, critério): mantém a instância mais
    -- grave de cada critério da paciente (ver doc-block).
    gravidade_max_por_criterio as (
        select
            cpf_particao,
            criterio,
            etapa,
            data_trigger,
            dias_atraso,
            intervalo_urgencia_dias,
            risco_evento_gatilho,
            peso_criterio,
            fator_tempo,
            fator_risco,
            gravidade_criterio,
            gestante
        from {{ ref("int_monitora_cancer__gravidade_instancias") }}
        qualify row_number() over (
            partition by cpf_particao, criterio
            order by gravidade_criterio desc, data_trigger desc
        ) = 1
    ),

    -- Etapa 4 — Eq. (2): contribuicao_criterio = peso · gravidade_criterio.
    contribuicao_por_criterio as (
        select
            *,
            peso_criterio * gravidade_criterio as contribuicao_criterio
        from gravidade_max_por_criterio
    ),

    -- Etapa 5 — agrega termo_max + termo_soma por paciente; single-pass
    -- com o ARRAY_AGG do detalhamento.
    agregado as (
        select
            cpf_particao,
            max(contribuicao_criterio) as contribuicao_max,
            sum(contribuicao_criterio) as contribuicao_soma,
            array_agg(
                struct(
                    criterio,
                    etapa,
                    data_trigger,
                    dias_atraso,
                    intervalo_urgencia_dias,
                    risco_evento_gatilho,
                    fator_tempo,
                    fator_risco,
                    gravidade_criterio,
                    peso_criterio,
                    contribuicao_criterio
                )
                order by contribuicao_criterio desc, criterio
            ) as gravidade_detalhamento
        from contribuicao_por_criterio
        group by cpf_particao
    ),

    -- Universo de pacientes do run atual (com OU sem critério ativo).
    -- Seed do LEFT JOIN abaixo — única via pela qual gestantes-sem-critério
    -- entram no mart.
    universo_pacientes as (
        select distinct
            cpf_particao,
            gestante
        from {{ ref("int_monitora_cancer__eventos_episodios") }}
        qualify run_id = max(run_id) over (partition by cpf_particao)
    ),

    -- Computa gravidade_base = termo_max + peso_carga_total · termo_soma
    -- (score do não-gestante, exposto para auditoria). LEFT JOIN inclui
    -- gestantes-sem-critério (termos = 0, score = 0); WHERE descarta
    -- não-gestantes sem critério (nada a reportar).
    agregado_score as (
        select
            u.cpf_particao,
            coalesce(a.contribuicao_max, 0) as gravidade_termo_max,
            coalesce(a.contribuicao_soma, 0) as gravidade_termo_soma,
            coalesce(a.contribuicao_max, 0)
                + {{ peso_carga_total }} * coalesce(a.contribuicao_soma, 0)
                as gravidade_base,
            coalesce(u.gestante, false) as gestante,
            a.gravidade_detalhamento
        from universo_pacientes as u
            left join agregado as a using (cpf_particao)
        where a.cpf_particao is not null
            or coalesce(u.gestante, false) = true
    ),

    -- Etapa 6 — Eq. (3): aplica o multiplicador de gestante sobre o
    -- termo_max (ver doc-block).
    score_paciente as (
        select
            cpf_particao,
            gravidade_termo_max * if(gestante, 1 + {{ multiplicador_gestante }}, 1)
                + {{ peso_carga_total }} * gravidade_termo_soma
                as gravidade_total,
            gravidade_base,
            gravidade_termo_max,
            gravidade_termo_soma,
            gestante,
            gravidade_detalhamento
        from agregado_score
    ),

    -- Teto p95 dinâmico para a reescala 0-100 (~5% satura em 100, spec
    -- §4.7). Recomputado a cada run; custo: aggregation em memória, sem
    -- scan adicional. Trade-off: gravidade_total_0_100 pode flutuar entre
    -- runs com a distribuição — use gravidade_total (bruto) para ordenar.
    teto_apresentacao as (
        select approx_quantiles(gravidade_total, 100)[offset(95)] as teto
        from score_paciente
    )

-- Etapa 7 — apresentação: gravidade_total_0_100 = clip no teto p95.
-- Use gravidade_total (bruto) para ordenar a fila.
select
    sp.cpf_particao,
    sp.gravidade_total,
    coalesce(least(safe_divide(sp.gravidade_total, t.teto), 1.0) * 100, 0)
        as gravidade_total_0_100,
    sp.gravidade_base,
    sp.gravidade_termo_max,
    sp.gravidade_termo_soma,
    sp.gestante,
    sp.gravidade_detalhamento
from score_paciente as sp
    cross join teto_apresentacao as t
