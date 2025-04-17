-- noqa: disable=LT08
{{
    config(
        enabled=true,
        schema="projeto_sisreg_tme",
        alias="procedimento_mensal",
        materialized="table",
    )
}}  -- Implementar particionamento

-- ##############################################################
-- --------------------------------------------------------------
-- Objetivo:
-- • Construir uma série contínua mês × procedimento, mesmo se
-- não houve solicitações em determinado mês.
-- • Calcular métricas robustas de tempo de espera (média,
-- mediana, p90, desvio‑padrão, IC‑95 %).
--
-- Motivos principais das decisões:
-- • Garantir calendário completo evita "encurtar" janelas móveis.
-- • Métricas além da média reduzem impacto de outliers.
-- • Flags de amostragem permitem alertar o usuário sobre
-- insuficiência de dados.
-- ##############################################################
-- ───────────────────────────────────────────────────────────────
-- 1. CALENDÁRIO MENSAL COMPLETO
-- • Usa generate_date_array() pois aceita passo de 1 mês.
-- • Convertido para TIMESTAMP no fuso America/Sao_Paulo
-- para manter coerência com horários locais.
-- ───────────────────────────────────────────────────────────────
with
    calendario as (
        select
            -- Primeiro dia de cada mês como TIMESTAMP,
            -- preservando meia‑noite no horário de São Paulo.
            timestamp(datetime(d), 'America/Sao_Paulo') as ts_inicio_mes,

            -- Guarda ano e mês em separado porque serão úteis
            -- para partição/clusterização e ordenação.
            extract(year from d) as ano,
            extract(month from d) as mes
        from
            unnest(
                generate_date_array(
                    date '2018-01-01',  -- Início histórico escolhido.
                    current_date('America/Sao_Paulo'),  -- Data atual no fuso local.
                    interval 1 month  -- Passo de 1 mês.
                )
            ) as d
    ),

    -- ───────────────────────────────────────────────────────────────
    -- 2. LISTA ÚNICA DE PROCEDIMENTOS
    -- ───────────────────────────────────────────────────────────────
    procedimentos as (
        select distinct procedimento_descricao as procedimento
        from {{ ref("mart_tme__tempos_espera") }}
    ),

    -- ───────────────────────────────────────────────────────────────
    -- 3. GRADE MÊS × PROCEDIMENTO
    -- • cross join entre calendário e procedimentos gera TODAS
    -- as combinações possíveis; isso garante que mesmo meses
    -- sem execuções apareçam na série.
    -- ───────────────────────────────────────────────────────────────
    base as (
        select c.ts_inicio_mes as mes_competencia_ts, p.procedimento
        from calendario c
        cross join procedimentos p
    ),

    -- ───────────────────────────────────────────────────────────────
    -- 4. FATOS TRUNCADOS AO MÊS
    -- • timestamp_trunc(..., 'America/Sao_Paulo') assegura que
    -- dados registrados perto da meia‑noite não "escorram"
    -- para o mês vizinho devido ao UTC padrão.
    -- ───────────────────────────────────────────────────────────────
    fato as (
        select
            timestamp_trunc(
                data_marcacao, month, 'America/Sao_Paulo'
            ) as mes_competencia_ts,
            procedimento_descricao as procedimento,
            tempo_espera
        from {{ ref("mart_tme__tempos_espera") }}
    ),

    -- ───────────────────────────────────────────────────────────────
    -- 5. AGREGAÇÃO + MÉTRICAS ROBUSTAS
    -- • count, avg = estatísticas básicas.
    -- • approx_quantiles → mediana (p50) e p90 (robustos/outliers).
    -- • Intervalo de Confiança 95 % só é calculado para n ≥ 30
    -- (regra empírica para CLT).
    -- ───────────────────────────────────────────────────────────────
    agregado as (
        select
            b.mes_competencia_ts,
            extract(year from b.mes_competencia_ts) as ano_marcacao,
            extract(month from b.mes_competencia_ts) as mes_marcacao,
            b.procedimento,

            -- Contagem de execuções
            count(f.tempo_espera) as n_execucoes,

            -- Tempo médio de espera
            round(avg(f.tempo_espera), 2) as tme,

            -- Mediana via approx_quantiles (mais robusto a outliers)
            round(approx_quantiles(f.tempo_espera, 100)[offset(50)], 2) as te_mediano,

            -- Percentil 90 → visão de cauda longa
            round(approx_quantiles(f.tempo_espera, 10)[offset(9)], 2) as te_p90,

            -- Desvio‑padrão populacional
            round(stddev_pop(f.tempo_espera), 2) as desvio_padrao,

            -- Intervalo de confiança inferior (95 %) da média
            case
                when count(f.tempo_espera) >= 30
                then
                    round(
                        avg(f.tempo_espera)
                        - 1.96
                        * stddev_pop(f.tempo_espera)
                        / sqrt(count(f.tempo_espera)),
                        2
                    )
            end as ic95_inf,

            -- Intervalo de confiança superior (95 %) da média
            case
                when count(f.tempo_espera) >= 30
                then
                    round(
                        avg(f.tempo_espera)
                        + 1.96
                        * stddev_pop(f.tempo_espera)
                        / sqrt(count(f.tempo_espera)),
                        2
                    )
            end as ic95_sup

        from base b
        left join
            fato f
            on f.mes_competencia_ts = b.mes_competencia_ts
            and f.procedimento = b.procedimento
        group by 1, 2, 3, 4
    ),

    medias_moveis as (
        select
            a.*,

            -- ───── Média móvel (3 meses) ─────
            round(
                avg(tme) over (
                    partition by procedimento
                    order by mes_competencia_ts
                    rows between 2 preceding and current row
                ),
                2
            ) as tme_movel_3m,

            -- ───── Média móvel (6 meses) ─────
            round(
                avg(tme) over (
                    partition by procedimento
                    order by mes_competencia_ts
                    rows between 5 preceding and current row
                ),
                2
            ) as tme_movel_6m,

            -- ───── Média móvel (12 meses) ─────
            round(
                avg(tme) over (
                    partition by procedimento
                    order by mes_competencia_ts
                    rows between 11 preceding and current row
                ),
                2
            ) as tme_movel_12m,

        from agregado a
    )

select * except (mes_competencia_ts)
from medias_moveis
order by ano_marcacao desc, mes_marcacao desc, tme desc, n_execucoes desc
