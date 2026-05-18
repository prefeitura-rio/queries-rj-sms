-- Eventos de procedimentos de mama extraídos do SISREG (regulação ambulatorial)
{% set data_inicio_monitoramento = "2021-01-01" %}

-- Sobre o JOIN em id_procedimento_sisreg:
-- mart_sisreg__solicitacoes une duas CTEs (`solicitacoes` e `marcacoes`) que
-- entregam id_procedimento_sisreg em FORMATOS DIFERENTES:
--   • raw_sisreg_api__marcacoes      → STRING zero-padded para 7 chars (ex.: "0703716")
--   • raw_sisreg_api__solicitacoes   → STRING sem padding         (ex.: "703716")
-- Para casar com o INT dos parâmetros, usamos safe_cast(... as int) na coluna source.
-- Custo: o cast envolve a chave de cluster (id_procedimento_sisreg é a 3ª
-- chave de cluster_by) e zera cluster pruning desse predicado. Mantido por
-- correção até o upstream normalizar (issue: padronizar lpad em
-- raw_sisreg_api__solicitacoes.procedimento_id).

with
    procedimentos as (
        select *
        from {{ ref("int_monitora_cancer__parametros_sisreg") }}
    ),

    base as (
        select *
        from {{ ref("mart_sisreg__solicitacoes") }}
        where data_solicitacao >= "{{ data_inicio_monitoramento }}"
            -- Pushdown do filtro de id_procedimento para o scan da fonte:
            -- reduz o row count que entra nos joins/transformações downstream.
            and safe_cast(id_procedimento_sisreg as int) in (
                select id_procedimento from procedimentos
            )
    ),

    -- Joina parametros e pré-casta datas para que o cálculo
    -- de atraso abaixo possa referenciar as colunas date-tipadas sem
    -- repetir safe_cast em cada case-when
    enriquecido as (
        select
            base.id_solicitacao,
            base.paciente_cns,
            base.paciente_cpf,
            base.id_cnes_unidade_solicitante,
            base.id_cnes_unidade_executante,
            base.cid_solicitacao,
            base.solicitacao_status,
            proc.procedimento,
            safe_cast(base.data_solicitacao as date) as data_solicitacao,
            safe_cast(base.data_autorizacao as date) as data_autorizacao,
            safe_cast(base.data_execucao as date) as data_execucao,
            proc.criterio_suspeita,
            proc.criterio_diagnostico,
            proc.limite_dias_solicitacao_autorizacao,
            proc.limite_dias_autorizacao_execucao,
            proc.limite_dias_regulacao
        from base
            inner join procedimentos as proc
                on safe_cast(base.id_procedimento_sisreg as int) = proc.id_procedimento
    )

select
-- pk
    "SISREG" as sistema_origem,
    "REGULACAO" as sistema_tipo,
    safe_cast(id_solicitacao as int) as id_sistema_origem,

-- paciente
    paciente_cns,
    safe_cast(paciente_cpf as int) as paciente_cpf_sisreg,

-- unidades
    id_cnes_unidade_solicitante as id_cnes_unidade_origem,
    id_cnes_unidade_executante,

-- qualificacao
    cid_solicitacao as cid,
    solicitacao_status as evento_status,
    procedimento,

-- datas
    data_solicitacao,
    data_autorizacao,
    data_execucao,

-- resultados siscan (não aplicável)
    cast(NULL as date) as data_exame_resultado,
    cast(NULL as string) as mama_esquerda_resultado,
    cast(NULL as string) as mama_direita_resultado,

-- indicadores
    criterio_suspeita,
    criterio_diagnostico,

-- atraso (severidade 0-3) por etapa da regulação.
-- Regra: 0 se interval < limite; 1 se >= limite; 2 se >= limite + 10; 3 se >= limite + 20.
-- NULL quando alguma das datas envolvidas estiver ausente.
    case
        when date_diff(data_autorizacao, data_solicitacao, day) is null then null
        when date_diff(data_autorizacao, data_solicitacao, day) >= limite_dias_solicitacao_autorizacao + 20 then 3
        when date_diff(data_autorizacao, data_solicitacao, day) >= limite_dias_solicitacao_autorizacao + 10 then 2
        when date_diff(data_autorizacao, data_solicitacao, day) >= limite_dias_solicitacao_autorizacao then 1
        else 0
    end as atraso_solicitacao_autorizacao,
    case
        when date_diff(data_execucao, data_autorizacao, day) is null then null
        when date_diff(data_execucao, data_autorizacao, day) >= limite_dias_autorizacao_execucao + 20 then 3
        when date_diff(data_execucao, data_autorizacao, day) >= limite_dias_autorizacao_execucao + 10 then 2
        when date_diff(data_execucao, data_autorizacao, day) >= limite_dias_autorizacao_execucao then 1
        else 0
    end as atraso_autorizacao_execucao,
    case
        when date_diff(data_execucao, data_solicitacao, day) is null then null
        when date_diff(data_execucao, data_solicitacao, day) >= limite_dias_regulacao + 20 then 3
        when date_diff(data_execucao, data_solicitacao, day) >= limite_dias_regulacao + 10 then 2
        when date_diff(data_execucao, data_solicitacao, day) >= limite_dias_regulacao then 1
        else 0
    end as atraso_regulacao

from enriquecido
