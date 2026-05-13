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
    )

select
-- pk
    "SISREG" as sistema_origem,
    "REGULACAO" as sistema_tipo,
    safe_cast(base.id_solicitacao as int) as id_sistema_origem,

-- paciente
    base.paciente_cns,
    safe_cast(base.paciente_cpf as int) as paciente_cpf_sisreg,

-- unidades
    base.id_cnes_unidade_solicitante as id_cnes_unidade_origem,
    base.id_cnes_unidade_executante,

-- qualificacao
    base.cid_solicitacao as cid,
    base.solicitacao_status as evento_status,
    proc.procedimento,

-- datas
    safe_cast(base.data_solicitacao as date) as data_solicitacao,
    safe_cast(base.data_autorizacao as date) as data_autorizacao,
    safe_cast(base.data_execucao as date) as data_execucao,

-- resultados siscan (não aplicável)
    cast(NULL as date) as data_exame_resultado,
    cast(NULL as string) as mama_esquerda_resultado,
    cast(NULL as string) as mama_direita_resultado,

-- indicadores
    proc.criterio_suspeita as criterio_suspeita,
    proc.criterio_diagnostico as criterio_diagnostico

from base
    inner join procedimentos as proc
        on safe_cast(base.id_procedimento_sisreg as int) = proc.id_procedimento
