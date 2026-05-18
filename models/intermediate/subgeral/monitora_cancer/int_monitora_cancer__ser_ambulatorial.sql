-- Eventos de procedimentos de mama extraídos do SER (regulação estadual ambulatorial)
{% set data_inicio_monitoramento = "2021-01-01" %}

with
    procedimentos as (
        select *
        from {{ ref("int_monitora_cancer__parametros_ser") }}
    ),

    base as (
        select
            *,
            safe_cast(cod_recurso_solicitado as int) as id_procedimento_solicitado,
            safe_cast(cod_recurso_regulado as int) as id_procedimento_regulado
        from {{ ref("raw_ser_metabase__ambulatorial") }}
        where data_solicitacao >= "{{ data_inicio_monitoramento }}"
            -- Filtro por id_procedimento dos parâmetros (fonte de verdade da inclusão).
            -- safe_cast(... as int): cod_recurso_* é STRING no source; o cast
            -- normaliza eventuais zeros à esquerda para casar com o INT da fonte.
            and (
                safe_cast(cod_recurso_solicitado as int) in (
                    select id_procedimento from procedimentos
                )
                or safe_cast(cod_recurso_regulado as int) in (
                    select id_procedimento from procedimentos
                )
            )
    ),

    enriquecido as (
        select
            base.*,
            ps.procedimento as procedimento_solicitado_seed,
            ps.criterio_suspeita as suspeita_solicitado,
            ps.criterio_diagnostico as diagnostico_solicitado,
            pr.procedimento as procedimento_regulado_seed,
            pr.criterio_suspeita as suspeita_regulado,
            pr.criterio_diagnostico as diagnostico_regulado,
            coalesce(pr.limite_dias_solicitacao_autorizacao, ps.limite_dias_solicitacao_autorizacao) as limite_dias_solicitacao_autorizacao,
            coalesce(pr.limite_dias_autorizacao_execucao, ps.limite_dias_autorizacao_execucao) as limite_dias_autorizacao_execucao,
            coalesce(pr.limite_dias_regulacao, ps.limite_dias_regulacao) as limite_dias_regulacao
        from base
            left join procedimentos as ps
                on base.id_procedimento_solicitado = ps.id_procedimento
            left join procedimentos as pr
                on base.id_procedimento_regulado = pr.id_procedimento
    )

select
-- pk
    "SER" as sistema_origem,
    "REGULACAO" as sistema_tipo,
    id_solicitacao as id_sistema_origem,

-- paciente
    paciente_cns,
    cast(NULL as int) as paciente_cpf_sisreg,

-- unidades
    id_cnes_unidade_origem,
    id_cnes_unidade_executante,

-- qualificacao
    cid,
    solicitacao_estado as evento_status,
    coalesce(procedimento_regulado_seed, procedimento_solicitado_seed) as procedimento,

-- datas
    data_solicitacao,
    data_agendamento as data_autorizacao,
    data_execucao,

-- resultados siscan (não aplicável)
    cast(NULL as date) as data_exame_resultado,
    cast(NULL as string) as mama_esquerda_resultado,
    cast(NULL as string) as mama_direita_resultado,

-- indicadores
    coalesce(suspeita_solicitado, false)
        or coalesce(suspeita_regulado, false) as criterio_suspeita,
    coalesce(diagnostico_solicitado, false)
        or coalesce(diagnostico_regulado, false) as criterio_diagnostico,

-- atraso (severidade 0-3) por etapa da regulação.
-- Regra: 0 se interval < limite; 1 se >= limite; 2 se >= limite + 10; 3 se >= limite + 20.
-- NULL quando alguma das datas envolvidas estiver ausente.
    case
        when date_diff(data_agendamento, data_solicitacao, day) is null then null
        when date_diff(data_agendamento, data_solicitacao, day) >= limite_dias_solicitacao_autorizacao + 20 then 3
        when date_diff(data_agendamento, data_solicitacao, day) >= limite_dias_solicitacao_autorizacao + 10 then 2
        when date_diff(data_agendamento, data_solicitacao, day) >= limite_dias_solicitacao_autorizacao then 1
        else 0
    end as atraso_solicitacao_autorizacao,
    case
        when date_diff(data_execucao, data_agendamento, day) is null then null
        when date_diff(data_execucao, data_agendamento, day) >= limite_dias_autorizacao_execucao + 20 then 3
        when date_diff(data_execucao, data_agendamento, day) >= limite_dias_autorizacao_execucao + 10 then 2
        when date_diff(data_execucao, data_agendamento, day) >= limite_dias_autorizacao_execucao then 1
        else 0
    end as atraso_autorizacao_execucao,
    case
        when date_diff(data_execucao, data_solicitacao, day) is null then null
        when date_diff(data_execucao, data_solicitacao, day) >= limite_dias_regulacao + 20 then 3
        when date_diff(data_execucao, data_solicitacao, day) >= limite_dias_regulacao + 10 then 2
        when date_diff(data_execucao, data_solicitacao, day) >= limite_dias_regulacao then 1
        else 0
    end as atraso_regulacao,

-- risco (float64): prioridade do SER convertida para numérico.
-- Casos inválidos (ex.: string 'nan' ou texto não numérico) viram NULL.
-- Subquery + unnest garante uma única avaliação do safe_cast por linha.
    (
        select if(is_nan(p), null, p)
        from unnest([safe_cast(prioridade as float64)]) as p
    ) as risco

from enriquecido
