-- noqa: disable=LT08

-- Eventos de monitoramento de câncer de mama em forma longa, com janelas
-- temporais (dias_proximo_evento), identificador de episódio (run_id) e
-- agregados por paciente (tempo_total, tempo_diagnostico,
-- tempo_diagnostico_sem_tratamento) broadcast em cada linha.
--
-- Episódios ("runs"): sequências consecutivas de eventos cujos gaps entre
-- data_referencia_evento não excedem `episodio_gap_dias` (default 180 dias).
-- O run_id incrementa a cada gap > episodio_gap_dias dentro do mesmo paciente.
--
-- tempo_total:
--   - pacientes com SER: dias do início da run que contém o primeiro SER até
--     a data desse primeiro SER;
--   - pacientes sem SER: dias do início da última run até a data atual.
--
-- tempo_diagnostico / tempo_diagnostico_sem_tratamento: medidos no run atual
-- (run_id = max(run_id), como int_monitora_cancer__eventos_run_atual):
--   - tempo_diagnostico: dias do início do run atual até o 1º evento que vira
--     DIAGNOSTICO/UNACON (criterio_diagnostico OU fonte = 'SER'). NULL em SUSPEITA.
--   - tempo_diagnostico_sem_tratamento: dias do 1º diagnóstico confirmado
--     (criterio_diagnostico) até a 1ª solicitação SER. NULL sem diagnóstico
--     em/antes do SER (só assume valor para UNACON).
--
-- Granularidade: 1 linha por evento (mesma granularidade de mart_monitora_cancer__fatos
-- restrita à população-alvo).

{% set episodio_gap_dias = var('episodio_gap_dias', 180) %}

-- Materializado como `table` para compartilhar computações entre
-- mart_monitora_cancer__gravidade e mart_monitora_cancer__pacientes_linha_tempo,
-- evitando trabalho repetido.
-- Antes, como ephemeral, era inlinado 12 vezes em gravidade.
-- Particionado por cpf_particao para pruning em consultas pontuais.
-- Clusterizado por ['fonte', 'procedimento'] para acelerar filtros das CTEs de critério em gravidade_instancias.
{{
    config(
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
},
cluster_by = ["fonte", "procedimento"],
)
}}

-- event_order: data_referencia_evento, data_solicitacao, data_autorizacao, data_execucao, data_resultado

with
    eventos as (
        select
            lpad(safe_cast(fcts.paciente_cpf as string), 11, '0') as cpf,
            fcts.paciente_cpf as cpf_particao,

            -- dados basicos paciente
            pop.nome,
            pop.raca_cor,
            pop.idade,
            pop.clinica_sf_ap as ap,
            pop.clinica_sf as cf,
            pop.equipe_sf,
            pop.status,
            pop.telefone,
            pop.clinica_sf_telefone as telefone_cf,
            pop.equipe_sf_telefone as telefone_esf,
            pop.gestante,

            -- dados evento
            fcts.sistema_origem as fonte,
            fcts.sistema_tipo as tipo,
            fcts.procedimento,
            fcts.cid,
            concat(
                coalesce(fcts.id_cnes_unidade_origem, 'CNES ?'),
                ' - ',
                coalesce(fcts.estabelecimento_origem_nome, 'Estabelecimento não identificado')
            ) as unidade_solicitante,
            concat(
                coalesce(fcts.id_cnes_unidade_executante, 'CNES ?'),
                ' - ',
                coalesce(fcts.estabelecimento_executante_nome, 'Estabelecimento não identificado')
            ) as unidade_executante,
            fcts.data_solicitacao,
            fcts.data_autorizacao,
            fcts.data_execucao,
            fcts.data_exame_resultado as data_resultado,
            fcts.mama_esquerda_resultado,
            fcts.mama_direita_resultado,
            fcts.criterio_diagnostico,
            fcts.evento_status,
            fcts.atraso_solicitacao_autorizacao,
            fcts.atraso_autorizacao_execucao,
            fcts.atraso_regulacao,
            fcts.risco,
            (
                select max(d)
                from unnest ([
                    fcts.data_solicitacao,
                    fcts.data_autorizacao,
                    fcts.data_execucao,
                    fcts.data_exame_resultado
                ]) as d
            ) as data_referencia_evento

        from {{ ref("int_monitora_cancer__populacao_alvo") }} as pop
            left join {{ref("mart_monitora_cancer__fatos")}} as fcts
            on pop.paciente_cpf = fcts.paciente_cpf
    ),

    eventos_com_janela as (
        select
            *,
            date_diff(
                lead(data_referencia_evento) over evento_order,
                data_referencia_evento,
                day
            ) as dias_proximo_evento,
            lag(data_referencia_evento) over evento_order as data_referencia_evento_anterior
        from eventos
            window evento_order as (
                partition by cpf_particao
                order by -- event_order
                    data_referencia_evento,
                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_resultado
            )
    ),

    eventos_com_run as (
        select
            *,
            sum(
                case
                    when data_referencia_evento_anterior is null then 1
                    when date_diff(
                        data_referencia_evento,
                        data_referencia_evento_anterior,
                        day
                    ) > {{ episodio_gap_dias }} then 1
                    else 0
                end
            ) over (evento_order rows between unbounded preceding and current row) as run_id
        from eventos_com_janela
            window evento_order as (
                partition by cpf_particao
                order by -- event_order
                    data_referencia_evento,
                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_resultado
            )
    ),

    -- "Início" da run = data de solicitação do primeiro evento da sequência.
    -- A sequência (run) é definida pelas fronteiras em data_referencia_evento
    -- (gaps > episodio_gap_dias quebram a run); aqui usamos a data_solicitacao
    -- mais antiga dentro da run como marcador de quando a paciente entrou
    -- naquele percurso de cuidado.
    run_starts as (
        select
            cpf_particao,
            run_id,
            min(data_solicitacao) as run_start_data
        from eventos_com_run
        group by cpf_particao, run_id
    ),

    primeira_ser_info as (
        select
            cpf_particao,
            data_solicitacao as primeira_ser_data,
            run_id as primeira_ser_run_id
        from eventos_com_run
        where fonte = 'SER'
            qualify row_number() over evento_order = 1
            window evento_order as (
                partition by cpf_particao
                order by -- event_order
                    data_referencia_evento,
                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_resultado
            )
    ),

    ultimo_evento_por_paciente as (
        select
            cpf_particao,
            data_referencia_evento as ultima_data_referencia,
            run_id as ultimo_run_id
        from eventos_com_run
            qualify row_number() over evento_order_desc = 1
            window evento_order_desc as (
                partition by cpf_particao
                order by -- event_order desc
                    data_referencia_evento desc,
                    data_solicitacao desc,
                    data_autorizacao desc,
                    data_execucao desc,
                    data_resultado desc
            )
    ),

    tempo_total_por_paciente as (
        select
            uep.cpf_particao,
            nullif(
                cast(
                    date_diff(
                        if(psi.cpf_particao is not null, psi.primeira_ser_data, current_date('America/Sao_Paulo')),
                        if(psi.cpf_particao is not null, rs_ser.run_start_data, rs_ultimo.run_start_data),
                        day
                    )
                    as int64
                ),
                0
            ) as tempo_total
        from ultimo_evento_por_paciente as uep
            join run_starts as rs_ultimo
            on uep.cpf_particao = rs_ultimo.cpf_particao
            and uep.ultimo_run_id = rs_ultimo.run_id
            left join primeira_ser_info as psi
            on uep.cpf_particao = psi.cpf_particao
            left join run_starts as rs_ser
            on psi.cpf_particao = rs_ser.cpf_particao
            and psi.primeira_ser_run_id = rs_ser.run_id
    ),

    -- Run atual (run_id = max(run_id)). Replica int_monitora_cancer__eventos_run_atual
    -- (downstream deste — evita ciclo). Base dos tempos de diagnóstico abaixo.
    eventos_run_atual as (
        select *
        from eventos_com_run
        qualify run_id = max(run_id) over (partition by cpf_particao)
    ),

    -- Início do run atual: data_solicitacao mais antiga (mesmo marcador de run_starts).
    run_atual_start as (
        select
            cpf_particao,
            min(data_solicitacao) as run_atual_start_data
        from eventos_run_atual
        group by cpf_particao
    ),

    -- 1º evento do run atual que vira DIAGNOSTICO/UNACON (criterio_diagnostico OU SER).
    marco_diagnostico_run_atual as (
        select
            cpf_particao,
            data_referencia_evento as marco_data
        from eventos_run_atual
        where criterio_diagnostico = true or fonte = 'SER'
            qualify row_number() over evento_order = 1
            window evento_order as (
                partition by cpf_particao
                order by -- event_order
                    data_referencia_evento,
                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_resultado
            )
    ),

    -- 1º diagnóstico confirmado do run atual (criterio_diagnostico).
    diagnostico_run_atual as (
        select
            cpf_particao,
            data_referencia_evento as diagnostico_data
        from eventos_run_atual
        where criterio_diagnostico = true
            qualify row_number() over evento_order = 1
            window evento_order as (
                partition by cpf_particao
                order by -- event_order
                    data_referencia_evento,
                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_resultado
            )
    ),

    -- 1ª solicitação SER do run atual.
    ser_run_atual as (
        select
            cpf_particao,
            data_solicitacao as primeira_ser_data
        from eventos_run_atual
        where fonte = 'SER'
            qualify row_number() over evento_order = 1
            window evento_order as (
                partition by cpf_particao
                order by -- event_order
                    data_referencia_evento,
                    data_solicitacao,
                    data_autorizacao,
                    data_execucao,
                    data_resultado
            )
    ),

    -- Broadcast por paciente. Dirigida por marco_diagnostico_run_atual: quem não
    -- tem marco no run atual (SUSPEITA) não entra e fica NULL via LEFT JOIN final.
    tempos_diagnostico_por_paciente as (
        select
            mdr.cpf_particao,
            -- (i) >= 0 por construção (o marco está no run que começa no min da solicitação)
            date_diff(mdr.marco_data, ras.run_atual_start_data, day) as tempo_diagnostico,
            -- (ii) NULL quando não há diagnóstico em/antes do SER
            if(
                dxr.diagnostico_data is not null
                    and ser.primeira_ser_data is not null
                    and ser.primeira_ser_data >= dxr.diagnostico_data,
                date_diff(ser.primeira_ser_data, dxr.diagnostico_data, day),
                null
            ) as tempo_diagnostico_sem_tratamento
        from marco_diagnostico_run_atual as mdr
            left join run_atual_start as ras using (cpf_particao)
            left join diagnostico_run_atual as dxr using (cpf_particao)
            left join ser_run_atual as ser using (cpf_particao)
    )

select
    ev.cpf_particao,
    ev.cpf,

    ev.nome,
    ev.raca_cor,
    ev.idade,
    ev.ap,
    ev.cf,
    ev.equipe_sf,
    ev.status,
    ev.telefone,
    ev.telefone_cf,
    ev.telefone_esf,
    ev.gestante,

    ev.fonte,
    ev.tipo,
    ev.evento_status,
    ev.procedimento,
    ev.cid,
    ev.unidade_solicitante,
    ev.unidade_executante,

    ev.data_solicitacao,
    ev.data_autorizacao,
    ev.data_execucao,
    ev.data_resultado,
    ev.data_referencia_evento,

    ev.mama_esquerda_resultado,
    ev.mama_direita_resultado,
    ev.criterio_diagnostico,

    ev.atraso_solicitacao_autorizacao,
    ev.atraso_autorizacao_execucao,
    ev.atraso_regulacao,
    ev.risco,

    ev.dias_proximo_evento,
    ev.run_id,

    ttp.tempo_total,
    tdp.tempo_diagnostico,
    tdp.tempo_diagnostico_sem_tratamento

from eventos_com_run as ev
    left join tempo_total_por_paciente as ttp using (cpf_particao)
    left join tempos_diagnostico_por_paciente as tdp using (cpf_particao)
