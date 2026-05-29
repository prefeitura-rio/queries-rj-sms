-- noqa: disable=LT08

-- Pendências atuais de cada paciente do monitoramento de câncer de mama.
-- Granularidade: 1 linha por paciente (cpf_particao), com um array
-- pendencia_atual contendo zero ou mais rótulos de pendência.
--
-- Regra geral: todas as pendências SISREG/SER são avaliadas sobre o ÚLTIMO
-- evento da linha do tempo da paciente. Pendências da família "Outros"
-- olham apenas para status e tempo_total da paciente.
--
-- Famílias de pendência:
--   • SISREG — último evento da linha do tempo é do SISREG:
--       - "Pendente de autorização SISREG"
--       - "Pendente de realização SISREG"
--       - "Aguardando execução SISREG"
--       - "Devolvido SISREG"
--       - "Falta SISREG"
--   • SER — último evento da linha do tempo é do SER:
--       - "Pendente de autorização SER"
--       - "Pendente de realização SER"
--       - "Aguardando execução SER"
--       - "Procedimento cancelado SER"
--       - "Falta SER"
--   • Outros — status da paciente + tempo_total. As 3 pendências abaixo
--     descrevem a mesma situação (paciente com diagnóstico ainda sem UNACON)
--     em níveis de gravidade crescentes; apenas a MAIS GRAVE é emitida:
--       - "Prazo legal para início de tratamento ultrapassado"
--         (status = 'DIAGNOSTICO' e tempo_total >= 60)
--       - "Prazo para início de tratamento próximo do limite legal"
--         (status = 'DIAGNOSTICO' e 45 <= tempo_total < 60)
--       - "Pendente de solicitação para UNACON"
--         (status = 'DIAGNOSTICO' e tempo_total < 45)
--
-- Convenções:
--   • "Último evento" é a linha com maior data_referencia_evento (com as
--     demais datas como desempate), seguindo a mesma ordenação usada em
--     int_monitora_cancer__eventos_episodios. Quando há empate exato, uma
--     linha é escolhida arbitrariamente via row_number.
--   • "Aguardando execução": data_execucao está preenchida e ainda no
--     futuro (data_execucao > hoje), indicando que a paciente está
--     aguardando a data agendada para execução.
--   • tempo_total já é calculado em eventos_episodios: para pacientes sem
--     SER, equivale aos dias decorridos do início da última run até hoje —
--     usado como proxy do tempo desde o ingresso no percurso de cuidado.
--   • Os valores de evento_status para SER são preservados de raw_ser_metabase
--     (coluna solicitacao_estado), que armazena 'CHEGADA_NAO_CONFIRMADA',
--     'CANCELADA' etc. com underscores. O caminho consumido aqui
--     (raw → int_* → mart__fatos → eventos_episodios → pendencias) preserva
--     a forma literal; o match abaixo usa essa forma. Nota: o mart
--     pacientes_linha_tempo, em ramo separado, substitui '_' por ' ' apenas
--     na emissão de eventos.evento_status — não afeta este modelo.

{{
    config(
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    eventos as (
        select *
        from {{ ref("int_monitora_cancer__eventos_episodios") }}
    ),

    ultimo_evento as (
        select
            cpf_particao,
            status,
            tempo_total,
            fonte as ultima_fonte,
            evento_status as ultimo_evento_status,
            data_solicitacao as ultima_data_solicitacao,
            data_autorizacao as ultima_data_autorizacao,
            data_execucao as ultima_data_execucao
        from eventos
        qualify row_number() over (
            partition by cpf_particao
            order by
                data_referencia_evento desc,
                data_solicitacao desc,
                data_autorizacao desc,
                data_execucao desc,
                data_resultado desc
        ) = 1
    )

select
    ue.cpf_particao,
    array_concat(
        -- ── SISREG (último evento é SISREG) ───────────────────────────────
        if(
            ue.ultima_fonte = 'SISREG'
            and ue.ultima_data_solicitacao is not null
            and ue.ultima_data_autorizacao is null,
            ['Pendente de autorização SISREG'],
            []
        ),
        if(
            ue.ultima_fonte = 'SISREG'
            and ue.ultima_data_autorizacao is not null
            and ue.ultima_data_execucao is null,
            ['Pendente de realização SISREG'],
            []
        ),
        if(
            ue.ultima_fonte = 'SISREG'
            and ue.ultima_data_execucao is not null
            and ue.ultima_data_execucao > current_date('America/Sao_Paulo'),
            ['Aguardando execução SISREG'],
            []
        ),
        if(
            ue.ultima_fonte = 'SISREG'
            and ue.ultimo_evento_status like '%DEVOLVID%',
            ['Devolvido SISREG'],
            []
        ),
        if(
            ue.ultima_fonte = 'SISREG'
            and ue.ultimo_evento_status like '%FALT%',
            ['Falta SISREG'],
            []
        ),

        -- ── SER (último evento é SER) ─────────────────────────────────────
        if(
            ue.ultima_fonte = 'SER'
            and ue.ultima_data_solicitacao is not null
            and ue.ultima_data_autorizacao is null,
            ['Pendente de autorização SER'],
            []
        ),
        if(
            ue.ultima_fonte = 'SER'
            and ue.ultima_data_autorizacao is not null
            and ue.ultima_data_execucao is null,
            ['Pendente de realização SER'],
            []
        ),
        if(
            ue.ultima_fonte = 'SER'
            and ue.ultima_data_execucao is not null
            and ue.ultima_data_execucao > current_date('America/Sao_Paulo'),
            ['Aguardando execução SER'],
            []
        ),
        if(
            ue.ultima_fonte = 'SER'
            and ue.ultimo_evento_status = 'CANCELADA',
            ['Procedimento cancelado SER'],
            []
        ),
        if(
            ue.ultima_fonte = 'SER'
            and ue.ultimo_evento_status = 'CHEGADA_NAO_CONFIRMADA',
            ['Falta SER'],
            []
        ),

        -- ── Outros (status da paciente + tempo_total) ────────────────────
        -- Apenas o nível mais grave entre as 3 pendências de UNACON é
        -- emitido: o CASE garante exclusividade entre as faixas de
        -- tempo_total.
        case
            when ue.status = 'DIAGNOSTICO' and ue.tempo_total >= 60
                then ['Prazo legal para início de tratamento ultrapassado']
            when ue.status = 'DIAGNOSTICO' and ue.tempo_total >= 45
                then ['Prazo para início de tratamento próximo do limite legal']
            when ue.status = 'DIAGNOSTICO'
                then ['Pendente de solicitação para UNACON']
            else []
        end
    ) as pendencia_atual
from ultimo_evento as ue
