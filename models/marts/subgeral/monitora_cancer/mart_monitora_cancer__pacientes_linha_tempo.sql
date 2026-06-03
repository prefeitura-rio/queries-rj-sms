-- noqa: disable=LT08

{{
  config(
    schema="projeto_monitora_cancer",
    alias="pacientes_linha_tempo",
    partition_by={
        "field": "cpf_particao",
        "data_type": "int64",
        "range": {"start": 0, "end": 100000000000, "interval": 34722222},
},
cluster_by = ['status', 'cf', 'equipe_sf', 'cpf_particao'],
on_schema_change = 'sync_all_columns'
)
}}

select
    -- pk
    ev.cpf_particao,
    ev.cpf,

    -- id paciente
    ev.nome,
    ev.raca_cor,
    ev.idade,
    ev.ap,
    ev.cf,
    ev.equipe_sf,

    -- qualificadores gerais
    ev.status,
    safe_cast(coalesce(any_value (grv.gravidade_total_0_100), 0) as int) as gravidade_score,
    any_value (ev.gestante) as gestante,

    -- contato paciente
    ev.telefone,
    ev.telefone_cf,
    ev.telefone_esf,

    -- sistemas com eventos do paciente
    struct(
        logical_or(ev.fonte = 'SISCAN') as siscan,
        logical_or(ev.fonte = 'SER') as ser,
        logical_or(ev.fonte = 'SISREG') as sisreg
    ) as fontes,

    -- eventos
    array_agg (
        struct(
            ev.fonte,
            ev.tipo,
            replace(ev.evento_status, '_', ' ') as evento_status,
            ev.procedimento,
            ev.cid,

            ev.unidade_solicitante,
            ev.unidade_executante,

            ev.data_solicitacao,
            ev.data_autorizacao,
            ev.data_execucao,
            ev.data_resultado,

            safe_cast(ev.data_solicitacao as string) as data_solicitacao_str,
            safe_cast(ev.data_autorizacao as string) as data_autorizacao_str,
            safe_cast(ev.data_execucao as string) as data_execucao_str,
            safe_cast(ev.data_resultado as string) as data_resultado_str,

            array_concat(
                if(
                    ev.mama_esquerda_resultado is null,
                    [],
                    [concat("Mama Esquerda ", ev.mama_esquerda_resultado)]
                ),
                if(
                    ev.mama_direita_resultado is null,
                    [],
                    [concat("Mama Direita ", ev.mama_direita_resultado)]
                )
            ) as resultados,

            ev.atraso_solicitacao_autorizacao,
            ev.atraso_autorizacao_execucao,
            ev.atraso_regulacao,

            ev.risco,

            ev.dias_proximo_evento,
            ev.run_id as jornada_id
        )

        order by
            ev.data_referencia_evento,
            ev.data_solicitacao,
            ev.data_autorizacao,
            ev.data_execucao,
            ev.data_resultado
    ) as eventos,

    any_value (ev.tempo_total) as tempo_total,

    -- pendências atuais (1 array por paciente, calculado em int_monitora_cancer__pendencias)
    any_value (pend.pendencia_atual) as pendencia_atual

from {{ ref("int_monitora_cancer__eventos_episodios") }} as ev
    left join {{ ref("mart_monitora_cancer__gravidade") }} as grv
    on ev.cpf_particao = grv.cpf_particao
    left join {{ ref("int_monitora_cancer__pendencias") }} as pend
    on ev.cpf_particao = pend.cpf_particao
group by
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
    ev.telefone_esf
