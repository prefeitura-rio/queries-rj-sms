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
    cpf_particao,
    cpf,

    -- id paciente
    nome,
    raca_cor,
    idade,
    ap,
    cf,
    equipe_sf,

    -- qualificadores gerais
    status,
    any_value (gravidade_score) as gravidade_score,

    -- contato paciente
    telefone,
    telefone_cf,
    telefone_esf,

    -- sistemas com eventos do paciente
    struct(
        logical_or(fonte = 'SISCAN') as siscan,
        logical_or(fonte = 'SER') as ser,
        logical_or(fonte = 'SISREG') as sisreg
    ) as fontes,

    -- eventos
    array_agg (
        struct(
            fonte,
            tipo,
            evento_status,
            procedimento,
            cid,

            unidade_solicitante,
            unidade_executante,

            data_solicitacao,
            data_autorizacao,
            data_execucao,
            data_resultado,

            safe_cast(data_solicitacao as string) as data_solicitacao_str,
            safe_cast(data_autorizacao as string) as data_autorizacao_str,
            safe_cast(data_execucao as string) as data_execucao_str,
            safe_cast(data_resultado as string) as data_resultado_str,

            array_concat(
                if(
                    mama_esquerda_resultado is null,
                    [],
                    [concat("Mama Esquerda ", mama_esquerda_resultado)]
                ),
                if(
                    mama_direita_resultado is null,
                    [],
                    [concat("Mama Direita ", mama_direita_resultado)]
                )
            ) as resultados,

            dias_proximo_evento
        )

        order by
            data_solicitacao,
            data_autorizacao,
            data_execucao,
            data_resultado
    ) as eventos,

    any_value (tempo_total) as tempo_total

from {{ ref("int_monitora_cancer__eventos_episodios") }}
group by
    cpf_particao,
    cpf,
    nome,
    raca_cor,
    idade,
    ap,
    cf,
    equipe_sf,
    status,
    telefone,
    telefone_cf,
    telefone_esf
