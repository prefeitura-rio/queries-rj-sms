{{
    config(
        materialized='table',
        alias = "sintomaticos_respiratorios_dia",
    )
}}

{% set ontem = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=1)
).isoformat() %}

with

-- Diagnosticados ontem (condicoes.data_diagnostico)
atendimentos_chegados_ontem as (
    SELECT DISTINCT
        paciente_cpf AS cpf,
        prontuario.fornecedor AS prontuario_fornecedor
    FROM
        {{ ref('mart_historico_clinico__episodio') }},
        UNNEST(condicoes) c
    WHERE
        c.id IN UNNEST({{ sinanrio_lista_cids_sintomaticos() }})
        AND paciente_cpf IS NOT NULL
        AND data_particao = '{{ontem}}'
),

cadastros_de_paciente as (
    select
        p.cpf,
        p.cns[OFFSET(0)] AS cns,
        p.dados.nome AS nome,
        p.dados.data_nascimento AS dt_nascimento,
        p.dados.genero AS sexo,
        {{ sinanrio_padronize_sexo('p.dados.genero') }} AS id_sexo,
        p.dados.raca AS raca,
        {{ sinanrio_padronize_raca_cor('p.dados.raca') }} AS id_raca_cor,
        p.dados.mae_nome AS nome_mae,
        p.dados.pai_nome AS nome_pai,
        p.dados.contato.telefone[OFFSET(0)].valor AS telefone,
        p.dados.endereco[OFFSET(0)].cep AS endereco_cep,
        p.dados.endereco[OFFSET(0)].tipo_logradouro AS endereco_tipo_logradouro,
        p.dados.endereco[OFFSET(0)].logradouro AS endereco_logradouro,
        CONCAT(
            IFNULL(p.dados.endereco[OFFSET(0)].tipo_logradouro, ''), 
            ' ', 
            IFNULL(p.dados.endereco[OFFSET(0)].logradouro, '')
        ) AS logradouro,
        p.dados.endereco[OFFSET(0)].numero AS endereco_numero,
        p.dados.endereco[OFFSET(0)].complemento AS endereco_complemento,
        p.dados.endereco[OFFSET(0)].bairro AS endereco_bairro,
        bairros.id AS id_bairro,
        p.dados.prontuario[OFFSET(0)].id_cnes AS cnes,
        p.dados.prontuario[OFFSET(0)].id_ine AS ine,
        p.dados.prontuario[OFFSET(0)].id_paciente AS n_prontuario
    from {{ ref('mart_historico_clinico__paciente') }} p
    left join {{ref('raw_plataforma_subpav_principal__bairros')}} as bairros on {{ clean_name_string("p.endereco[OFFSET(0)].bairro") }} = {{ clean_name_string("bairros.descricao") }}
),

atendimentos_com_cids as (
    select
        atendimentos_chegados_ontem.*,
        cadastros_de_paciente.* except(cpf),
        array_concat(
            ARRAY(
                SELECT cod
                FROM UNNEST(JSON_EXTRACT_ARRAY(atendimentos_chegados_ontem.condicoes)) AS item,
                    UNNEST([
                        JSON_VALUE(item, '$.cod_cid10'),
                        JSON_VALUE(item, '$.cod_ciap2')
                    ]) AS cod
                WHERE cod IS NOT NULL AND cod != ''
                ),
            regexp_extract_all(upper(atendimentos_chegados_ontem.soap_subjetivo_motivo), r'\b[A-Z][0-9]{3}\b'),
            regexp_extract_all(upper(atendimentos_chegados_ontem.soap_objetivo_descricao), r'\b[A-Z][0-9]{3}\b'),
            regexp_extract_all(upper(atendimentos_chegados_ontem.soap_avaliacao_observacoes), r'\b[A-Z][0-9]{3}\b')
        ) as cids_extraidos
    from atendimentos_chegados_ontem
        left join cadastros_de_paciente using (cpf)
    where cast(datahora_inicio as date) >= '{{ ontem }}'
),

-- Filtra atendimentos que contêm ao menos 1 CID da lista macro
atendimentos_filtrados as (
    select *
    from atendimentos_com_cids
    where exists (
        select cid
        from unnest(cids_extraidos) as cid
        where cid in UNNEST({{ sinanrio_lista_cids_sintomaticos() }})
    )
),

-- Retorna apenas o atendimento mais recente por CPF
atendimento_unico as (
    select *,
        row_number() over (partition by cpf order by datahora_fim desc) as rn
    from atendimentos_filtrados
)

select
    *
from atendimento_unico
where rn = 1
