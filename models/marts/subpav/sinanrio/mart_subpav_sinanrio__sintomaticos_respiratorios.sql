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

atendimentos_chegados_ontem as (
    -- Utiliza data_particao para filtrar apenas os atendimentos do dia de ontem e diminuir custos de processamento
    select 
        cpf,
        condicoes,
        soap_subjetivo_motivo,
        soap_objetivo_descricao,
        soap_avaliacao_observacoes,
        datahora_fim
    from {{ ref('raw_prontuario_vitacare__atendimento') }}
    where data_particao >= '{{ ontem }}'
),

cadastros_de_paciente as (
    select
        cpf,
        nome,
        data_nascimento
    from {{ ref('raw_prontuario_vitacare__paciente') }}
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
    where cast(datahora_fim as date) >= '{{ ontem }}'

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
