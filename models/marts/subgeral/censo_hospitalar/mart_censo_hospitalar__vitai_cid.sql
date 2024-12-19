{{
    config(
        alias="vitai_cid",
        schema="app_plataforma_smsrio",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
        tags=['daily']
    )
}}
with vitai as (
    select prontuario.id_atendimento, cpf, entrada_datahora, cid.id, cid.descricao,
    from {{ ref("int_historico_clinico__episodio__vitai") }}, unnest(condicoes) as cid
    where saida_datahora is null
    qualify row_number() over (partition by cpf order by entrada_datahora desc) = 1
),
formatting as (
    select 
        id_atendimento, 
        cpf, 
        entrada_datahora, 
        array_agg(struct(id, descricao)) as condicoes
    from vitai
    group by 1,2,3
)
select
    current_datetime() as data_referencia,
    cpf, 
    entrada_datahora, 
    condicoes,
    safe_cast(cpf as int64) as cpf_particao
from formatting