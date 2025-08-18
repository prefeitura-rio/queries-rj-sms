{{
    config(
        schema="intermediario_historico_clinico",
        alias="vacinacao_vitacare",
        materialized="table",
    )
}}

with
    vacinas_bruto as (
        select * except(updated_at_rank)
        from {{ref('raw_prontuario_vitacare__vacina')}}
    ),

    episodio as (
        select 
            id_prontuario_global as id,
            cpf
        from {{ref('raw_prontuario_vitacare__atendimento')}}
    ),
    agregado as (
        select 
            v.*,
            e.cpf 
        from vacinas_bruto v
        left join episodio e on rtrim(v.id, concat('.', v.nome_vacina)) = e.id

    ),
    vacinacoes as (
        select
            id, 
            id_cnes,
            cpf,
            nome_vacina,
            dose,
            aplicacao_data,
            registro_data,
            diff,
            lote,
            registro_tipo,
            estrategia_imunizacao,
            loaded_at
        from agregado
    )
select 
    *
from vacinacoes

