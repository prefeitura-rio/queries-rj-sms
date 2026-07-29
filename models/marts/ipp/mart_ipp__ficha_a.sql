{{
    config(
        alias="ficha_a",
        schema="projeto_ipp",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}


with 
-- As colunas comentadas foram solicitadas pelo IPP mas estão presentes apenas no ficha_a_v2 que foi descontinuado.

    ficha_a as (
        select
            cpf,
            cns,
            nome,
            nome_social,
            nome_mae,
            data_cadastro,
            data_atualizacao_cadastro,
            obito,
            possui_filtro_agua,
            vulnerabilidade_social,
            territorio_social,
            familia_beneficiaria_cfc,
            familia_beneficiaria_auxilio_brasil,
            -- hanseniase,
            -- tuberculose
        from {{ ref('raw_prontuario_vitacare__ficha_a') }}
    )

select
    cpf,
    {{validate_cpf('cpf')}} as cpf_valido,
    cns,
    nome,
    nome_social,
    nome_mae as mae_nome,
    data_cadastro,
    data_atualizacao_cadastro as cadastro_atualizacao,
    obito,
    possui_filtro_agua,
    vulnerabilidade_social,
    territorio_social,
    if(
        familia_beneficiaria_cfc is true or familia_beneficiaria_auxilio_brasil is true, 
        true, 
        false
    ) as recebe_algum_beneficio,
    safe_cast(cpf as int64) as cpf_particao
from ficha_a