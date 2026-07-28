{{
    config(
        alias="ficha_a",
        schema="projeto_ipp",
        materialized="incremental"
    )
}}


-- TODO: Adiciona particionamento e desenvolver lógica incremental 
with 
    ficha_a as (
        select
            cpf,
            --cpf_valido,
            nis,
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
    nis,
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
from ficha_a