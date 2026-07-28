{{
    config(
        schema = "projeto_ipp",
        alias="estabelecimento",
        materialized="table",
        cluster_by='id_cnes'
    )
}}


with 
    estabelecimento as (
        select
            id_cnes,
            id_unidade,
            nome_razao_social,
            nome_fantasia,
            data_atualizao_registro,
        from {{ ref('dim_estabelecimento_sus_rio_historico') }}
        where 
            id_cnes is not null
            and id_unidade is not null
        qualify row_number() over(partition by id_cnes, id_unidade order by data_atualizao_registro desc) = 1
    )

select 
    id_cnes,
    id_unidade,
    {{ proper_estabelecimento('nome_razao_social') }} as razao_social,
    {{ add_accents_estabelecimento('nome_fantasia') }} as nome_fantasia,
    data_atualizao_registro as atualizado_em,
    current_date('America/Sao_Paulo') processado_em
from estabelecimento



