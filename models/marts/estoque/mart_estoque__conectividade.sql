{{
    config(
        alias="conectividade",
        schema="projeto_estoque",
        materialized="table",
    )
}}
with
    source as (select * from {{ ref("mart_estoque__posicao_atual") }}),

    grouped as (
        select
            estabelecimento_tipo_sms_agrupado,
            sistema_origem,
            estabelecimento_area_programatica,
            id_cnes,
            estabelecimento_nome_limpo,
            max(data_particao) as data_ultima_atualizacao
        from source
        group by 1, 2, 3, 4, 5
    ),

    freshness as (

        select
            *,
            if(
                data_ultima_atualizacao = current_date("America/Sao_Paulo"), true, false
            ) as dado_atualizado_hoje_indicador,
            date_diff(
                current_date("America/Sao_Paulo"), data_ultima_atualizacao, day
            ) as dias_desde_atualizacao

        from grouped
    ),

    final as (
        select
            *,
            case
                when dias_desde_atualizacao = 0
                then "Confiável"
                when dias_desde_atualizacao <= 2
                then "Atenção"
                else "Não confiável"
            end as confiabilidade
        from freshness
    )

select *
from final
