{{
    config(
        enabled=true,
        materialized="table",
        schema="saude_sisreg",
        alias="unidades"
    )
}}

with 
unidades as (
    select 
        'SOLICITANTE' as atividade,
        data_referencia,
        unidade_ativa_ultimos_3m,
        id_cnes,
        procedimentos
    from {{ref("int_sisreg__unidades_solicitantes")}}
    where id_cnes is not null

UNION ALL

    select
        'EXECUTANTE' as atividade,
        data_referencia,
        unidade_ativa_ultimos_3m,
        id_cnes,
        procedimentos 
    from {{ref("int_sisreg__unidades_executantes")}}
    where id_cnes is not null
)

select
        unidades.data_referencia,
        unidades.atividade as unidade_atividade,
        unidades.id_cnes as unidade_id_cnes,
        estabs.nome_fantasia as unidade_nome,
        estabs.tipo_unidade_agrupado_subgeral as unidade_tipo,
        estabs.esfera_subgeral as unidade_esfera,
        unidades.unidade_ativa_ultimos_3m,
        unidades.procedimentos

from unidades
left join {{ref("raw_sheets__estabelecimento_auxiliar")}} as estabs
using (id_cnes)
