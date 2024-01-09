with
    -- Sources 
    remume as (
        select *
        from {{ ref("raw_sheets__material_remume") }}
        where
            remume_grupo = "Atenção Básica - Medicamentos" or remume_grupo = "Hospitalar"  -- só mostrar medicamentos por enquanto
    ),  

    estabelecimento as (
        select *
        from {{ ref("dim_estabelecimento") }}
        where prontuario_estoque_tem_dado = "sim"  -- só mostrar unidade que usam o modulo de estoque do prontuario
    ),

    -- Unidades de Saúde
    combinacao_estabelecimento_remume as (
        select
            est.id_cnes,
            est.tipo_sms_simplificado,
            remume.id_material,
            remume.remume_grupo,
            remume.estabelecimento_disponibilidade
        from estabelecimento as est
        cross join remume
    ),

    relacao_remume_unidades as (
        select *
        from combinacao_estabelecimento_remume
        where
            tipo_sms_simplificado in unnest(estabelecimento_disponibilidade)
            or id_cnes in unnest(estabelecimento_disponibilidade)
    ),

    -- TPC
    remume_distintos as (select distinct id_material from remume),

    relacao_remume_tpc as (
        select
            "-" as id_cnes,
            "TPC" as tipo_sms_simplificado,
            remume.id_material,
            "-" as remume_grupo,
            array["TPC"] as estabelecimento_disponibilidade,
        from remume_distintos as remume
    )

--- Result
select *
from relacao_remume_unidades
union all
select *
from relacao_remume_tpc

