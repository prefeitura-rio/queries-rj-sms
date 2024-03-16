with
    -- Sources 
    remume as (select * from {{ ref("raw_sheets__material_remume") }}),

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
            remume.estabelecimento_disponibilidade,
            remume.id_material,
            remume.remume_grupos,
            remume.remume_basico,
            remume.remume_hospitalar,
            remume.remume_uso_interno,
            remume.remume_antiseptico,
            remume.remume_estrategico
        from estabelecimento as est
        cross join remume
    ),

    relacao_remume_unidades as (
        select *
        from combinacao_estabelecimento_remume
        where
            tipo_sms_simplificado in unnest(estabelecimento_disponibilidade)
            or id_cnes in unnest(estabelecimento_disponibilidade)
        order by id_cnes
    ),

    relacao_remume_ajutasdo_unidades as (
        select
            id_cnes,
            tipo_sms_simplificado,
            estabelecimento_disponibilidade,
            id_material,
            remume_grupos,
            if(
                tipo_sms_simplificado in ("HOSPITAL", "MATERNIDADE"),
                null,
                remume_basico
            ) as remume_basico,
            if(
                tipo_sms_simplificado in ("HOSPITAL", "MATERNIDADE"),
                null,
                remume_uso_interno
            ) as remume_uso_interno,
            if(
                tipo_sms_simplificado not in ("HOSPITAL", "MATERNIDADE"),
                null,
                remume_hospitalar
            ) as remume_hospitalar,
            remume_antiseptico,
            remume_estrategico
        from relacao_remume_unidades
    ),

    -- TPC
    remume_distintos as (
        select
            id_material,
            remume_grupos,
            remume_basico,
            remume_hospitalar,
            remume_uso_interno,
            remume_antiseptico,
            remume_estrategico
        from remume
        where remume_basico = "sim" or remume_uso_interno = "sim"  -- TPC só abastece APS
    ),

    relacao_remume_tpc as (
        select
            "tpc" as id_cnes,
            "TPC" as tipo_sms_simplificado,
            array["TPC"] as estabelecimento_disponibilidade,
            remume.id_material,
            remume.remume_grupos,
            remume.remume_basico,
            "" as remume_hospitalar,  -- TPC só abastece APS
            remume.remume_uso_interno,
            remume.remume_antiseptico,
            remume_estrategico,
        from remume_distintos as remume
    )

-- Result
select *
from relacao_remume_unidades
union all
select *
from relacao_remume_tpc
