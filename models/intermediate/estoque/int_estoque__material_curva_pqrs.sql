with
    estabelecimento as (
        select id_cnes, tipo_sms_agrupado from {{ ref("dim_estabelecimento") }}
    ),

    material as (select * from {{ ref("dim_material") }}),

    movimento as (
        select *
        from {{ ref("fct_estoque_movimento") }}
        where movimento_tipo_grupo = 'CONSUMO'
    ),

    demanda as (
        select
            e.tipo_sms_agrupado,
            m.id_material,
            count(distinct m.consumo_paciente_cpf) as contagem_cpf_distintos,
        from movimento as m
        left join estabelecimento as e using (id_cnes)
        group by tipo_sms_agrupado, id_material
    ),

    demanda_acumulada as (
        select
            tipo_sms_agrupado,
            id_material,
            contagem_cpf_distintos as contagem_cpf,
            sum(contagem_cpf_distintos) over (
                partition by tipo_sms_agrupado order by contagem_cpf_distintos desc
            ) as contagem_cpf_acumulada
        from demanda
    ),

    demanda_total as (
        select tipo_sms_agrupado, sum(contagem_cpf_distintos) as total_cpf
        from demanda
        group by tipo_sms_agrupado
    ),

    final as (
        select
            da.tipo_sms_agrupado,
            da.id_material,
            da.contagem_cpf,
            da.contagem_cpf_acumulada,
            (da.contagem_cpf_acumulada / dt.total_cpf)
            * 100 as contagem_cpf_acumulada_percentual
        from demanda_acumulada as da
        inner join demanda_total as dt on da.tipo_sms_agrupado = dt.tipo_sms_agrupado
    ),

    pqrs_analysis as (
        select
            *,
            case
                when contagem_cpf = 0
                then 'S'
                when contagem_cpf_acumulada_percentual <= 80
                then 'P'
                when
                    contagem_cpf_acumulada_percentual > 80
                    and contagem_cpf_acumulada_percentual <= 95
                then 'Q'
                else 'R'
            end as pqrs_categoria
        from final
        order by tipo_sms_agrupado, contagem_cpf_acumulada_percentual asc
    )

select
    a.tipo_sms_agrupado,
    a.id_material,
    m.nome,
    a.contagem_cpf,
    a.contagem_cpf_acumulada,
    a.contagem_cpf_acumulada_percentual,
    a.pqrs_categoria
from pqrs_analysis as a
left join material as m using (id_material)
