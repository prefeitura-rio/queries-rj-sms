{{
  config(
    enabled=true,
    schema="saude_sisreg",
    alias="procedimentos",
    unique_key='id_procedimento_sisreg',
    cluster_by=['id_procedimento_sisreg'],
    on_schema_change='sync_all_columns'
  )
}}

with 
procedimentos_sisreg as (
    select
        id_procedimento_sisreg,
        array_agg(distinct id_procedimento_sigtap ignore nulls) as id_procedimento_sigtap,
        array_agg(distinct procedimento_grupo ignore nulls) as procedimento_grupo,
        array_agg(distinct procedimento ignore nulls) as procedimento
    from {{ref("mart_sisreg__solicitacoes")}}
    where id_procedimento_sisreg is not null
    group by 1
),

procedimentos_parametrizados as (
    select
        id_procedimento,
        especialidade,
        tipo_procedimento,
        parametro_consultas_por_hora,
        parametro_reservas,
        parametro_retornos
    from {{ref("raw_sheets__assistencial_procedimento")}}
),
final as (
    select
        sisreg.id_procedimento_sisreg,
        sisreg.id_procedimento_sigtap,
        sisreg.procedimento_grupo,
        sheets.especialidade,
        sheets.tipo_procedimento,
        sisreg.procedimento,
        sheets.parametro_consultas_por_hora,
        sheets.parametro_reservas,
        sheets.parametro_retornos
    from procedimentos_sisreg as sisreg 
    left join procedimentos_parametrizados as sheets
    on sisreg.id_procedimento_sisreg = sheets.id_procedimento
)

select *
from final
