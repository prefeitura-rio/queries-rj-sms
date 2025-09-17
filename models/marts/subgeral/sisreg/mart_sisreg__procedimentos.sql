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
    id_procedimento_sigtap,
    procedimento_grupo,
    procedimento
  from {{ ref("mart_sisreg__solicitacoes") }}
  where id_procedimento_sisreg is not null
  qualify row_number() over (
            partition by id_procedimento_sisreg
            order by data_atualizacao_registro desc
          ) = 1
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
        case 
            when sisreg.procedimento_grupo is not null then concat(sisreg.procedimento_grupo, ' - ', sisreg.procedimento)
            else sisreg.procedimento
        end as procedimento,
        sheets.parametro_consultas_por_hora,
        sheets.parametro_reservas,
        sheets.parametro_retornos
    from procedimentos_sisreg as sisreg 
    left join procedimentos_parametrizados as sheets
    on sisreg.id_procedimento_sisreg = sheets.id_procedimento
)

select *
from final
