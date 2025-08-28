{{
    config(
        enabled=true,
        materialized="table",
        schema="saude_sisreg",
        alias="procedimentos",
        cluster_by=['procedimento_sisreg_id']

    )
}}

with
procedimentos_sisreg as (
    select distinct
        procedimento_id as procedimento_sisreg_id,
        procedimento_grupo as procedimento_sisreg_grupo,
        procedimento as procedimento_sisreg_nome,
        procedimento_sigtap_id
    from {{ref("raw_sisreg_api__solicitacoes")}}
    where date(data_solicitacao) >= date_sub(current_date('America/Sao_Paulo'), interval 3 month)
    qualify row_number() over (
        partition by procedimento_id
        order by data_atualizacao desc nulls last
    ) = 1
),

procedimentos_atributos_sheetss as (
    select
        id_procedimento as procedimento_sisreg_id,
        descricao as sheets_procedimento_sisreg_nome,
        parametro_consultas_por_hora as sheets_parametro_consultas_por_hora,
        parametro_reservas as sheets_parametro_reservas,
        parametro_retornos as sheets_parametro_retornos,
        especialidade as sheets_especialidade,
        tipo_procedimento as sheets_tipo_procedimento
    from {{ref("raw_sheets__assistencial_procedimento")}}
)

select
    *,
    case
        when sheets_procedimento_sisreg_nome is null then false 
        else true
    end as parametrizado_indicador,

    case 
        when procedimento_sisreg_nome is null then false
        else true
    end as presente_sisreg_indicador

from procedimentos_atributos_sheetss
full outer join procedimentos_sisreg
using (procedimento_sisreg_id)