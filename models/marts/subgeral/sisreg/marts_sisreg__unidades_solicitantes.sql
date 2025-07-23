{{
    config(
        enabled=true,
        materialized="table",
        schema="saude_sisreg",
        alias="unidades_solicitantes"
    )
}}

with
    sisreg_unidades_solicitantes as (
        select
            current_date() as data_referencia,
            unidade_solicitante_id as id_cnes,
            date(data_solicitacao) as data_solicitacao,
            procedimento_id as procedimento_interno_id
        from {{ref("raw_sisreg_api__solicitacoes")}}
    ),

    sisreg_unidades_solicitantes_ativas as (
        select distinct
            data_referencia,
            id_cnes,
            procedimento_interno_id
        from sisreg_unidades_solicitantes 
        where  
            data_solicitacao between
                date_sub(data_referencia, interval 90 day)
                and data_referencia        
    ),

    consolidando as (
        select
            todas.data_referencia,
            todas.id_cnes,
            case 
                when ativas.id_cnes is not null then 1
                else 0
            end as unidade_ativa_ultimos_3m,
            ativas.procedimento_interno_id 

        from sisreg_unidades_solicitantes as todas
        left join sisreg_unidades_solicitantes_ativas as ativas
        using (id_cnes)
    ),

    final as (
        select 
            data_referencia,
            unidade_ativa_ultimos_3m,    
            id_cnes,
            array_agg(distinct procedimento_interno_id ignore nulls) as procedimentos

        from consolidando
        group by 1, 2, 3
        order by 
            unidade_ativa_ultimos_3m desc,
            id_cnes asc
    )

select * from final
