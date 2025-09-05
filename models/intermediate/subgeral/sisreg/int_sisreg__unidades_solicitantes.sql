with
    sisreg_unidades_solicitantes as (
        select
            current_date() as data_referencia,
            id_cnes_unidade_solicitante as id_cnes,
            date(data_solicitacao) as data_solicitacao,
            id_procedimento_sisreg
        from {{ref("mart_sisreg__solicitacoes")}}
    ),

    sisreg_unidades_solicitantes_ativas as (
        select distinct
            data_referencia,
            id_cnes,
            id_procedimento_sisreg
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
            ativas.id_procedimento_sisreg 

        from sisreg_unidades_solicitantes as todas
        left join sisreg_unidades_solicitantes_ativas as ativas
        using (id_cnes)
    ),

    final as (
        select 
            data_referencia,
            unidade_ativa_ultimos_3m,    
            id_cnes,
            array_agg(distinct id_procedimento_sisreg ignore nulls) as procedimentos

        from consolidando
        group by 1, 2, 3
        order by 
            unidade_ativa_ultimos_3m desc,
            id_cnes asc
    )

select * from final
