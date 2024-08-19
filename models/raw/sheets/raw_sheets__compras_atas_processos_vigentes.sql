{{
    config(
        schema="brutos_sheets",
        alias="compras_atas_processos_vigentes",
    )
}}


with
    source as (
        select *
        from {{ source("brutos_sheets_staging", "compras_atas_processos_vigentes") }}
    ),
    transformed as (
        select
            {{ clean_numeric_string("codigo_sma") }} as id_material,
            codigo_br as id_br,
            nome_padronizado,
            licitacao_realizada__para as licitacao_realizada_para,
            u___c as apresentacao,
            processo_licitatorio as id_processo_licitatorio,
            if(
                regexp_contains(pregao___rp, r'^\d{3,4}/\d{2,4}$'), pregao___rp, null
            ) as id_registro_preco,
            if(regexp_contains(ata, r'^\d{3}/\d{4}$'), ata, null) as id_ata,
            if(
                regexp_contains(vencimento__da_ata, r'^\d{2}/\d{2}/\d{4}$'),
                parse_date('%d/%m/%Y', vencimento__da_ata),
                null
            ) as vencimento_data,
            empresa_vencedora,
            fabricante,
            pregao___rp,
            ata,
            status
        from source
    ),

    final as (
        select
            * except (status),
            if(id_ata is not null, 'sim', 'nao') as rp_vigente_indicador,
            if(
                id_ata is not null,
                '',
                {{ clean_name_string("status") }}
            ) as status,
        from transformed
    )

select * from final