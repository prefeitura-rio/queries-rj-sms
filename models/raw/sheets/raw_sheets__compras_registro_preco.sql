{{
    config(
        schema="brutos_sheets",
        alias="compras_registro_preco",
        tag=["daily", "subgeral", "cnes_subgeral", "monitora_reg"],
    )
}}
-- TODO: conferir tags acima

with source as (
      select * from {{ source('brutos_sheets_staging', 'compras_registro_preco') }}
),
renamed as (
    select
        -- item,
        cast(cod_sigma as string) as id_material,
        item_nome as material_nome,
        item_unid as material_unidade,
        itens_bloqueados as material_blowueado,
        avaliacao_estoque_critico_em_dias,
        cmm_atual,
        -- _2024_08_13,
        status,
        rp_vigente,
        if(REGEXP_CONTAINS(rp_vigente, r'^\d{3}/\d{4}$'), 'sim', 'nao') AS rp_vigente_indicador,
        lower({{ clean_name_string('emergencial') }}) as emergencial,
        lower({{ clean_name_string('em_andamento_licitatorio') }}) as em_andamento_licitatorio,
        lower({{ clean_name_string('impedimento') }}) as impedimento,
        parse_date('%d/%m/%Y',if(vencimento_da_ata = "0", Null, vencimento_da_ata)) as vencimento_data,
        {{ clean_name_string('situacao') }} as situacao,
        {{ clean_name_string('atualizacao') }} as atualizacao,
        fornecedor_principal,
        fabricante_principal

    from source
    where cod_sigma is not null
)
select * from renamed
