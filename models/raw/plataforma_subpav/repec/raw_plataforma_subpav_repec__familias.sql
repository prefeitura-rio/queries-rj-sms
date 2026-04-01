{{
    config(
        alias="repec__familias",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__familias") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_familia') }} as id_familia,
            {{ process_null('num_familia') }} as num_familia,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('cep_endereco') ~ " as string)") }}) as cep_endereco,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('tipo_logradouro_endereco') ~ " as string)") }}) as tipo_logradouro_endereco,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('endereco') ~ " as string)") }}) as endereco,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('num_endereco') ~ " as string)") }}) as num_endereco,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('bairro_endereco') ~ " as string)") }}) as bairro_endereco,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('municipio_endereco') ~ " as string)") }}) as municipio_endereco,
            {{ process_null('situacao_moradia') }} as situacao_moradia,
            {{ process_null('localizacao') }} as localizacao,
            {{ process_null('material_predominante') }} as material_predominante,
            {{ process_null('energia_eletrica') }} as energia_eletrica,
            {{ process_null('destino_lixo') }} as destino_lixo,
            {{ process_null('tratamento_agua') }} as tratamento_agua,
            {{ process_null('abastecimento_agua') }} as abastecimento_agua,
            {{ process_null('destino_fezes') }} as destino_fezes,
            {{ process_null('benef_bolsa_fam') }} as benef_bolsa_fam,
            {{ process_null('id_responsavel_bf') }} as id_responsavel_bf,
            {{ process_null('benef_cfc') }} as benef_cfc,
            {{ process_null('id_responsavel_fam') }} as id_responsavel_fam,
            {{ process_null('observacoes_familia') }} as observacoes_familia,
            {{ process_null('data_registro_familia') }} as data_registro_familia,
            {{ process_null('origem_arquivo') }} as origem_arquivo,
            {{ process_null('origem_banco') }} as origem_banco,
            {{ repec_origem_unidade_para_cnes("origem_unidade") }} as cnes_origem,
            safe_cast({{ process_null('datalake_loaded_at') }} as timestamp) as datalake_loaded_at
        from source
    ),

    deduplicar as (
        select *
        from tratar_campos
        qualify row_number() over (
            partition by
                    id_familia,
                    num_familia,
                    cep_endereco,
                    tipo_logradouro_endereco,
                    endereco,
                    num_endereco,
                    bairro_endereco,
                    municipio_endereco,
                    situacao_moradia,
                    localizacao,
                    material_predominante,
                    energia_eletrica,
                    destino_lixo,
                    tratamento_agua,
                    abastecimento_agua,
                    destino_fezes,
                    benef_bolsa_fam,
                    id_responsavel_bf,
                    benef_cfc,
                    id_responsavel_fam,
                    observacoes_familia,
                    data_registro_familia
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
