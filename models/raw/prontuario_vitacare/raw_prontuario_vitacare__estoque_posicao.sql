{{
    config(
        alias="estoque_posicao",
        labels={
            "dominio": "estoque",
            "dado_publico": "nao",
            "dado_pessoal": "nao",
            "dado_anonimizado": "nao",
            "dado_sensivel_saude": "nao",
        },
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=['daily','vitacare_estoque']
    )
}}

with
    source as (
        select *
        from {{ source("brutos_prontuario_vitacare_staging", "estoque_posicao") }}
    ),

    renamed as (
        select
            ap as area_programatica,
            cnesunidade as id_cnes,
            nomeunidade as estabelecimento_nome,
            desigmedicamento as material_descricao,
            atc as id_atc,
            code as id_material,
            lote as id_lote,
            status as lote_status,
            warehouse as armazem,
            dtacrilote as lote_data_cadastro,
            dtavalidadelote as lote_data_vencimento,
            estoquelote as material_quantidade,
            id,
            dtareplicacao as data_replicacao,
            _data_carga as data_carga,
            ano_particao as ano_particao,
            mes_particao as mes_particao,
            dtacrilote as data_particao
        from source
    ),

    final as (

        select
            -- Primary Key
            concat(id_cnes, '.', id, '.', data_particao) as id,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id_cnes",
                        "id_material",
                        "id_lote",
                        "armazem",
                        "material_quantidade",
                        "data_particao",
                    ]
                )
            }} as id_surrogate,
            
            -- Foreign Keys
            safe_cast(area_programatica as string) as area_programatica,
            safe_cast(id_cnes as string) as id_cnes,
            safe_cast(id_lote as string) as id_lote,
            regexp_replace(
                safe_cast(id_material as string), r'[^a-zA-Z0-9]', ''
            ) as id_material,
            safe_cast(id_atc as string) as id_atc,

            -- Common Fields
            estabelecimento_nome,
            if(lote_status = "", null, lower(lote_status)) as lote_status,
            safe_cast(lote_data_cadastro as date) as lote_data_cadastro,
            safe_cast(lote_data_vencimento as date) as lote_data_vencimento,
            material_descricao,
            safe_cast(material_quantidade as float64) as material_quantidade,
            lower({{ clean_name_string("armazem") }}) as armazem,

            -- Metadata
            safe_cast(data_particao as date) as data_particao,
            safe_cast(data_replicacao as datetime) as data_replicacao,
            safe_cast(data_carga as datetime) as data_carga

        from renamed
        where safe_cast(material_quantidade as float64) > 0
    )

select *
from final
