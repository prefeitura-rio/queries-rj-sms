{{
    config(
        alias="estoque_posicao",
        schema="brutos_prontuario_vitacare_api_centralizadora",
        tags="vitacare_estoque",
        labels={
            "dominio": "estoque",
            "dado_publico": "nao",
            "dado_pessoal": "nao",
            "dado_anonimizado": "nao",
            "dado_sensivel_saude": "nao",
        },
        partition_by={
            "field": "particao_data_posicao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    source as (
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_posicao_ap10") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_posicao_ap21") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_posicao_ap22") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_posicao_ap31") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_posicao_ap32") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_posicao_ap33") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_posicao_ap40") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_posicao_ap51") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_posicao_ap52") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_posicao_ap53") }}
    ),

    renamed as (
        select 
            json_extract_scalar(data, '$.id') as id,
            json_extract_scalar(data, '$.ap') as area_programatica,
            json_extract_scalar(data, '$.cnesUnidade') as id_cnes,
            json_extract_scalar(data, '$.nomeUnidade') as estabelecimento_nome,
            json_extract_scalar(data, '$.desigMedicamento') as material_descricao,
            json_extract_scalar(data, '$.atc') as id_atc,
            json_extract_scalar(data, '$.code') as id_material,
            json_extract_scalar(data, '$.lote') as id_lote,
            json_extract_scalar(data, '$.status') as lote_status,
            json_extract_scalar(data, '$.warehouse') as armazem,
            json_extract_scalar(data, '$.dtaCriLote') as lote_data_cadastro,
            json_extract_scalar(data, '$.dtaValidadeLote') as lote_data_vencimento,
            json_extract_scalar(data, '$.estoqueLote') as material_quantidade,
            json_extract_scalar(data, '$.dtaReplicacao') as data_replicacao,

            _source_cnes as requisicao_id_cnes,
            _source_ap as requisicao_area_programatica,
            _endpoint as requisicao_endpoint,

            _loaded_at as loaded_at
        from source
    ),

    final as (

        select
            -- Primary Key
            concat(id_cnes, '.', id, '.', safe_cast(safe_cast(data_replicacao as datetime) as date)) as id,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id_cnes",
                        "id_material",
                        "id_lote",
                        "armazem",
                        "material_quantidade",
                        "data_replicacao",
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
            safe_cast(material_quantidade as int64) as material_quantidade,
            lower({{ clean_name_string("armazem") }}) as armazem,
            
            struct(
                safe_cast(data_replicacao as datetime) as updated_at,
                safe_cast(loaded_at as timestamp) as loaded_at
            ) as metadados,

            cast(safe_cast(data_replicacao as datetime) as date) as particao_data_posicao

        from renamed
    )

select *
from final
qualify row_number() over(partition by id_surrogate order by metadados.updated_at desc) = 1
