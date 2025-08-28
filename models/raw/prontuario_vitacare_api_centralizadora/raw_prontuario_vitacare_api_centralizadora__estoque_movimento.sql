{{
    config(
        alias="estoque_movimento",
        tags="vitacare_estoque",
        labels={
            "dominio": "estoque",
            "dado_publico": "nao",
            "dado_pessoal": "nao",
            "dado_anonimizado": "nao",
            "dado_sensivel_saude": "nao",
        },
        partition_by={
            "field": "particao_data_movimento",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with estoque_movimento_historico as (
    select * except(data_particao),
    safe_cast(safe_cast(safe_cast(_loaded_at as timestamp) as date) as string) as data_particao 
    from {{ source("brutos_prontuario_vitacare_api_centralizadora", "estoque_movimento_historico") }}
),

    source as (
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_movimento_ap10") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_movimento_ap21") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_movimento_ap22") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_movimento_ap31") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_movimento_ap32") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_movimento_ap33") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_movimento_ap40") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_movimento_ap51") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_movimento_ap52") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_centralizadora_staging", "estoque_movimento_ap53") }}
        union all
        select * from estoque_movimento_historico
    ),

    renamed as (
        select 
            json_extract_scalar(data, '$.id') as id_estoque_movimento_local,
            json_extract_scalar(data, '$.ap') as area_programatica,
            json_extract_scalar(data, '$.cnesUnidade') as id_cnes,
            json_extract_scalar(data, '$.nomeUnidade') as estabelecimento_nome,
            json_extract_scalar(data, '$.desigMedicamento') as material_descricao,
            json_extract_scalar(data, '$.atc') as id_atc,
            regexp_replace(
                json_extract_scalar(data, '$.code'), r'[^a-zA-Z0-9]', ''
            ) as id_material,
            json_extract_scalar(data, '$.lote') as id_lote,
            json_extract_scalar(data, '$.dtaMovimento') as estoque_movimento_data_hora,
            json_extract_scalar(data, '$.tipoMovimento') as estoque_movimento_tipo,
            json_extract_scalar(data, '$.motivoCorrecao') as estoque_movimento_correcao_tipo,
            json_extract_scalar(data, '$.justificativa') as estoque_movimento_justificativa,
            json_extract_scalar(data, '$.cpfProfPrescritor') as dispensacao_prescritor_cpf,
            json_extract_scalar(data, '$.cnsProfPrescritor') as dispensacao_prescritor_cns,
            json_extract_scalar(data, '$.cpfPatient') as dispensacao_paciente_cpf,
            json_extract_scalar(data, '$.cnsPatient') as dispensacao_paciente_cns,
            json_extract_scalar(data, '$.qtd') as material_quantidade,
            json_extract_scalar(data, '$.codWms') as id_pedido_wms,
            json_extract_scalar(data, '$.armazemOrigem') as estoque_armazem_origem,
            json_extract_scalar(data, '$.armazemDestino') as estoque_armazem_destino,
            json_extract_scalar(data, '$.dtaReplicacao') as data_replicacao,

            _source_cnes as requisicao_id_cnes,
            _source_ap as requisicao_area_programatica,
            _endpoint as requisicao_endpoint,

            _loaded_at as loaded_at
        from source
    ),

    casted as (
        select
            safe_cast({{ process_null('id_estoque_movimento_local') }} as string) as id_estoque_movimento_local,
            safe_cast({{ process_null('area_programatica') }} as string) as area_programatica,
            safe_cast({{ process_null('id_cnes') }} as string) as id_cnes,
            safe_cast({{ process_null('estabelecimento_nome') }} as string) as estabelecimento_nome,
            safe_cast({{ process_null('material_descricao') }} as string) as material_descricao,
            safe_cast({{ process_null('id_atc') }} as string) as id_atc,
            safe_cast({{ process_null('id_material') }} as string) as id_material,
            safe_cast({{ process_null('id_lote') }} as string) as id_lote,
            safe_cast({{ process_null('estoque_movimento_data_hora') }} as datetime) as estoque_movimento_data_hora,
            safe_cast({{ process_null('estoque_movimento_tipo') }} as string) as estoque_movimento_tipo,
            safe_cast({{ process_null('estoque_movimento_correcao_tipo') }} as string) as estoque_movimento_correcao_tipo,
            safe_cast({{ process_null('estoque_movimento_justificativa') }} as string) as estoque_movimento_justificativa,
            safe_cast({{ process_null('dispensacao_prescritor_cpf') }} as string) as dispensacao_prescritor_cpf,
            safe_cast({{ process_null('dispensacao_prescritor_cns') }} as string) as dispensacao_prescritor_cns,
            safe_cast({{ process_null('dispensacao_paciente_cpf') }} as string) as dispensacao_paciente_cpf,
            safe_cast({{ process_null('dispensacao_paciente_cns') }} as string) as dispensacao_paciente_cns,
            safe_cast({{ process_null('material_quantidade') }} as int64) as material_quantidade,
            safe_cast({{ process_null('id_pedido_wms') }} as string) as id_pedido_wms,
            safe_cast({{ process_null('estoque_armazem_origem') }} as string) as estoque_armazem_origem,
            safe_cast({{ process_null('estoque_armazem_destino') }} as string) as estoque_armazem_destino,
            safe_cast({{ process_null('data_replicacao') }} as datetime) as data_replicacao,
            safe_cast({{ process_null('requisicao_id_cnes') }} as string) as requisicao_id_cnes,
            safe_cast({{ process_null('requisicao_area_programatica') }} as string) as requisicao_area_programatica,
            safe_cast({{ process_null('requisicao_endpoint') }} as string) as requisicao_endpoint,
            safe_cast({{ process_null('loaded_at') }} as timestamp) as loaded_at,
        from renamed
    ),

    final as (

        select
            -- Primary Key
            concat(id_cnes, '.', id_estoque_movimento_local) as id_estoque_movimento,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id_cnes",
                        "id_material",
                        "id_lote",
                        "estoque_armazem_origem",
                        "estoque_armazem_destino",
                        "estoque_movimento_data_hora",
                        "material_quantidade"
                    ]
                )
            }} as id_surrogate, 

            -- Foreign Key
            area_programatica,
            id_cnes,
            id_pedido_wms,
            id_material,
            id_atc,

            -- Common Fields
            estabelecimento_nome,
            material_descricao,
            estoque_movimento_data_hora,
            id_lote,
            estoque_movimento_tipo,
            estoque_movimento_correcao_tipo,
            estoque_movimento_justificativa,
            estoque_armazem_origem,
            estoque_armazem_destino,
            dispensacao_prescritor_cpf,
            dispensacao_prescritor_cns,
            dispensacao_paciente_cpf,
            dispensacao_paciente_cns,
            material_quantidade,

            requisicao_id_cnes,
            requisicao_area_programatica,
            requisicao_endpoint,

            struct(
                estoque_movimento_data_hora as updated_at,
                data_replicacao as extracted_at,
                loaded_at
            ) as metadados,
            
            cast(estoque_movimento_data_hora as date) as particao_data_movimento

        from casted
    )

select *
from final
qualify row_number() over(partition by id_surrogate order by metadados.updated_at desc) = 1