{{
    config(
        alias="vacinacao",
        labels={
            "dado_publico": "nao",
            "dado_pessoal": "nao",
            "dado_anonimizado": "nao",
            "dado_sensivel_saude": "nao",
        },
        partition_by={
            "field": "particao_data_vacinacao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    source as (
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "vacinacao_ap10") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "vacinacao_ap21") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "vacinacao_ap22") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "vacinacao_ap31") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "vacinacao_ap32") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "vacinacao_ap33") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "vacinacao_ap40") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "vacinacao_ap51") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "vacinacao_ap52") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "vacinacao_ap53") }}
    ),

    renamed as (
        select 
            json_extract_scalar(data, '$.ap') as area_programatica,
            json_extract_scalar(data, '$.nCnesUnidade') as id_cnes,
            json_extract_scalar(data, '$.nomeUnidadeSaude') as estabelecimento_nome,
            json_extract_scalar(data, '$.nomeEquipeSaude') as equipe_nome,
            json_extract_scalar(data, '$.codigoEquipeSaude') as id_equipe,
            json_extract_scalar(data, '$.codigoIneEquipeSaude') as id_equipe_ine,
            json_extract_scalar(data, '$.microAreaCodigo') as id_microarea,
            json_extract_scalar(data, '$.nProtuario') as paciente_id_prontuario,
            json_extract_scalar(data, '$.nCpf') as paciente_cpf,
            json_extract_scalar(data, '$.nCns') as paciente_cns,
            json_extract_scalar(data, '$.nomePessoaCadastrada') as paciente_nome,
            json_extract_scalar(data, '$.sexoNascimento') as paciente_sexo,
            json_extract_scalar(data, '$.dataNascimento') as paciente_nascimento_data,
            json_extract_scalar(data, '$.nomeMaePessoaCadastrada') as paciente_nome_mae,
            json_extract_scalar(data, '$.dataNascimentoMae') as paciente_mae_nascimento_data,
            json_extract_scalar(data, '$.situacaoUsuario') as paciente_situacao,
            json_extract_scalar(data, '$.dataCadastro') as paciente_cadastro_data,
            json_extract_scalar(data, '$.obito') as paciente_obito,
            json_extract_scalar(data, '$.vacina') as vacina_descricao,
            json_extract_scalar(data, '$.dataAplicacao') as vacina_aplicacao_data,
            json_extract_scalar(data, '$.dataHoraRegistro') as vacina_registro_data,
            json_extract_scalar(data, '$.doseVtc') as vacina_dose,
            json_extract_scalar(data, '$.lote') as vacina_lote,
            json_extract_scalar(data, '$.tipoRegistro') as vacina_registro_tipo,
            json_extract_scalar(data, '$.estrategia') as vacina_estrategia,
            json_extract_scalar(data, '$.diff') as vacina_diff,
            json_extract_scalar(data, '$.cbo') as profissional_cbo,
            json_extract_scalar(data, '$.cnsProfissional') as profissional_cns,
            json_extract_scalar(data, '$.cpfProfissional') as profissional_cpf,
            json_extract_scalar(data, '$.profissional') as profissional_nome,
            json_extract_scalar(data, '$.id') as id_vacinacao_local,
            json_extract_scalar(data, '$.dtaReplicacao') as data_replicacao,

            _source_cnes as requisicao_id_cnes,
            _source_ap as requisicao_area_programatica,
            _endpoint as requisicao_endpoint,

            _loaded_at as loaded_at
        from source
        where json_extract_scalar(data,'$.nCnesUnidade') is not null
    ),

    casted as (
        select
            safe_cast({{ process_null('id_vacinacao_local') }} as string) as id_vacinacao_local,
            safe_cast({{ process_null('area_programatica') }} as string) as area_programatica,
            safe_cast({{ process_null('id_cnes') }} as string) as id_cnes,
            safe_cast({{ proper_estabelecimento(process_null('estabelecimento_nome')) }} as string) as estabelecimento_nome,
            safe_cast({{ process_null('equipe_nome') }} as string) as equipe_nome,
            safe_cast({{ process_null('id_equipe') }} as string) as id_equipe,
            safe_cast({{ process_null('id_equipe_ine') }} as string) as id_equipe_ine,
            safe_cast({{ process_null('id_microarea') }} as string) as id_microarea,
            safe_cast({{ process_null('paciente_id_prontuario') }} as string) as paciente_id_prontuario,
            safe_cast({{ process_null('paciente_cpf') }} as string) as paciente_cpf,
            safe_cast({{ process_null('paciente_cns') }} as string) as paciente_cns,
            safe_cast({{ proper_br(process_null('paciente_nome')) }} as string) as paciente_nome,
            safe_cast({{ process_null('lower(paciente_sexo)') }} as string) as paciente_sexo,
            safe_cast({{ parse_date(process_null('paciente_nascimento_data')) }} as date) as paciente_nascimento_data,
            safe_cast({{ proper_br(process_null('paciente_nome_mae')) }} as string) as paciente_nome_mae,
            safe_cast({{ parse_date(process_null('paciente_mae_nascimento_data')) }} as date) as paciente_mae_nascimento_data,
            safe_cast({{ process_null('lower(paciente_situacao)') }} as string) as paciente_situacao,
            safe_cast({{ parse_date(process_null('paciente_cadastro_data')) }} as datetime) as paciente_cadastro_data,
            safe_cast({{ process_null('paciente_obito') }} as string) as paciente_obito,
            safe_cast({{ process_null('lower(vacina_descricao)') }} as string) as vacina_descricao,
            safe_cast({{ parse_date(process_null('vacina_aplicacao_data')) }} as date) as vacina_aplicacao_data,
            safe_cast({{ parse_date(process_null('vacina_registro_data')) }} as date) as vacina_registro_data,
            safe_cast({{ process_null('lower(vacina_dose)') }} as string) as vacina_dose,
            safe_cast({{ process_null('vacina_lote') }} as string) as vacina_lote,
            safe_cast({{ process_null('lower(vacina_registro_tipo)') }} as string) as vacina_registro_tipo,
            safe_cast({{ process_null('lower(vacina_estrategia)') }} as string) as vacina_estrategia,
            safe_cast({{ process_null('vacina_diff') }} as string) as vacina_diff,
            safe_cast({{ process_null('profissional_cbo') }} as string) as profissional_cbo,
            safe_cast({{ process_null('profissional_cns') }} as string) as profissional_cns,
            safe_cast({{ process_null('profissional_cpf') }} as string) as profissional_cpf,
            safe_cast({{ proper_br(process_null('profissional_nome')) }} as string) as profissional_nome,

            safe_cast({{ process_null('requisicao_id_cnes') }} as string) as requisicao_id_cnes,
            safe_cast({{ process_null('requisicao_area_programatica') }} as string) as requisicao_area_programatica,
            safe_cast({{ process_null('requisicao_endpoint') }} as string) as requisicao_endpoint,

            safe_cast({{ parse_date(process_null('data_replicacao')) }} as datetime) as data_replicacao,
            safe_cast({{ process_null('loaded_at') }} as timestamp) as loaded_at,
        from renamed
    ),

    final as (

        select
            -- Primary Key
            concat(id_cnes, '.', id_vacinacao_local) as id_vacinacao,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id_cnes",
                        "id_vacinacao_local",
                        "paciente_cns"
                    ]
                )
            }} as id_surrogate,

            -- Foreign Keys
            id_cnes,
            id_equipe,
            id_equipe_ine,
            id_microarea,
            paciente_id_prontuario,
            paciente_cns,

            -- Common Fields
            estabelecimento_nome,
            equipe_nome,

            profissional_nome,
            profissional_cbo,
            profissional_cns,
            profissional_cpf,

            vacina_descricao,
            vacina_dose,
            vacina_lote,
            vacina_registro_tipo,
            vacina_estrategia,
            vacina_diff,
            vacina_aplicacao_data,
            vacina_registro_data,
            
            paciente_nome,
            paciente_sexo,
            paciente_nascimento_data,
            paciente_nome_mae,
            paciente_mae_nascimento_data,
            paciente_situacao,
            paciente_cadastro_data,
            paciente_obito,

            requisicao_id_cnes,
            requisicao_area_programatica,
            requisicao_endpoint,

            struct(
                safe_cast(vacina_aplicacao_data as datetime) as updated_at,
                safe_cast(data_replicacao as datetime) as extracted_at,
                safe_cast(loaded_at as timestamp) as loaded_at
            ) as metadados,

            cast(vacina_aplicacao_data as date) as particao_data_vacinacao

        from casted
    )

select *
from final
qualify row_number() over(partition by id_surrogate order by metadados.updated_at desc) = 1
