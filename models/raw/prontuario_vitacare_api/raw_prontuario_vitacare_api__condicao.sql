{{
    config(
        alias="condicao",
        labels={
            "dado_publico": "nao",
            "dado_pessoal": "nao",
            "dado_anonimizado": "nao",
            "dado_sensivel_saude": "nao",
        },
        partition_by={
            "field": "particao_data_consulta",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    source as (
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "condicao_ap10") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "condicao_ap21") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "condicao_ap22") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "condicao_ap31") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "condicao_ap32") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "condicao_ap33") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "condicao_ap40") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "condicao_ap51") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "condicao_ap52") }}
        union all
        select * from {{ source("brutos_prontuario_vitacare_api_staging", "condicao_ap53") }}
    ),

    renamed as (
        select 
            json_extract_scalar(data, '$.nCnsUnidade') as id_cnes,
            json_extract_scalar(data, '$.ap') as area_programatica,
            json_extract_scalar(data, '$.unidade') as unidade,

            json_extract_scalar(data, '$.numeroIneEquipe') as id_ine,
            json_extract_scalar(data, '$.numeroCnesProfissional') as id_cns_profissional,
            json_extract_scalar(data, '$.nomeProfissional') as nome_profissional,
            json_extract_scalar(data, '$.cboProfissional') as cbo_profissional,

            json_extract_scalar(data, '$.tipoAtendimento') as tipo_atendimento,
            json_extract_scalar(data, '$.dataConsulta') as data_consulta,

            json_extract_scalar(data, '$.nomePaciente') as nome_paciente,
            json_extract_scalar(data, '$.cpfPaciente') as cpf_paciente,
            json_extract_scalar(data, '$.cnsPaciente') as cns_paciente,
            json_extract_scalar(data, '$.sexoPaciente') as sexo_paciente,
            json_extract_scalar(data, '$.dataNascPaciente') as data_nasc_paciente,
            json_extract_scalar(data, '$.racaPaciente') as raca_paciente,

            json_extract(data, '$.diagnosticosCid') as diagnosticos_cid,
            json_extract(data, '$.diagnosticosCiap') as diagnosticos_ciap,

            json_extract_scalar(data, '$.peso') as peso,
            json_extract_scalar(data, '$.altura') as altura,
            json_extract_scalar(data, '$.paMax') as pa_max,
            json_extract_scalar(data, '$.paMin') as pa_min,
            json_extract_scalar(data, '$.temperatura') as temperatura,
            json_extract_scalar(data, '$.saturacaoO2') as saturacao_o2,
            json_extract_scalar(data, '$.pacienteTemporario') as paciente_temporario,
            json_extract_scalar(data, '$.pacienteSituacaoRua') as paciente_situacao_rua,

            json_extract_scalar(data, '$.dtaReplicacao') as data_replicacao,

            _source_cnes as requisicao_id_cnes,
            _source_ap as requisicao_area_programatica,
            _endpoint as requisicao_endpoint,

            _loaded_at as loaded_at
        from source
    ),

    casted as (
        select
            safe_cast({{ process_null('id_cnes') }} as string) as id_cnes,
            safe_cast({{ process_null('id_ine') }} as string) as id_ine,
            safe_cast({{ process_null('id_cns_profissional') }} as string) as id_cns_profissional,
            safe_cast({{ process_null('area_programatica') }} as string) as area_programatica,
            safe_cast({{ process_null('unidade') }} as string) as unidade,
            safe_cast({{ process_null('nome_profissional') }} as string) as nome_profissional,
            safe_cast({{ process_null('cbo_profissional') }} as string) as cbo_profissional,
            safe_cast({{ process_null('tipo_atendimento') }} as string) as tipo_atendimento,

            safe_cast({{ parse_date(process_null('data_consulta')) }} as date) as data_consulta,
            safe_cast({{ process_null('nome_paciente') }} as string) as nome_paciente,
            safe_cast({{ process_null('cpf_paciente') }} as string) as cpf_paciente,
            safe_cast({{ process_null('cns_paciente') }} as string) as cns_paciente,
            safe_cast({{ process_null('sexo_paciente') }} as string) as sexo_paciente,
            safe_cast({{ parse_date(process_null('data_nasc_paciente')) }} as date) as data_nasc_paciente,
            safe_cast({{ process_null('raca_paciente') }} as string) as raca_paciente,
            safe_cast({{ process_null('diagnosticos_cid') }} as string) as diagnosticos_cid,
            safe_cast({{ process_null('diagnosticos_ciap') }} as string) as diagnosticos_ciap,
            safe_cast({{ process_null('peso') }} as float64) as peso,
            safe_cast({{ process_null('altura') }} as float64) as altura,
            safe_cast({{ process_null('pa_max') }} as float64) as pa_max,
            safe_cast({{ process_null('pa_min') }} as float64) as pa_min,
            safe_cast({{ process_null('temperatura') }} as float64) as temperatura,
            safe_cast({{ process_null('saturacao_o2') }} as float64) as saturacao_o2,
            safe_cast({{ process_null('paciente_temporario') }} as string) as paciente_temporario,
            safe_cast({{ process_null('paciente_situacao_rua') }} as string) as paciente_situacao_rua,

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
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id_cnes",
                        "cpf_paciente",
                        "data_consulta",
                        "tipo_atendimento"
                    ]
                )
            }} as id_surrogate,
            
            -- Foreign Keys
            safe_cast(id_cnes as string) as id_cnes,
            safe_cast(id_ine as string) as id_ine,
            safe_cast(id_cns_profissional as string) as id_cns_profissional,
            safe_cast(area_programatica as string) as area_programatica,
            safe_cast(unidade as string) as unidade,

            -- Common Fields
            nome_profissional,
            cbo_profissional,

            nome_paciente,
            cpf_paciente,
            cns_paciente,
            sexo_paciente,
            data_nasc_paciente,
            raca_paciente,

            tipo_atendimento,
            data_consulta,

            diagnosticos_cid,
            diagnosticos_ciap,
            peso,
            altura,
            pa_max,
            pa_min,
            temperatura,
            saturacao_o2,
            paciente_temporario,
            paciente_situacao_rua,

            requisicao_id_cnes,
            requisicao_area_programatica,
            requisicao_endpoint,

            struct(
                safe_cast(data_consulta as datetime) as updated_at,
                safe_cast(data_replicacao as datetime) as extracted_at,
                safe_cast(loaded_at as timestamp) as loaded_at
            ) as metadados,

            cast(data_consulta as date) as particao_data_consulta

        from casted
    )

select *
from final
qualify row_number() over(partition by id_surrogate order by metadados.updated_at desc) = 1
