{{
    config(
        alias="anamnese",
        materialized="incremental",
        schema='brutos_prontuario_mv',
        unique_key = "id_hci",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=['mv'],
        
    )
}}


with 

source as (
    select * 
    from {{ source("brutos_prontuario_mv_api_staging", "anamnese_continuo") }}   
    {% if is_incremental() %}
        where TIMESTAMP_TRUNC(datalake_loaded_at, DAY) >= TIMESTAMP(date_sub(current_date('America/Sao_Paulo'), interval 30 day))
    {% endif %}
),


anamnese_json as (
    select 
        payload_cnes as id_cnes,
        json_extract_scalar(data, '$.numero_atendimento') as numero_atendimento,
        json_extract_scalar(data, '$.data_hora_fechamento') as data_hora_fechamento,
        json_extract_scalar(data, '$.queixa_principal') as queixa_principal,
        json_extract_scalar(data, '$.historia_doenca_atual') as historia_doenca_atual,
        json_extract_scalar(data, '$.exame_fisico_resultados') as exame_fisico_resultados,
        json_extract_scalar(data, '$.exames_complementares') as exames_complementares,
        json_extract_scalar(data, '$.ultimas_afericoes') as ultimas_afericoes,
        json_extract_scalar(data, '$.hipotese_diagnostica') as hipotese_diagnostica,
        json_extract_scalar(data, '$.cid') as cid,
        json_extract_scalar(data, '$.conduta_proposta') as conduta_proposta,
        json_extract_scalar(data, '$.plano_terapeutico') as plano_terapeutico,
        json_extract_scalar(data, '$.necessidade_retorno') as necessidade_retorno,
        json_extract_scalar(data, '$.destino_paciente') as destino_paciente,
        json_extract_scalar(data, '$.profissional_saude_nome') as profissional_saude_nome,
        json_extract_scalar(data, '$.unidade_atendimento_nome') as unidade_atendimento_nome,
        json_extract_scalar(data, '$.nome_paciente') as nome_paciente,
        json_extract_scalar(data, '$.cpf_paciente') as cpf_paciente,
        json_extract_scalar(data, '$.cns_paciente') as cns_paciente,
        json_extract_scalar(data, '$.nome_social_paciente') as nome_social_paciente,
        json_extract_scalar(data, '$.data_nascimento_paciente') as data_nascimento_paciente,
        json_extract_scalar(data, '$.data_atendimento') as data_atendimento,
        json_extract_scalar(data, '$.sexo_paciente') as sexo_paciente,
        json_extract_scalar(data, '$.pcd') as pcd,
        json_extract_scalar(data, '$.nome_mae') as nome_mae,
        json_extract_scalar(data, '$.pas') as pas,
        json_extract_scalar(data, '$.peso') as peso,
        json_extract_scalar(data, '$.sc') as sc,
        json_extract_scalar(data, '$.spo2') as spo2,
        json_extract_scalar(data, '$.temp') as temp,
        json_extract_scalar(data, '$.dor') as dor,
        json_extract_scalar(data, '$.usoMedicamentos') as usoMedicamentos,
        json_extract_scalar(data, '$.escalaDor') as escalaDor,
        json_extract_scalar(data, '$.especialidadeRetorno') as especialidadeRetorno,
        json_extract_scalar(data, '$.dataRetorno') as dataRetorno,
        json_extract_scalar(data, '$.detalheDor') as detalheDor,
        json_extract_scalar(data, '$.diagnosticoProvavel') as diagnosticoProvavel,
        json_extract_scalar(data, '$.historiaFamiliar') as historiaFamiliar,
        json_extract_scalar(data, '$.historiaPregressa') as historiaPregressa,
        json_extract_scalar(data, '$.historiaSocial') as historiaSocial,
        json_extract_scalar(data, '$.descricaoIMC') as descricaoIMC,
        json_extract_scalar(data, '$.quaisMedicamentos') as quaisMedicamentos,
        json_extract_scalar(data, '$.altura') as altura,
        json_extract_scalar(data, '$.fc') as fc,
        json_extract_scalar(data, '$.fr') as fr,
        json_extract_scalar(data, '$.imc') as imc,
        json_extract_scalar(data, '$.pad') as pad,
        json_extract_scalar(data, '$.tipoAtendimento') as tipoAtendimento,
        json_extract_scalar(data, '$.especialidadeAtendimento') as especialidadeAtendimento,
        source_updated_at,
        datalake_loaded_at
    from source
),

anamnese_renomeado as (
    select
        -- Atendimento
        {{ process_null('numero_atendimento') }} as id_atendimento,
        {{ process_null('id_cnes') }} as id_cnes,
        {{ process_null('unidade_atendimento_nome') }} as estabelecimento_nome,
        safe.parse_datetime('%Y/%m/%d %H:%M:%S', data_atendimento) as atendimento_datahora,
        case 
            when tipoAtendimento like 'A' then 'AMBULATORIAL'
            when tipoAtendimento like 'B' then 'BUSCA ATIVA'
            when tipoAtendimento like 'E' then 'EXTERNO'
            when tipoAtendimento like 'H' then 'HOME CARE'
            when tipoAtendimento like 'I' then 'INTERNAÇÃO'
            when tipoAtendimento like 'S' then 'SUS - AIH'
            when tipoAtendimento like 'U' then 'URGÊNCIA'
            else tipoAtendimento
        end as atendimento_tipo,
        {{ process_null('especialidadeAtendimento') }} as atendimento_especialidade,
        {{ process_null('profissional_saude_nome') }} as profissional_nome,
        safe.parse_datetime('%Y/%m/%d %H:%M:%S', data_hora_fechamento) as fechamento_datahora,

        -- Paciente
        {{ process_null('nome_paciente') }} as paciente_nome,
        {{ process_null('cpf_paciente') }} as paciente_cpf,
        {{ process_null('cns_paciente') }} as paciente_cns,
        {{ process_null('nome_social_paciente') }} as paciente_nome_social,
        safe.parse_datetime('%Y/%m/%d', data_nascimento_paciente) as paciente_data_nascimento,
        {{ process_null('sexo_paciente') }} as paciente_sexo,
        {{ process_null('pcd') }} as paciente_pcd,
        {{ process_null('nome_mae') }} as paciente_mae_nome,

        -- Anamnese
        {{ process_null('queixa_principal') }} as queixa_principal,
        {{ process_null('historia_doenca_atual') }} as historia_doenca_atual,
        {{ process_null('historiaPregressa') }} as historia_pregressa,
        {{ process_null('historiaFamiliar') }} as historia_familiar,
        {{ process_null('historiaSocial') }} as historia_social,
        {{ process_null('usoMedicamentos') }} as medicamentos_em_uso,
        {{ process_null('quaisMedicamentos') }} as medicamentos_quais,

        -- Exame Físico e Sinais Vitais
        {{ process_null('exame_fisico_resultados') }} as exame_fisico,
        {{ process_null('ultimas_afericoes') }} as sinais_vitais_ultimas_afericoes,
        safe_cast({{ process_null('pas') }} as float64) as pressao_arterial_sistolica,
        safe_cast({{ process_null('pad') }} as float64) as pressao_arterial_diastolica,
        safe_cast({{ process_null('fc') }} as float64) as frequencia_cardiaca,
        safe_cast({{ process_null('fr') }} as float64) as frequencia_respiratoria,
        safe_cast({{ process_null('temp') }} as float64) as temperatura,
        safe_cast({{ process_null('spo2') }} as float64) as saturacao_oxigenio,
        safe_cast({{ process_null('peso') }} as float64) as peso,
        safe_cast({{ process_null('altura') }} as float64) as altura,
        safe_cast({{ process_null('imc') }} as float64) as imc,
        {{ process_null('descricaoIMC') }} as imc_descricao,
        safe_cast({{ process_null('sc') }} as float64) as superficie_corporal,
        {{ process_null('dor') }} as dor_indicador,
        {{ process_null('escalaDor') }} as dor_escala,
        {{ process_null('detalheDor') }} as dor_detalhes,

        -- Diagnóstico e Conduta
        {{ process_null('hipotese_diagnostica') }} as hipotese_diagnostica,
        {{ process_null('diagnosticoProvavel') }} as diagnostico_provavel,
        {{ process_null('cid') }} as cid,
        {{ process_null('exames_complementares') }} as exames_complementares,
        {{ process_null('conduta_proposta') }} as conduta_proposta,
        {{ process_null('plano_terapeutico') }} as plano_terapeutico,
        {{ process_null('destino_paciente') }} as destino_paciente,
        {{ process_null('necessidade_retorno') }} as retorno_necessidade,
        safe.parse_date('%d/%m/%Y', dataRetorno) as retorno_data,
        {{ process_null('especialidadeRetorno') }} as retorno_especialidade,

        -- Metadados
        datetime(datalake_loaded_at, 'America/Sao_Paulo') as loaded_at,
        parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at) as updated_at,
        cast(datalake_loaded_at as date) as data_particao
    from anamnese_json
),

anamnese_deduplicado as (
    select *
    from anamnese_renomeado
    qualify row_number() over (partition by id_atendimento, id_cnes order by updated_at desc) = 1
)

select 
    {{ dbt_utils.generate_surrogate_key(['id_atendimento', 'id_cnes']) }} as id_hci,
    *
from anamnese_deduplicado
