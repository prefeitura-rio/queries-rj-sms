{{
    config(
        alias="bam",
        materialized="incremental",
        schema='brutos_prontuario_mv',
        incremental_strategy="insert_overwrite",
        unique_key = "id_hci",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=['mv']
    )
}}


with 

source as (
    select * 
    from {{ source("brutos_prontuario_mv_api_staging", "bam_continuo") }}   
    {% if is_incremental() %}
        where TIMESTAMP_TRUNC(datalake_loaded_at, DAY) >= TIMESTAMP(date_sub(current_date('America/Sao_Paulo'), interval 7 day))
    {% endif %}
),

bam_json as (
    select 
        json_extract_scalar(data, '$.numero_atendimento') as numero_atendimento,
        payload_cnes as id_cnes,
        json_extract_scalar(data, '$.data_hora_fechamento') as data_hora_fechamento,
        json_extract_scalar(data, '$.sinal_vital_pas') as sinal_vital_pas,
        json_extract_scalar(data, '$.sinal_vital_pad') as sinal_vital_pad,
        json_extract_scalar(data, '$.sinal_vital_fc') as sinal_vital_fc,
        json_extract_scalar(data, '$.sinal_vital_tax') as sinal_vital_tax,
        json_extract_scalar(data, '$.sinal_vital_spo2') as sinal_vital_spo2,
        json_extract_scalar(data, '$.sinal_vital_fr') as sinal_vital_fr,
        json_extract_scalar(data, '$.queixa_principal') as queixa_principal,
        json_extract_scalar(data, '$.medicamento_uso_continio') as medicamento_uso_continio,
        json_extract_scalar(data, '$.exame_fisico') as exame_fisico,
        json_extract_scalar(data, '$.exames_complementares') as exames_complementares,
        json_extract_scalar(data, '$.hipotese_diagnotica') as hipotese_diagnotica,
        json_extract_scalar(data, '$.conduta_procedimento') as conduta_procedimento,
        json_extract_scalar(data, '$.destino_pos_atendimento') as destino_pos_atendimento,
        json_extract_scalar(data, '$.profissional_saude_nome') as profissional_saude_nome,
        json_extract_scalar(data, '$.unidade_atendimento_nome') as unidade_atendimento_nome,
        json_extract_scalar(data, '$.risco_suicidio') as risco_suicidio,
        json_extract_scalar(data, '$.classificacao_risco') as classificacao_risco,
        json_extract_scalar(data, '$.alergia') as alergia,
        json_extract_scalar(data, '$.nome_paciente') as nome_paciente,
        json_extract_scalar(data, '$.cpf_paciente') as cpf_paciente,
        json_extract_scalar(data, '$.cns_paciente') as cns_paciente,
        json_extract_scalar(data, '$.nome_social_paciente') as nome_social_paciente,
        json_extract_scalar(data, '$.data_atendimento') as data_atendimento,
        json_extract_scalar(data, '$.data_nascimento_paciente') as data_nascimento_paciente,
        json_extract_scalar(data, '$.sexo_paciente') as sexo_paciente,
        json_extract_scalar(data, '$.pcd') as pcd,
        json_extract_scalar(data, '$.nome_mae') as nome_mae,
        json_extract_scalar(data, '$.ds_historico_doenca_atual') as ds_historico_doenca_atual,
        json_extract_scalar(data, '$.ds_queixa_medica') as ds_queixa_medica,
        json_extract_scalar(data, '$.ds_historico_doenca_pregressa') as ds_historico_doenca_pregressa,
        json_extract_scalar(data, '$.ds_historico_social') as ds_historico_social,
        json_extract_scalar(data, '$.ds_exame_fisico_especifico') as ds_exame_fisico_especifico,
        json_extract_scalar(data, '$.tipoAtendimento') as tipoAtendimento,
        json_extract_scalar(data, '$.especialidadeAtendimento') as especialidadeAtendimento,
        source_updated_at,
        datalake_loaded_at
    from source
),

bam_renomeado as (
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
        safe.parse_date('%Y/%m/%d', data_nascimento_paciente) as paciente_data_nascimento,
        {{ process_null('sexo_paciente') }} as paciente_sexo,
        {{ process_null('pcd') }} as paciente_pcd,
        {{ process_null('nome_mae') }} as paciente_mae_nome,
        {{ process_null('alergia') }} as paciente_alergia,

        -- BAM (Boletim de Atendimento Médico)
        {{ process_null('queixa_principal') }} as queixa_principal,
        {{ process_null('ds_queixa_medica') }} as queixa_medica,
        {{ process_null('ds_historico_doenca_atual') }} as historia_doenca_atual,
        {{ process_null('ds_historico_doenca_pregressa') }} as historia_doenca_pregressa,
        {{ process_null('ds_historico_social') }} as historia_social,
        {{ process_null('medicamento_uso_continio') }} as medicamento_uso_continuo,

        -- Exame Físico e Sinais Vitais
        {{ process_null('exame_fisico') }} as exame_fisico,
        {{ process_null('ds_exame_fisico_especifico') }} as exame_fisico_especifico,
        safe_cast({{ process_null('sinal_vital_pas') }} as float64) as pressao_arterial_sistolica,
        safe_cast({{ process_null('sinal_vital_pad') }} as float64) as pressao_arterial_diastolica,
        safe_cast({{ process_null('sinal_vital_fc') }} as float64) as frequencia_cardiaca,
        safe_cast({{ process_null('sinal_vital_fr') }} as float64) as frequencia_respiratoria,
        safe_cast({{ process_null('sinal_vital_tax') }} as float64) as temperatura,
        safe_cast({{ process_null('sinal_vital_spo2') }} as float64) as saturacao_oxigenio,

        -- Diagnóstico e Conduta
        {{ process_null('hipotese_diagnotica') }} as hipotese_diagnostica,
        {{ process_null('exames_complementares') }} as exames_complementares,
        {{ process_null('conduta_procedimento') }} as conduta_proposta,
        {{ process_null('destino_pos_atendimento') }} as destino_paciente,
        {{ process_null('risco_suicidio') }} as risco_suicidio,
        {{ process_null('classificacao_risco') }} as classificacao_risco,

        -- Metadados
        datetime(datalake_loaded_at, 'America/Sao_Paulo') as loaded_at,
        parse_datetime('%Y/%m/%d', source_updated_at) as updated_at,
        cast(datalake_loaded_at as date) as data_particao
    from bam_json
),

bam_deduplicado as (
    select *
    from bam_renomeado
    qualify row_number() over (partition by id_atendimento, id_cnes order by updated_at desc) = 1

)

select 
    {{ dbt_utils.generate_surrogate_key(['id_atendimento', 'id_cnes']) }} as id_hci,
    *
from bam_deduplicado