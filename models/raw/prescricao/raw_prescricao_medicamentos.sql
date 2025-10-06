{{
    config(
        schema="brutos_prontuario_carioca_saude_mental_prescricao",
        alias="medicamentos",
        materialized="table",
        tags=["raw", "pcsm", "medicamentos"],
        description="Produtos farmacêuticos disponíveis para dispensação e uso nos hospitais e clínicas da rede municipal de saúde do Rio de Janeiro."
    )
}}

select
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.id_medicamento'))") }} as id_medicamento,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.id_prescricao'))") }} as id_prescricao,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.nome'))") }} as nome,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.via'))") }} as via_administracao,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.dose'))") }} as dose,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.intervalo'))") }} as intervalo,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.apresentacao'))") }} as apresentacao,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.observacao'))") }} as observacao,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.previsao'))") }} as previsao,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.justificativa'))") }} as justificativa,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.soro'))") }} as soro,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.qtd_atendida'))") }} as quantidade_atendida,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.dose_tot'))") }} as dose_total_prescrita,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.dias_medic'))") }} as total_dias_tratamento,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.qtd_va'))") }} as quantidade_estoque,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.medicva'))") }} as medicamento_disponivel,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.datispens'))") }} as data_dispensacao,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.numqtddispensdia'))") }} as quantidade_dispensada_dia,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.hidratacao'))") }} as hidratacao_associada,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.numqtdposo2'))") }} as quantidade_posologia2,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.numqtdposo3'))") }} as quantidade_posologia3,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.cod_med_hospub'))") }} as codigo_medicamento,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.cpf_farmaceutico'))") }} as farmaceutico_cpf,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.nome_farmaceutico'))") }} as farmaceutico_nome,
    {{ process_null("(json_extract_scalar(_airbyte_data, '$.etiqueta_aprasamento'))") }} as etiqueta_aprasamento,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('airbyte_internal', 'brutos_prescricao_staging_raw__stream_fa_medicamento') }}