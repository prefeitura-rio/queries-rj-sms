{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="medicamento",
        materialized="table",
        tags=["prontuaRio"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


with 

    source_ as (
        select * from {{source('brutos_prontuario_prontuaRio_staging', 'medicamento') }} 
    ),

    medicamento as (
        select
            json_extract_scalar(data, '$.id_prescricao') as id_prescricao,
            json_extract_scalar(data, '$.id_medicamento') as id_medicamento,
            json_extract_scalar(data, '$.nome') as medicamento_nome,
            json_extract_scalar(data, '$.via') as via,
            json_extract_scalar(data, '$.dose') as dose,
            json_extract_scalar(data, '$.intervalo') as intervalo,
            json_extract_scalar(data, '$.previsao') as previsao,
            json_extract_scalar(data, '$.justificativa') as justificativa,
            json_extract_scalar(data, '$.observacoes') as observacoes,
            json_extract_scalar(data, '$.qtd_atendida') as qtd_atendida,
            json_extract_scalar(data, '$.cod_med_hospub') as cod_med_hospub,
            json_extract_scalar(data, '$.cpf_farmaceutico') as cpf_farmaceutico,
            json_extract_scalar(data, '$.nome_farmaceutico') as nome_farmaceutico,
            json_extract_scalar(data, '$.apresentacao') as apresentacao,
            json_extract_scalar(data, '$.dose_tot') as dose_tot,
            json_extract_scalar(data, '$.dias_medic') as dias_medic,
            json_extract_scalar(data, '$.soro') as soro,
            json_extract_scalar(data, '$.hidratacao') as hidratacao,
            json_extract_scalar(data, '$.qtd_va') as qtd_va,
            json_extract_scalar(data, '$.medicva') as medicva,
            json_extract_scalar(data, '$.etiqueta_aprasamento') as etiqueta_aprasamento,
            cnes,
            loaded_at
        from source_
    ),

    final as (
        select 
            safe_cast(id_prescricao as int64) as id_prescricao,
            safe_cast(id_medicamento as int64) as id_medicamento,
            {{ process_null('medicamento_nome') }} as medicamento_nome,
            {{ process_null('via') }} as via,
            {{ process_null('dose') }} as dose,
            {{ process_null('intervalo') }} as intervalo,
            {{ process_null('previsao') }} as previsao,
            {{ process_null('justificativa') }} as justificativa,
            {{ process_null('observacoes') }} as observacoes,
            {{ process_null('qtd_atendida') }} as qtd_atendida,
            {{ process_null('cod_med_hospub') }} as cod_med_hospub,
            {{ process_null('cpf_farmaceutico') }} as cpf_farmaceutico,
            {{ process_null('nome_farmaceutico') }} as nome_farmaceutico,
            {{ process_null('apresentacao') }} as apresentacao,
            {{ process_null('dose_tot') }} as dose_tot,
            {{ process_null('dias_medic') }} as dias_medic,
            {{ process_null('soro') }} as soro,
            {{ process_null('hidratacao') }} as hidratacao,
            {{ process_null('qtd_va') }} as qtd_va,
            {{ process_null('medicva') }} as medicva,
            {{ process_null('etiqueta_aprasamento') }} as etiqueta_aprasamento,
            cnes,
            loaded_at,
            cast(safe_cast(loaded_at as timestamp) as date) as data_particao
        from medicamento
        qualify row_number() over(partition by id_prescricao, id_medicamento, cnes order by loaded_at desc) = 1
    )

select
    concat(cnes, '.', id_prescricao) as gid_prescricao,
    concat(cnes, '.', id_medicamento) as gid_medicamento,
    *
from final