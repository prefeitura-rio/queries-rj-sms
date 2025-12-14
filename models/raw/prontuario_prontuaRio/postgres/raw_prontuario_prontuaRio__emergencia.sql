{{
    config(
        alias="emergencia",
        materialized="table",
        tags=["prontuaRio"],
    )
}}

with 

    source_ as (
        select * from {{source('brutos_prontuario_prontuaRIO', 'hp_rege_emerg') }}
    )

    emergencia as (
        select
            json_extract_scalar(data, '$.be') as id_boletim,
            json_extract_scalar(data, '$.num_cor') as numero_cor,
            json_extract_scalar(data, '$.cor') as cor,
            safe_cast(json_extract_scalar(data, '$.tempo_espera') as datetime) as tempo_espera,
            json_extract_scalar(data, '$.nome_pac') as paciente_nome,
            json_extract_scalar(data, '$.motivo') as motivo,
            json_extract_scalar(data, '$.dt_ent_saida') as data_entrada_saida,
            json_extract_scalar(data, '$.cpf_emerg') as cpf_emergencia,
            cnes,
            loaded_at
    from source_
    ),

    deduplicated as (
        select * from emergencia 
        qualify row_number() over(partition by id_boletim, cnes order by loaded_at desc) = 1
    )

select *, date(loaded_at) as data_particao
from deduplicated