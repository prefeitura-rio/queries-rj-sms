{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="profissional_atividade",
        materialized="table",
        tags=["prontuaRio"],
    )
}}

with 
    source_ as (
        select * from {{ source('brutos_prontuario_prontuaRio_staging', 'intb0') }}
    ),

    ocupacoes as (
        select 
            json_extract_scalar(data, '$.ib0codigo') as id_atividade,
            json_extract_scalar(data, '$.ib0descr') as descricao,
            cnes,
            safe_cast(loaded_at as timestamp) as loaded_at,
        from source_
    )

    final as (
        select 
            concat(cnes, '.', id_atividade) as gid_atividade,
            id_atividade,
            case 
                when descricao = 'EDUCADORA EM SAUDE' then 'EDUCADOR(A) EM SAÚDE'
                when descricao = 'ENFERMEIRA DE PACS' then 'ENFERMEIRO(A) DE PACS'
                when descricao = 'TECNICO DE ENFERMAGEM' then 'TÉCNICO(A) DE ENFERMAGEM'
                when descricao = 'ENFERMEIRA OBSTETRA' then 'ENFERMEIRO(A) OBSTETRA'
                else upper(descricao)
            end as descricao,
        from ocupacoes

    )

qualify row_number() over(partition by id_atividade, cnes, descricao order by loaded_at desc)=1