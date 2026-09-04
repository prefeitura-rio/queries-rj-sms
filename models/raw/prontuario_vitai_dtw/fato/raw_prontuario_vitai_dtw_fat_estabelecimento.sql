{{
    config(
        alias="fat_estabelecimento",
        materialized="table",
        unique_key="estabelecimento_gid"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fato_estabelecimento") }}
),

renomeado as (
    select
        safe_cast(datahora as datetime) as datahora,
        safe_cast(sigla as string) as sigla,
        safe_cast(cnes as string) as cnes,
        safe_cast(cnpj as string) as cnpj,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
        safe_cast(nomeestabelecimento as string) as nomeestabelecimento,
        safe_cast(versao_atual as string) as versao_atual,
        safe_cast(tp_unid_id as string) as tp_unid_id,
        safe_cast(cd_subtipo as string) as cd_subtipo,
        safe_cast(area_programatica as string) as area_programatica,
        safe_cast(cep as string) as cep,
        safe_cast(inicio_faturamento as datetime) as inicio_faturamento,
        safe_cast(latitude as string) as latitude,
        safe_cast(longitude as string) as longitude,
        safe_cast(tipounidade as string) as tipounidade,
        safe_cast(atende_virtual as bool) as atende_virtual,
        safe_cast(raio_cobertura as int64) as raio_cobertura,
        safe_cast(monitora as bool) as monitora,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(numero as string) as numero,
        safe_cast(complemento as string) as complemento,
        safe_cast(bai_id as int64) as bai_id,
        safe_cast(uf as string) as uf,
        safe_cast(endereco as string) as endereco,
        safe_cast(mun_id as int64) as mun_id,
        
        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
