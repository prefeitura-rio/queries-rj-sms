{{
    config(
        schema="brutos_minhasaude_mongodb",
        alias="perfil_acessos",
        materialized="incremental",
        unique_key="_id",
        cluster_by=["_id", "idusuario"],
        partition_by={
            "field": "createdat",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}


with
    particao as (
        select distinct data_extracao
        from {{ ref ("raw_minhasaude_mongodb__modulos_perfil_acessos") }}
    ),

    source as (
        select
            _id,
            idmodulo,
            idusuario,

            nome,
            lpad(cpf, 11, '0') as cpf,
            lpad(cns, 15, '0') as cns,

            safe_cast(safe_cast(logingovbr as int64) as bool) as logingovbr,
            origem,

            date(safe_cast(createdat as timestamp), 'America/Sao_Paulo') as createdat,
            safe_cast(updatedat as datetime) as updatedat,

            __v,

            date(safe_cast(data_extracao as timestamp), 'America/Sao_Paulo') as data_extracao,
        from {{ source("brutos_minhasaude_mongodb_staging", "perfil_acessos") }}

        {% if is_incremental() %}
            where date(data_particao) >= (select data_extracao from particao)
        {% endif %}
    )

select
    s.*,
    (
        {% if is_incremental() %}
            1
        {% else %}
            row_number() over (partition by _id order by updatedat desc)
        {% endif %}
    ) as rn

from source as s
{% if not is_incremental() %}
    qualify rn = 1
{% endif %}
