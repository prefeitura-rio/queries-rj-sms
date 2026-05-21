{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__profissionais',
        materialized = "table",
        tags = ["subpav", "cnes_aps"]
    )
}}

with fonte as (
    select
        json,
        _source_file,
        safe_cast(_loaded_at as timestamp) as loaded_at,
        safe_cast(data_particao as date) as data_particao,
        ano_particao,
        mes_particao
    from {{ source("brutos_gdb_cnes_staging", "LFCES018") }}
),

extraido as (
    select
        -- metadados da carga
        data_particao,
        ano_particao,
        mes_particao,
        loaded_at,
        _source_file,

        -- identificação do profissional
        nullif(json_value(json, '$.PROF_ID'), '') as profissional_id_original,
        lpad(nullif(json_value(json, '$.CPF_PROF'), ''), 11, '0') as cpf,
        lpad(nullif(json_value(json, '$.COD_CNS'), ''), 15, '0') as cns,
        nullif(json_value(json, '$.NOME_PROF'), '') as nome,

        -- dados pessoais
        safe_cast(nullif(json_value(json, '$.DATA_NASC'), '') as date) as dt_nasc,
        nullif(json_value(json, '$.SEXO'), '') as sexo_id_original,
        nullif(json_value(json, '$.CD_RACA'), '') as raca_cor_id_original,
        nullif(json_value(json, '$.CODESCOLAR'), '') as nivel_escolaridade_id_original,
        nullif(json_value(json, '$.IND_NACIO'), '') as ind_nacio,
        nullif(json_value(json, '$.NOME_PAIS'), '') as nome_pais,

        -- contato
        nullif(json_value(json, '$.TELEFONE'), '') as telefone,
        nullif(json_value(json, '$.NO_EMAIL'), '') as email,

        -- atualização
        safe_cast(nullif(json_value(json, '$.DATA_ATU'), '') as date) as dt_atualiza
    from fonte
),

tratado as (
    select
        *,

        case
            when sexo_id_original in ('M', '1') then 1
            when sexo_id_original in ('F', '2') then 2
            else 0
        end as sexo_id,

        safe_cast(nullif(raca_cor_id_original, '') as int64) as raca_cor_id,
        safe_cast(nullif(nivel_escolaridade_id_original, '') as int64) as nivel_escolaridade_id

    from extraido
    where cpf is not null
),

deduplicado as (
    select *
    from tratado
    qualify row_number() over (
        partition by data_particao, cpf
        order by loaded_at desc, dt_atualiza desc
    ) = 1
)

select *
from deduplicado
