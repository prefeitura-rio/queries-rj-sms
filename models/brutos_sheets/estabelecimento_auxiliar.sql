{{
    config(
        schema="brutos_sheets",
    )
}}


with
    source as (
        select * from {{ source("brutos_sheets_staging", "unidade_saude_auxiliar") }}
    )

select
    -- Primary key
    format("%07d", cast(id_cnes as int64)) as id_cnes,  -- fix cases where 0 on the left is lost

    -- Common fields
    safe_cast(nome_fantasia as string) as nome_fantasia,
    if(nome_limpo = "nan", null, nome_limpo) as nome_limpo,
    if(sigla = "nan", null, sigla) as nome_sigla,
    if(distrito_sanitario = "nan", null, distrito_sanitario) as area_programatica,
    if(prontuario = "nan", null, prontuario) as prontuario,
    if(administracao = "nan", null, administracao) as administracao,

from source
