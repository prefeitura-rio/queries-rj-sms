{{
    config(
        schema="brutos_sisvisa",
        alias="grupo_atividade",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sisvisa_staging", "GrupoAtividade") }}
    ),

    dedup as (
        select *
        from source
        qualify
            row_number() over (
                partition by Id
                order by _airbyte_extracted_at desc
            ) = 1
    ),

    renamed as (
        select

            -- 1. IDENTIFICAÇÃO
            t.Id                                        as id,
            {{ process_null('t.Descricao') }}           as descricao,

            -- 2. INDICADORES
            t.QualificaServidor                         as qualifica_servidor,
            t.RelatorioGerencial                        as relatorio_gerencial

        from dedup t
    )

select *
from renamed