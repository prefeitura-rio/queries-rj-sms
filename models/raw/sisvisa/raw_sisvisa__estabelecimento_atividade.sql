{{
    config(
        schema="brutos_sisvisa",
        alias="estabelecimento_atividade",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sisvisa_staging", "AtividadesDoEstabelecimento") }}
    ),

    dedup as (
        select *
        from source
        qualify
            row_number() over (
                partition by EstabelecimentoId, Codigo
                order by _airbyte_extracted_at desc
            ) = 1
    ),

    renamed as (
        select

            -- 1. IDENTIFICAÇÃO DA ATIVIDADE
            {{ process_null('t.Codigo') }}                   as codigo,
            {{ process_null('t.Descricao') }}                as descricao,
            t.EstabelecimentoId                              as estabelecimento_id,

            -- 2. INDICADORES PRINCIPAIS
            {{ process_null('t.afe') }}                      as afe,
            t.Regulada                                       as regulada,
            t.Licenciada                                     as licenciada,

            -- 3. PROCEDIMENTOS / SERVIÇOS DE SAÚDE
            t.TemInvasivo                                    as tem_invasivo,
            t.TemProcedimentoInvasivo                        as tem_procedimento_invasivo,
            t.TemInternacao                                  as tem_internacao,

            -- 4. COMPLEMENTOS
            {{ process_null('t.Complemento') }}              as complemento,
            t.NecessitaComplemento                           as necessita_complemento

        from dedup t
    )

select *
from renamed