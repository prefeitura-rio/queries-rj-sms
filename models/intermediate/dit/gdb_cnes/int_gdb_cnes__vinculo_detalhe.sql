{{
    config(
        schema="intermediario_gdb_cnes",
        alias="vinculo_detalhe",
        materialized="table",
        tags=["gdb_cnes"]
    )
}}

with
    vinculo as (
        select distinct
            cast({{ process_null("IND_VINC") }} as string) as id_vinculo,
            -- CD_VINCULACAO: FK NFCES057
            cast({{ process_null("CD_VINCULACAO") }} as string) as id_vinculacao,
            cast({{ process_null("TP_VINCULO") }} as string) as id_tipo_vinculo,
            cast({{ process_null("TP_SUBVINCULO") }} as string) as id_tipo_subvinculo,
            cast({{ process_null("DS_SUBVINCULO") }} as string) as descricao_subvinculo,
            cast({{ process_null("DS_CONCEITO") }} as string) as descricao_conceito,
            case
                when lower(trim(ST_HABILITADO)) = 's' then true
                when lower(trim(ST_HABILITADO)) = 'n' then false
                else null
            end as habilitado,
            case
                when lower(trim(ST_SOLICITA_CNPJ)) = 's' then true
                when lower(trim(ST_SOLICITA_CNPJ)) = 'n' then false
                else null
            end as solicita_cnpj,
            _loaded_at

        from {{ ref("raw_gdb_cnes__nfces058") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces058") }}
        )
        qualify row_number() over (
            partition by id_vinculo, id_vinculacao, id_tipo_vinculo, id_tipo_subvinculo
            order by _loaded_at desc
        ) = 1
    ),

    vinculo_empregador as (
        select distinct
            cast({{ process_null("CD_VINCULACAO") }} as string) as id_vinculacao,
            cast({{ process_null("TP_VINCULO") }} as string) as id_tipo_vinculo,
            cast({{ process_null("DS_VINCULO") }} as string) as descricao_vinculo,

            _loaded_at

        from {{ ref("raw_gdb_cnes__nfces057") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces057") }}
        )
        qualify row_number() over (
            partition by id_vinculacao, id_tipo_vinculo
            order by _loaded_at desc
        ) = 1
    ),

    vinculo_estabelecimento as (
        select distinct
            cast({{ process_null("CD_VINCULACAO") }} as string) as id_vinculacao,
            cast({{ process_null("DS_VINCULACAO") }} as string) as descricao_vinculacao,

            _loaded_at
        from {{ ref("raw_gdb_cnes__nfces056") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces056") }}
        )
        qualify row_number() over (
            partition by id_vinculacao
            order by _loaded_at desc
        ) = 1
    )

select
    id_vinculo,
    id_vinculacao,
    id_tipo_vinculo,
    id_tipo_subvinculo,
    descricao_vinculo,
    descricao_subvinculo,
    descricao_conceito,
    descricao_vinculacao,
    habilitado,
    solicita_cnpj
from vinculo
left join vinculo_empregador
    using (id_tipo_vinculo, id_vinculacao)
left join vinculo_estabelecimento
    using (id_vinculacao)
