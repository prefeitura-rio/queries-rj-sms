{{
    config(
        schema="intermediario_gdb_cnes",
        alias="equipe",
        materialized="table",
        tags=["gdb_cnes"]
    )
}}


with
    base as (
        select
            -- TP_EQUIPE: FK NFCES046
            cast({{ process_null("TP_EQUIPE") }} as string) as id_equipe_tipo,
            -- UNIDADE_ID: FK LFCES004
            cast({{ process_null("UNIDADE_ID") }} as string) as id_unidade,
            -- COD_MUN: FK LFCES041
            cast({{ process_null("COD_MUN") }} as string) as id_municipio,
            -- COD_AREA: FK LFCES041
            cast({{ process_null("COD_AREA") }} as string) as id_area,
            -- CD_MOTIVO_DESATIV: FK NFCES053
            cast({{ process_null("CD_MOTIVO_DESATIV") }} as string) as id_motivacao_desativacao_equipe,
            -- CD_TP_DESATIV: FK NFCES050
            cast({{ process_null("CD_TP_DESATIV") }} as string) as id_tipo_desativacao_equipe,
            case
                when trim(CD_TP_DESATIV)="01" then "Temporária"
                when trim(CD_TP_DESATIV)="02" then "Definitiva"
                else null
            end as tipo_desativacao_equipe,

            cast({{ process_null("CO_EQUIPE") }} as string) as equipe_ine,
            cast({{ process_null("SEQ_EQUIPE") }} as string) as equipe_sequencial,
            cast({{ process_null("NM_REFERENCIA") }} as string) as equipe_nome,
            cast({{ process_null("CO_SUB_TIPO_EQUIPE") }} as string) as id_subtipo_equipe,
            safe_cast({{ process_null("DT_ATIVACAO") }} as date) as data_ativacao,
            safe_cast({{ process_null("DT_DESATIVACAO") }} as date) as data_desativacao,
            cast({{ process_null("TP_POP_ASSIST_QUILOMB") }} as string) as atende_pop_quilombola,
            cast({{ process_null("TP_POP_ASSIST_ASSENT") }} as string) as atende_pop_assentados,
            cast({{ process_null("TP_POP_ASSIST_GERAL") }} as string) as atende_pop_geral,
            cast({{ process_null("TP_POP_ASSIST_ESCOLA") }} as string) as atende_pop_escola,
            cast({{ process_null("TP_POP_ASSIST_PRONASCI") }} as string) as atende_pop_pronasci,
            cast({{ process_null("TP_POP_ASSIST_INDIGENA") }} as string) as atende_pop_indigena,
            cast({{ process_null("TP_POP_ASSIST_RIBEIRINHA") }} as string) as atende_pop_ribeirinha,
            cast({{ process_null("TP_POP_ASSIST_SITUACAO_RUA") }} as string) as atende_pop_situacao_rua,
            cast({{ process_null("TP_POP_ASSIST_PRIV_LIBERDADE") }} as string) as atende_pop_privada_liberdade,
            cast({{ process_null("TP_POP_ASSIST_CONFLITO_LEI") }} as string) as atende_pop_conflito_lei,
            cast({{ process_null("TP_POP_ASSIST_ADOL_CONF_LEI") }} as string) as atende_pop_adolescente_conflito_lei,
            cast({{ process_null("CO_CNES_UOM") }} as string) as id_cnes_uom,
            cast({{ process_null("NU_CH_AMB_UOM") }} as string) as carga_horaria_uom,
            cast({{ process_null("CO_PROF_SUS_PRECEPTOR") }} as string) as id_profissional_preceptor,
            cast({{ process_null("CO_CNES_PRECEPTOR") }} as string) as id_cnes_preceptor,
            safe_cast({{ process_null("DATA_ATU") }} as date) as data_atualizacao,
            cast({{ process_null("USUARIO") }} as string) as usuario,

            data_particao as competencia,
            _loaded_at

        from {{ ref("raw_gdb_cnes__lfces037") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__lfces037") }}
        )
        qualify row_number() over (
            partition by id_unidade, equipe_ine
            order by _loaded_at desc
        ) = 1
    ),
    tipo as (
        select
            cast({{ process_null("TP_EQUIPE") }} as string) as id_equipe_tipo,
            cast({{ process_null("DS_EQUIPE") }} as string) as equipe_descricao,
            cast({{ process_null("CO_GRUPO_EQUIPE") }} as string) as id_equipe_grupo,

            _loaded_at

        from {{ ref("raw_gdb_cnes__nfces046") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces046") }}
        )
        qualify row_number() over (
            partition by id_equipe_tipo
            order by _loaded_at desc
        ) = 1
    ),

    joined as (
        select
            id_equipe_tipo,
            tipo.equipe_descricao,
            tipo.id_equipe_grupo,
            base.* except (id_equipe_tipo)
        from base
        left join tipo
            using (id_equipe_tipo)
    )

select *
from joined
