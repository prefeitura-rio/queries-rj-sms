{{
    config(
        schema="intermediario_gdb_cnes",
        alias="equipe_profissionais",
        materialized="table",
        tags=["gdb_cnes"]
    )
}}


with
    profissional as (
        select
            substr(upper(to_hex(md5(cast(PROF_ID as string)))), 0, 16) as id_profissional_sus,

            -- PROF_ID: FK LFCES021
            cast({{ process_null("PROF_ID") }} as string) as id_profissional_cnes,
            -- COD_CBO: FK LFCES021
            cast({{ process_null("COD_CBO") }} as string) as id_cbo,
            -- SEQ_EQUIPE: FK LFCES037
            cast({{ process_null("SEQ_EQUIPE") }} as string) as equipe_sequencial,
            -- UNIDADE_ID: LFCES021
            cast({{ process_null("UNIDADE_ID") }} as string) as id_unidade,
            -- COD_MUN: FK LFCES037
            cast({{ process_null("COD_MUN") }} as string) as id_municipio,
            -- COD_AREA: FK LFCES037
            cast({{ process_null("COD_AREA") }} as string) as id_area,
            -- TP_SUS_NAO_SUS: FK LFCES021
            cast({{ process_null("TP_SUS_NAO_SUS") }} as string) as tipo_sus_nao_sus,
            -- IND_VINC: FK LFCES021
            cast({{ process_null("IND_VINC") }} as string) as id_vinculo_profissional,

            cast({{ process_null("MICROAREA") }} as string) as id_microarea,
            safe_cast({{ process_null("DT_ENTRADA") }} as date) as data_entrada_profissional,
            safe_cast({{ process_null("DT_DESLIGAMENTO") }} as date) as data_desligamento_profissional,
            cast({{ process_null("CNES_OUTRAEQUIPE") }} as string) as id_cnes_outra_equipe,
            cast({{ process_null("COD_MUN_OUTRAEQUIPE") }} as string) as id_municipio_outra_equipe,
            cast({{ process_null("COD_AREA_OUTRAEQUIPE") }} as string) as id_area_outra_equipe,
            cast({{ process_null("PROF_ID_CH_COMPL") }} as string) as id_profissional_complementar,
            cast({{ process_null("COD_CBO_CH_COMPL") }} as string) as cbo_profissional_complementar,
            cast({{ process_null("FL_EQUIPEMINIMA") }} as string) as pertence_equipe_minima,
            cast({{ process_null("CO_MUN_ATUACAO") }} as string) as id_municipio_atuacao,
            cast({{ process_null("DATA_ATU") }} as string) as data_atualizacao,
            cast({{ process_null("USUARIO") }} as string) as nome_usuario,

            data_particao as competencia,
            _loaded_at

        from {{ ref("raw_gdb_cnes__lfces038") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__lfces038") }}
        )
        qualify row_number() over (
            partition by id_profissional_sus, id_unidade, id_cbo, equipe_sequencial
            order by _loaded_at desc
        ) = 1
    )

select *
from profissional
