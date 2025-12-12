{{
    config(
        alias="equipe_profissionais",
        schema= "brutos_gdb_cnes"
    )
}}

with
    source as (
        select *
        from {{ source("brutos_gdb_cnes_staging", "LFCES038") }}
        -- TODO: filtrar por mais recentes?
        --       algo como `where data_particao = (... max(data_particao))
        --       ou então transformar a tabela em incremental?
    ),
    renamed as (
        select
            substr(upper(to_hex(md5(cast(PROF_ID as string)))), 0, 16) as id_profissional_sus,

            -- PROF_ID: FK LFCES021
            cast(PROF_ID as string) as id_profissional_cnes,
            -- COD_CBO: FK LFCES021
            cast(COD_CBO as string) as id_cbo,
            -- SEQ_EQUIPE: FK LFCES037
            cast(SEQ_EQUIPE as string) as equipe_sequencial,
            -- UNIDADE_ID: LFCES021
            cast(UNIDADE_ID as string) as id_unidade,
            -- COD_MUN: FK LFCES037
            cast(COD_MUN as string) as id_municipio,
            -- COD_AREA: FK LFCES037
            cast(COD_AREA as string) as id_area,
            -- TP_SUS_NAO_SUS: FK LFCES021
            cast(TP_SUS_NAO_SUS as string) as tipo_sus_nao_sus,
            -- IND_VINC: FK LFCES021
            cast(IND_VINC as string) as id_vinculo_profissional,

            cast({{process_null('MICROAREA')}} as string) as id_microarea,
            safe_cast(DT_ENTRADA as date) as data_entrada_profissional,
            safe_cast(DT_DESLIGAMENTO as date) as data_desligamento_profissional,
            cast(CNES_OUTRAEQUIPE as string) as id_cnes_outra_equipe,
            cast(COD_MUN_OUTRAEQUIPE as string) as id_municipio_outra_equipe,
            cast(COD_AREA_OUTRAEQUIPE as string) as id_area_outra_equipe,
            cast(PROF_ID_CH_COMPL as string) as id_profissional_complementar,
            cast(COD_CBO_CH_COMPL as string) as cbo_profissional_complementar,
            cast(FL_EQUIPEMINIMA as string) as pertence_equipe_minima,
            cast(CO_MUN_ATUACAO as string) as id_municipio_atuacao,
            cast(DATA_ATU as string) as data_atualizacao,
            cast(USUARIO as string) as nome_usuario,

            -- Podem ser usados posteriormente para deduplicação
            data_particao,
            _loaded_at as data_carga,
        from source
    )

select *
from renamed
