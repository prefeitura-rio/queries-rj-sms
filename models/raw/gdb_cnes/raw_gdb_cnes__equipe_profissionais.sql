{{
    config(
        alias="equipe_profissionais",
        schema= "brutos_gdb_cnes"
    )
}}

with source as (
    select *
    from {{ source("brutos_gdb_cnes_staging", "LFCES038") }}
),
extracted as (
    select
        json_extract_scalar(json, "$.PROF_ID") as PROF_ID,
        json_extract_scalar(json, "$.COD_CBO") as COD_CBO,
        json_extract_scalar(json, "$.SEQ_EQUIPE") as SEQ_EQUIPE,
        json_extract_scalar(json, "$.UNIDADE_ID") as UNIDADE_ID,
        json_extract_scalar(json, "$.COD_MUN") as COD_MUN,
        json_extract_scalar(json, "$.COD_AREA") as COD_AREA,
        json_extract_scalar(json, "$.TP_SUS_NAO_SUS") as TP_SUS_NAO_SUS,
        json_extract_scalar(json, "$.IND_VINC") as IND_VINC,
        json_extract_scalar(json, "$.MICROAREA") as MICROAREA,
        json_extract_scalar(json, "$.DT_ENTRADA") as DT_ENTRADA,
        json_extract_scalar(json, "$.DT_DESLIGAMENTO") as DT_DESLIGAMENTO,
        json_extract_scalar(json, "$.CNES_OUTRAEQUIPE") as CNES_OUTRAEQUIPE,
        json_extract_scalar(json, "$.COD_MUN_OUTRAEQUIPE") as COD_MUN_OUTRAEQUIPE,
        json_extract_scalar(json, "$.COD_AREA_OUTRAEQUIPE") as COD_AREA_OUTRAEQUIPE,
        json_extract_scalar(json, "$.PROF_ID_CH_COMPL") as PROF_ID_CH_COMPL,
        json_extract_scalar(json, "$.COD_CBO_CH_COMPL") as COD_CBO_CH_COMPL,
        json_extract_scalar(json, "$.FL_EQUIPEMINIMA") as FL_EQUIPEMINIMA,
        json_extract_scalar(json, "$.CO_MUN_ATUACAO") as CO_MUN_ATUACAO,
        json_extract_scalar(json, "$.DATA_ATU") as DATA_ATU,
        json_extract_scalar(json, "$.USUARIO") as USUARIO,
        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
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

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)

select *
from renamed
