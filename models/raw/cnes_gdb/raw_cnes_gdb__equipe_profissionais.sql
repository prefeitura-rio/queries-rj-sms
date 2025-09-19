{{
    config(
        alias="equipe_profissionais",
        schema= "brutos_cnes_gdb"
    )
}}

with
    source as (
        select * from {{ source("brutos_cnes_gdb_staging", "equipe_vinculo") }}
    )

select
    cast(PROF_ID as string) as id_profissional_cnes,
    cast(COD_CBO as string) as id_cbo,
    cast(SEQ_EQUIPE as string) as equipe_sequencial,
    cast(UNIDADE_ID as string) as id_unidade,
    cast(COD_MUN as string) as id_municipio,
    cast(COD_AREA as string) as id_area,
    cast({{process_null('MICROAREA')}} as string) as id_microarea,
    cast(TP_SUS_NAO_SUS as string) as tipo_sus_nao_sus,
    cast(IND_VINC as string) as id_vinculo_profissional,
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
    cast(USUARIO as string) as nome_usuario
from source