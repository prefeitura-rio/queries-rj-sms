{{
    config(
        alias="equipe_profissionais",
    )
}}

with
    source as (
        select * from {{ source("brutos_cnes_web_staging", "rlEstabEquipeProf") }}
    )

select
    safe_cast(co_profissional_sus as string) as id_profissional_sus,
    safe_cast(co_cbo as string) as id_cbo,
    safe_cast(seq_equipe as string) as equipe_sequencial,
    safe_cast(co_unidade as string) as id_unidade,
    safe_cast(co_municipio as string) as id_municipio,
    safe_cast({{process_null('co_microarea')}} as string) as id_microarea,
    safe_cast(co_area as string) as id_area,
    safe_cast(tp_sus_nao_sus as string) as tipo_sus_nao_sus,
    safe_cast(ind_vinculacao as string) as id_vinculo_profissional,
    safe_cast({{process_null('dt_entrada')}} as date format 'DD/MM/YYYY') as data_entrada_profissional,
    safe_cast({{process_null('dt_desligamento')}} as date format 'DD/MM/YYYY') as data_desligamento_profissional,
    safe_cast(co_cnes_outraequipe as string) as id_cnes_outra_equipe,
    safe_cast(co_municipio_outraequipe as string) as id_municipio_outra_equipe,
    safe_cast(co_area_outraequipe as string) as id_area_outra_equipe,
    safe_cast(co_profissional_sus_compl as string) as id_profissional_complementar,
    safe_cast(co_cbo_ch_compl as string) as cbo_profissional_complementar,
    safe_cast(st_equipeminima as string) as pertence_equipe_minima,
    safe_cast(co_mun_atuacao as string) as id_municipio_atuacao,
    safe_cast(dt_atualizacao as string) as data_atualizacao,
    safe_cast(dt_atualizacao_origem as string) as data_atualizacao_origem,
    safe_cast(no_usuario as string) as nome_usuario,
    safe_cast(_data_carga as date format 'DD/MM/YYY') as data_carga,
    safe_cast(_data_snapshot as date format 'DD/MM/YYY') as data_snapshot,
    concat(ano_particao, '-', mes_particao, '-', '01') as data_particao,
    safe_cast(ano_particao as string) as ano_particao,
from source