{{
    config(
        alias="equipe_tipo",
    )
}}

with
    source as (
        select * from {{ source("brutos_cnes_web_staging", "tbTipoEquipe") }}
    )

select
    safe_cast(tp_equipe as string) as id_equipe_tipo,
    safe_cast(DS_EQUIPE as string) as equipe_descricao,
    safe_cast(co_grupo_equipe as string) as id_equipe_grupo,
    safe_cast(_data_carga as date format 'DD/MM/YYY') as data_carga,
    safe_cast(_data_snapshot as date format 'DD/MM/YYY') as data_snapshot,
    safe_cast(mes_particao as string) as mes_particao,
    safe_cast(ano_particao as string) as ano_particao,
    concat(ano_particao, '-', mes_particao, '-', '01') as data_particao
from source