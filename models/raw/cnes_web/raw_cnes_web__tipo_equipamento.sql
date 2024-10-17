  {{  config(alias="tipo_equipamento")  }}
  
with
source as (select * from {{ source("brutos_cnes_web_staging", "tbTipoEquipamento") }})

select
    safe_cast(CO_TIPO_EQUIPAMENTO as int64) as equipamento_tipo,
    upper(DS_TIPO_EQUIPAMENTO) as equipamento,
    safe_cast(_data_carga as date format 'DD/MM/YYY') as data_carga,
    safe_cast(_data_snapshot as date format 'DD/MM/YYY') as data_snapshot,
    safe_cast(mes_particao as string) as mes_particao,
    safe_cast(ano_particao as string) as ano_particao,
    concat(ano_particao, '-', mes_particao, '-', '01') as data_particao

from source