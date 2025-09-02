{{
    config(
        schema="brutos_sheets",
        alias="aps_farmacias",
        tags=["daily", "subgeral", "cnes_subgeral", "monitora_reg"],
    )
}}
-- TODO: conferir tags acima

with
    source as (select * from {{ source("brutos_sheets_staging", "aps_farmacias") }}),
    renamed as (
        select
            ap as area_programatica,
            cnes as id_cnes,
            razao_social as nome,
            endereco,
            horario_da_farmacia_seg_a_sexta as horario_seg_a_sexta,
            horario_da_farmacia_sab as horario_sabado,
            tem_farmacia as farmacia_indicador,
            {{ proper_br("nome_farmaceutico_rt") }} as farmaceutico_nome,
            crf as farmaceutico_crf,
            lower(vinculo) as farmaceutico_vinculo,
            obs,

        from source
    )
select *
from renamed
