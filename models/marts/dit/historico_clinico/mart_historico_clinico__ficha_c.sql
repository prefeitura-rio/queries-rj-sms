{{
    config(
        schema="saude_historico_clinico",
        alias="ficha_c",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    ficha_c as (
        select
            cpf,
            vacinas_em_dia,
            primeira_cons_7_dias,
            estado_nutricional,
            tipo_de_aleitamento,
            atraso_desenvolvimento,
            sinais_risco,
            diarreia,
            infeccao_respirat_aguda,
            estatura_ao_nascer,
            per_cefalico,
            data_acs_visita,
            safe_cast(cpf as int64) as cpf_particao
        from {{ ref("raw_informes_vitacare__ficha_c_v2") }}
    ),

    -- TODO Caso o paciente tenha tido mais de 1 visita, manter todas ou apenas a mais recente?
    ficha_c_deducplicado as (
        select
            *
        from ficha_c
        where cpf is not null
        qualify row_number() over(partition by cpf, data_acs_visita order by data_acs_visita desc) = 1
    )

select *
from ficha_c_deducplicado
