{{ config(
    schema = 'brutos_plataforma_subpav',
    alias = 'visita_gestantes_tipos',
    materialized = 'table'
) }}

with source as (

    select *
    from {{ source('brutos_plataforma_subpav_staging', 'subpav_cegonha__visita_gestantes_tipos') }}

),

base as (

    select
        id_visita_gestante_tipo,
        nme_horario,
        num_cnes_aps,
        num_cnes_atendimento,
        id_agendamento_gestante,
        id_maternidade_tipos_gestante,
        datalake_loaded_at
    from source

),

base_limpa as (

    select
        safe_cast({{ normalize_null("trim(id_visita_gestante_tipo)") }} as int64) as id_visita_gestante_tipo,
        {{ normalize_null("trim(nme_horario)") }} as nme_horario_raw,
        {{ normalize_null("trim(num_cnes_aps)") }} as num_cnes_aps,
        {{ normalize_null("trim(num_cnes_atendimento)") }} as num_cnes_atendimento,
        safe_cast({{ normalize_null("trim(id_agendamento_gestante)") }} as int64) as id_agendamento_gestante,
        safe_cast({{ normalize_null("trim(id_maternidade_tipos_gestante)") }} as int64) as id_maternidade_tipos_gestante,
        safe_cast({{ normalize_null("trim(datalake_loaded_at)") }} as timestamp) as datalake_loaded_at
    from base

),

-- Trata horarios preenchidos fora do padrao
tratado as (

    select
        id_visita_gestante_tipo,
        nme_horario_raw,

        -- Cria horario padronizado
        case

            -- horario ja esta no padrao HH:MM
            when regexp_contains(nme_horario_raw, r'^(?:[01]\d|2[0-3]):[0-5]\d$')
                then nme_horario_raw

            -- corrige horario com dois pontos no inicio, ex: :0909 -> 09:09
            when regexp_contains(nme_horario_raw, r'^:\d{4}$')
                and regexp_contains(
                    substr(nme_horario_raw, 2, 2) || ':' || substr(nme_horario_raw, 4, 2),
                    r'^(?:[01]\d|2[0-3]):[0-5]\d$'
                )
                then substr(nme_horario_raw, 2, 2) || ':' || substr(nme_horario_raw, 4, 2)

            -- completa minuto com zero a direita, ex: 09:0 -> 09:00
            when regexp_contains(nme_horario_raw, r'^\d{2}:\d$')
                and regexp_contains(
                    concat(substr(nme_horario_raw, 1, 4), '0'),
                    r'^(?:[01]\d|2[0-3]):[0-5]\d$'
                )
                then concat(substr(nme_horario_raw, 1, 4), '0')

            -- interpreta valor so com hora, ex: 9 -> 09:00 e 14 -> 14:00
            when regexp_contains(nme_horario_raw, r'^\d{1,2}$')
                and regexp_contains(
                    lpad(nme_horario_raw, 2, '0') || ':00',
                    r'^(?:[01]\d|2[0-3]):[0-5]\d$'
                )
                then lpad(nme_horario_raw, 2, '0') || ':00'

            -- interpreta horario sem separador, ex: 0909 -> 09:09
            when regexp_contains(nme_horario_raw, r'^\d{4}$')
                and regexp_contains(
                    substr(nme_horario_raw, 1, 2) || ':' || substr(nme_horario_raw, 3, 2),
                    r'^(?:[01]\d|2[0-3]):[0-5]\d$'
                )
                then substr(nme_horario_raw, 1, 2) || ':' || substr(nme_horario_raw, 3, 2)

            -- interpreta formato com H, ex: 9H -> 09:00 e 14H -> 14:00
            when regexp_contains(upper(nme_horario_raw), r'^\d{1,2}H$')
                and regexp_contains(
                    lpad(regexp_extract(upper(nme_horario_raw), r'^(\d{1,2})H$'), 2, '0') || ':00',
                    r'^(?:[01]\d|2[0-3]):[0-5]\d$'
                )
                then lpad(regexp_extract(upper(nme_horario_raw), r'^(\d{1,2})H$'), 2, '0') || ':00'

            -- qualquer outro formato nao reconhecido fica nulo
            else null
        end as nme_horario_padronizado,

        num_cnes_aps,
        num_cnes_atendimento,
        id_agendamento_gestante,
        id_maternidade_tipos_gestante,
        datalake_loaded_at

    from base_limpa
),

final as (

    select
        id_visita_gestante_tipo,
        nme_horario_raw,
        nme_horario_padronizado,
        nme_horario_raw is not null
        and nme_horario_padronizado is null as flag_nme_horario_invalido,
        num_cnes_aps,
        num_cnes_atendimento,
        id_agendamento_gestante,
        id_maternidade_tipos_gestante,
        datalake_loaded_at
    from tratado

)

select *
from final