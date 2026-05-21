{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__equipes',
        materialized = "table",
        tags = ["subpav", "cnes_aps"]
    )
}}

with fonte as (
    select
        json,
        _source_file,
        safe_cast(_loaded_at as timestamp) as loaded_at,
        safe_cast(data_particao as date) as data_particao,
        ano_particao,
        mes_particao
    from {{ source("brutos_gdb_cnes_staging", "LFCES037") }}
),

unidades as (
    select
        data_particao,
        unidade_id_original,
        cnes,
        ap,
        ap_formatada,
        nome_fanta
    from {{ ref("int_subpav_cnes_aps__unidades") }}
),

extraido as (
    select
        -- metadados da carga
        data_particao,
        ano_particao,
        mes_particao,
        loaded_at,
        _source_file,

        -- identificação da equipe
        lpad(nullif(json_value(json, '$.CO_EQUIPE'), ''), 10, '0') as ine,
        nullif(json_value(json, '$.UNIDADE_ID'), '') as unidade_id_original,
        nullif(json_value(json, '$.COD_AREA'), '') as cod_area,
        nullif(json_value(json, '$.NM_REFERENCIA'), '') as nm_referencia,

        -- datas
        safe_cast(nullif(json_value(json, '$.DT_ATIVACAO'), '') as date) as dt_ativacao,
        safe_cast(nullif(json_value(json, '$.DT_DESATIVACAO'), '') as date) as dt_desativacao,
        safe_cast(nullif(json_value(json, '$.DATA_ATU'), '') as date) as dt_atualiza,

        -- tipologia / desativação
        safe_cast(nullif(json_value(json, '$.TP_EQUIPE'), '') as int64) as tipo_equipe_id,
        nullif(json_value(json, '$.CO_SUB_TIPO_EQUIPE'), '') as subtipo_equipe_id,
        nullif(json_value(json, '$.CD_MOTIVO_DESATIV'), '') as motivo_desativacao_equipe_id,
        nullif(json_value(json, '$.CD_TP_DESATIV'), '') as tipo_desativacao_id,

        -- população assistida: valores originais do CNES
        nullif(json_value(json, '$.TP_POP_ASSIST_QUILOMB'), '') as pop_assist_quilomb_original,
        nullif(json_value(json, '$.TP_POP_ASSIST_ASSENT'), '') as pop_assist_assent_original,
        nullif(json_value(json, '$.TP_POP_ASSIST_GERAL'), '') as pop_assist_geral_original,
        nullif(json_value(json, '$.TP_POP_ASSIST_ESCOLA'), '') as pop_assist_escola_original,
        nullif(json_value(json, '$.TP_POP_ASSIST_PRONASCI'), '') as pop_assist_pronasci_original,
        nullif(json_value(json, '$.TP_POP_ASSIST_INDIGENA'), '') as pop_assist_indigena_original,
        nullif(json_value(json, '$.TP_POP_ASSIST_RIBEIRINHA'), '') as pop_assist_ribeirinha_original,
        nullif(json_value(json, '$.TP_POP_ASSIST_SITUACAO_RUA'), '') as pop_assist_situacao_rua_original,
        nullif(json_value(json, '$.TP_POP_ASSIST_PRIV_LIBERDADE'), '') as pop_assist_priv_liberdade_original,
        nullif(json_value(json, '$.TP_POP_ASSIST_CONFLITO_LEI'), '') as pop_assist_conflito_lei_original,
        nullif(json_value(json, '$.TP_POP_ASSIST_ADOL_CONF_LEI'), '') as pop_assist_adol_conf_lei_original,

        -- outros campos
        nullif(json_value(json, '$.CO_PROF_SUS_PRECEPTOR'), '') as co_prof_sus_preceptor

    from fonte
),

tratado as (
    select
        e.*,

        -- dados herdados da unidade
        u.cnes,
        u.ap,
        u.ap_formatada,
        u.nome_fanta as nome_unidade,

        case
            when e.dt_desativacao is null then 1
            else 0
        end as equipe_ativa,

        -- população assistida: no legado, 1 = sim e 2 = não
        case
            when pop_assist_quilomb_original = '2' then 0
            when pop_assist_quilomb_original is null then null
            else 1
        end as pop_assist_quilomb,

        case
            when pop_assist_assent_original = '2' then 0
            when pop_assist_assent_original is null then null
            else 1
        end as pop_assist_assent,

        case
            when pop_assist_geral_original = '2' then 0
            when pop_assist_geral_original is null then null
            else 1
        end as pop_assist_geral,

        case
            when pop_assist_escola_original = '2' then 0
            when pop_assist_escola_original is null then null
            else 1
        end as pop_assist_escola,

        case
            when pop_assist_pronasci_original = '2' then 0
            when pop_assist_pronasci_original is null then null
            else 1
        end as pop_assist_pronasci,

        case
            when pop_assist_indigena_original = '2' then 0
            when pop_assist_indigena_original is null then null
            else 1
        end as pop_assist_indigena,

        case
            when pop_assist_ribeirinha_original = '2' then 0
            when pop_assist_ribeirinha_original is null then null
            else 1
        end as pop_assist_ribeirinha,

        case
            when pop_assist_situacao_rua_original = '2' then 0
            when pop_assist_situacao_rua_original is null then null
            else 1
        end as pop_assist_situacao_rua,

        case
            when pop_assist_priv_liberdade_original = '2' then 0
            when pop_assist_priv_liberdade_original is null then null
            else 1
        end as pop_assist_priv_liberdade,

        case
            when pop_assist_conflito_lei_original = '2' then 0
            when pop_assist_conflito_lei_original is null then null
            else 1
        end as pop_assist_conflito_lei,

        case
            when pop_assist_adol_conf_lei_original = '2' then 0
            when pop_assist_adol_conf_lei_original is null then null
            else 1
        end as pop_assist_adol_conf_lei,
        
        -- Regra de período
        case
            when e.data_particao < date '2020-05-01'
                then '2016_01_A_2020_04'
            when e.data_particao < date '2021-06-01'
                then '2020_05_A_2021_05'
            else '2021_06_EM_DIANTE'
        end as periodo_regra_calculo,

        -- classificação tipo de equipe temporal
        case
            when e.data_particao < date '2020-05-01'
                and tipo_equipe_id in (1, 2, 3, 33, 34, 35, 36, 37, 38)
                then 'ESF'

            when e.data_particao >= date '2020-05-01'
                and tipo_equipe_id = 70
                then 'ESF'

            when e.data_particao < date '2020-05-01'
                and tipo_equipe_id in (4, 10, 11)
                then 'EACS'

            when e.data_particao >= date '2020-05-01'
                and tipo_equipe_id in (4, 10, 11)
                then 'EACS'

            when e.data_particao < date '2020-05-01'
                and tipo_equipe_id in (43, 44)
                then 'ESB'

            when e.data_particao >= date '2020-05-01'
                and tipo_equipe_id in (43, 44, 71)
                then 'ESB'

            when e.data_particao < date '2020-05-01'
                and tipo_equipe_id in (40, 41, 42)
                then 'ECNAR'

            when e.data_particao >= date '2020-05-01'
                and tipo_equipe_id in (40, 41, 42, 73)
                then 'ECNAR'

            when e.data_particao < date '2020-05-01'
                and tipo_equipe_id in (6, 7, 45)
                then 'NASF/EMULTI'

            when e.data_particao >= date '2020-05-01'
                and tipo_equipe_id in (6, 7, 45, 72)
                then 'NASF/EMULTI'

            when e.data_particao < date '2020-05-01'
                and tipo_equipe_id in (16, 17, 18, 19, 20, 21, 49)
                then 'EAP'

            when e.data_particao >= date '2020-05-01'
                and tipo_equipe_id in (16, 17, 18, 19, 20, 21, 49, 76)
                then 'EAP'
            when e.data_particao >= date '2020-05-01'
                and tipo_equipe_id in (1, 2, 3, 33, 34, 35, 36, 37, 38)
                then 'ESF_TIPO_ANTIGO'

            else 'OUTROS'
        end as classificacao_equipe,

        -- Regras de contagem do sistema legado
        case
            when e.data_particao < date '2020-05-01'
                and tipo_equipe_id in (1, 2, 3, 33, 34, 35, 36, 37, 38)
                then 1
            when e.data_particao >= date '2020-05-01'
                and tipo_equipe_id = 70
                then 1
            else 0
        end as is_esf,

        case
            when e.data_particao < date '2020-05-01'
                and tipo_equipe_id in (2, 3, 10, 11, 19, 20, 21, 34, 35, 37, 38, 43, 44)
                then 1
            when e.data_particao >= date '2020-05-01'
                and tipo_equipe_id in (43, 44, 71)
                then 1
            else 0
        end as is_esb,

        case
            when e.data_particao < date '2020-05-01'
                and tipo_equipe_id in (
                    1, 2, 3, 4, 10, 11, 16, 17, 18, 19, 20, 21,
                    33, 34, 35, 36, 37, 38, 40, 41, 42, 49
                )
                then 1
            when e.data_particao >= date '2020-05-01'
                and tipo_equipe_id in (16, 17, 18, 19, 20, 21, 49, 70, 73, 76)
                then 1
            else 0
        end as is_aps

    from extraido e
    left join unidades u
        on e.data_particao = u.data_particao
        and e.unidade_id_original = u.unidade_id_original
),

deduplicado as (
    select *
    from tratado
    qualify row_number() over (
        partition by data_particao, ine
        order by loaded_at desc, dt_atualiza desc
    ) = 1
)

select *
from deduplicado
