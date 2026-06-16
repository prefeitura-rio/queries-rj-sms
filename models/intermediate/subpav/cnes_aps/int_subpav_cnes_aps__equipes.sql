{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__equipes',
        materialized = "table",
        partition_by = {
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by = ["ine", "cnes", "ap"],
        tags = ["subpav", "cnes_aps"]
    )
}}

with fonte as (
    select
        _source_file,
        safe_cast(_loaded_at as timestamp) as loaded_at,
        safe_cast(data_particao as date) as data_particao,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,

        COD_MUN,
        COD_AREA,
        SEQ_EQUIPE,
        UNIDADE_ID,
        TP_EQUIPE,
        CO_SUB_TIPO_EQUIPE,
        NM_REFERENCIA,
        DT_ATIVACAO,
        DT_DESATIVACAO,
        TP_POP_ASSIST_QUILOMB,
        TP_POP_ASSIST_ASSENT,
        TP_POP_ASSIST_GERAL,
        TP_POP_ASSIST_ESCOLA,
        TP_POP_ASSIST_PRONASCI,
        TP_POP_ASSIST_INDIGENA,
        TP_POP_ASSIST_RIBEIRINHA,
        TP_POP_ASSIST_SITUACAO_RUA,
        TP_POP_ASSIST_PRIV_LIBERDADE,
        TP_POP_ASSIST_CONFLITO_LEI,
        TP_POP_ASSIST_ADOL_CONF_LEI,
        CO_CNES_UOM,
        NU_CH_AMB_UOM,
        CD_MOTIVO_DESATIV,
        CD_TP_DESATIV,
        CO_PROF_SUS_PRECEPTOR,
        CO_CNES_PRECEPTOR,
        CO_EQUIPE,
        DATA_ATU,
        USUARIO,
        STATUSMOV,
        DT_ATUALIZACAO_ORIGEM,
        DT_CMTP_INICIO,
        DT_CMTP_FIM,
        NU_SEQ_PROCESSO

    from {{ ref("raw_gdb_cnes__lfces037") }}
),

unidades as (
    select
        data_particao,
        unidade_id_original,
        cnes,
        ap,
        ap_formatada,
        nome_fanta as nome_unidade,
        tipo_unidade_sms,
        is_unidade_aps_panorama,
        is_municipio_rio,
        esfera_administrativa_id
    from {{ ref("int_subpav_cnes_aps__unidades") }}
),

tipo_equipe as (
    select
        data_particao,
        safe_cast(nullif(TP_EQUIPE, '') as int64) as tipo_equipe_id,
        nullif(DS_EQUIPE, '') as tipo_equipe_descricao,
        nullif(CO_GRUPO_EQUIPE, '') as grupo_equipe_id
    from {{ ref("raw_gdb_cnes__nfces046") }}
    qualify row_number() over (
        partition by
            data_particao,
            safe_cast(nullif(TP_EQUIPE, '') as int64)
        order by _loaded_at desc
    ) = 1
),

motivo_desativacao as (
    select
        data_particao,
        lpad(nullif(CD_MOTIVO_DESATIV, ''), 2, '0') as motivo_desativacao_equipe_id,
        nullif(DS_MOTIVO_DESATIV, '') as motivo_desativacao_equipe
    from {{ ref("raw_gdb_cnes__nfces053") }}
    qualify row_number() over (
        partition by
            data_particao,
            lpad(nullif(CD_MOTIVO_DESATIV, ''), 2, '0')
        order by _loaded_at desc
    ) = 1
),

extraido as (
    select
        
        lpad(nullif(regexp_replace(CO_EQUIPE, r'[^0-9]', ''), ''), 10, '0') as ine,
        nullif(UNIDADE_ID, '') as unidade_id_original,
        nullif(COD_MUN, '') as cod_mun,
        nullif(COD_AREA, '') as cod_area,
        nullif(SEQ_EQUIPE, '') as seq_equipe,
        nullif(NM_REFERENCIA, '') as nm_referencia,

        
        safe_cast(nullif(TP_EQUIPE, '') as int64) as tipo_equipe_id,
        nullif(CO_SUB_TIPO_EQUIPE, '') as subtipo_equipe_id,
        lpad(nullif(CD_MOTIVO_DESATIV, ''), 2, '0') as motivo_desativacao_equipe_id,
        lpad(nullif(CD_TP_DESATIV, ''), 2, '0') as tipo_desativacao_id,

        
        safe_cast(nullif(DT_ATIVACAO, '') as date) as dt_ativacao,
        safe_cast(nullif(DT_DESATIVACAO, '') as date) as dt_desativacao,
        safe_cast(nullif(DATA_ATU, '') as date) as dt_atualiza,
        safe_cast(nullif(DT_ATUALIZACAO_ORIGEM, '') as date) as dt_atualizacao_origem,
        safe_cast(nullif(DT_CMTP_INICIO, '') as date) as dt_cmtp_inicio,
        safe_cast(nullif(DT_CMTP_FIM, '') as date) as dt_cmtp_fim,

        
        nullif(TP_POP_ASSIST_QUILOMB, '') as pop_assist_quilomb_original,
        nullif(TP_POP_ASSIST_ASSENT, '') as pop_assist_assent_original,
        nullif(TP_POP_ASSIST_GERAL, '') as pop_assist_geral_original,
        nullif(TP_POP_ASSIST_ESCOLA, '') as pop_assist_escola_original,
        nullif(TP_POP_ASSIST_PRONASCI, '') as pop_assist_pronasci_original,
        nullif(TP_POP_ASSIST_INDIGENA, '') as pop_assist_indigena_original,
        nullif(TP_POP_ASSIST_RIBEIRINHA, '') as pop_assist_ribeirinha_original,
        nullif(TP_POP_ASSIST_SITUACAO_RUA, '') as pop_assist_situacao_rua_original,
        nullif(TP_POP_ASSIST_PRIV_LIBERDADE, '') as pop_assist_priv_liberdade_original,
        nullif(TP_POP_ASSIST_CONFLITO_LEI, '') as pop_assist_conflito_lei_original,
        nullif(TP_POP_ASSIST_ADOL_CONF_LEI, '') as pop_assist_adol_conf_lei_original,

        
        nullif(CO_CNES_UOM, '') as cnes_unidade_odontologica_movel,
        safe_cast(nullif(NU_CH_AMB_UOM, '') as int64) as carga_horaria_ambulatorial_uom,
        nullif(CO_PROF_SUS_PRECEPTOR, '') as co_prof_sus_preceptor,
        nullif(CO_CNES_PRECEPTOR, '') as cnes_preceptor,
        nullif(USUARIO, '') as usuario_atualizacao,
        nullif(STATUSMOV, '') as status_movimento,
        nullif(NU_SEQ_PROCESSO, '') as nu_seq_processo,

        
        data_particao,
        ano_particao,
        mes_particao,
        format_date('%Y-%m', data_particao) as competencia_mes,
        loaded_at,
        _source_file

    from fonte
),

com_dimensoes as (
    select
        e.*,

        u.cnes,
        u.ap,
        u.ap_formatada,
        u.nome_unidade,
        u.tipo_unidade_sms,
        u.esfera_administrativa_id,
        u.is_unidade_aps_panorama,
        coalesce(u.is_municipio_rio, 0) as is_municipio_rio,
        te.tipo_equipe_descricao,
        te.grupo_equipe_id,
        md.motivo_desativacao_equipe

    from extraido e
    left join unidades u
        on e.data_particao = u.data_particao
        and e.unidade_id_original = u.unidade_id_original
    left join tipo_equipe te
        on e.data_particao = te.data_particao
        and e.tipo_equipe_id = te.tipo_equipe_id
    left join motivo_desativacao md
        on e.data_particao = md.data_particao
        and e.motivo_desativacao_equipe_id = md.motivo_desativacao_equipe_id
),

tratado as (
    select
        e.*,

        case
            when e.dt_desativacao is null then 1
            else 0
        end as equipe_ativa,

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

        case
            when data_particao < date '2020-05-01' then 'ATE_2020_04'
            when data_particao < date '2021-06-01' then '2020_05_A_2021_05'
            else '2021_06_EM_DIANTE'
        end as periodo_regra_calculo

    from com_dimensoes e
),

classificado as (
    select
        t.*,

        case
            when tipo_equipe_id in (
                1, 2, 3, 12, 13, 14, 15, 24, 25, 26, 27, 28, 29, 30, 31, 32,
                33, 34, 35, 36, 37, 38, 70
            ) then 'ESF'
            when tipo_equipe_id in (4, 10, 11) then 'EACS'
            when tipo_equipe_id in (43, 44, 71) then 'ESB'
            when tipo_equipe_id in (40, 41, 42, 73) then 'ECR'
            when tipo_equipe_id in (16, 17, 18, 19, 20, 21, 49, 76) then 'EAP'
            when tipo_equipe_id in (6, 7, 45, 72) then 'ENASF'
            when tipo_equipe_id in (5, 50, 51, 52, 53, 54, 74) then 'EAPP'
            when tipo_equipe_id in (22, 46) then 'EMAD'
            when tipo_equipe_id = 23 then 'EMAP'
            else 'OUTROS'
        end as classificacao_equipe,

        case
            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (1, 2, 3, 33, 34, 35, 36, 37, 38)
                then 'ESF'
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id = 70
                then 'ESF'

            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (4, 10, 11)
                then 'EACS'
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id in (4, 10, 11)
                then 'EACS'

            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (2, 3, 10, 11, 19, 20, 21, 34, 35, 37, 38, 43, 44)
                then 'ESB'
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id in (43, 44, 71)
                then 'ESB'

            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (40, 41, 42)
                then 'ECR'
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id in (40, 41, 42, 73)
                then 'ECR'

            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (6, 7, 45)
                then 'ENASF'
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id in (6, 7, 45, 72)
                then 'ENASF'

            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (16, 17, 18, 19, 20, 21, 49)
                then 'EAP'
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id in (16, 17, 18, 19, 20, 21, 49, 76)
                then 'EAP'

            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (5, 50, 51, 52, 53, 54)
                then 'EAPP'
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id in (5, 50, 51, 52, 53, 54, 74)
                then 'EAPP'

            when tipo_equipe_id in (22, 46) then 'EMAD'
            when tipo_equipe_id = 23 then 'EMAP'

            else 'OUTROS'
        end as classificacao_equipe_temporal,

        case
            when tipo_equipe_id = 70 then 'ESF'
            when tipo_equipe_id = 76 then 'EAP'
            when tipo_equipe_id = 73 then 'ECR'
            when tipo_equipe_id = 71 then 'ESB'
            when tipo_equipe_id = 72 then 'ENASF'
            when tipo_equipe_id = 74 then 'EAPP'
            when tipo_equipe_id in (22, 46) then 'EMAD'
            when tipo_equipe_id = 23 then 'EMAP'
            else 'OUTROS'
        end as classificacao_equipe_painel,

        case
            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (1, 2, 3, 33, 34, 35, 36, 37, 38)
                then 1
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id = 70
                then 1
            else 0
        end as is_esf,

        case
            when tipo_equipe_id in (4, 10, 11) then 1
            else 0
        end as is_eacs,

        case
            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (2, 3, 10, 11, 19, 20, 21, 34, 35, 37, 38, 43, 44)
                then 1
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id in (43, 44, 71)
                then 1
            else 0
        end as is_esb,

        case
            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (40, 41, 42)
                then 1
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id in (40, 41, 42, 73)
                then 1
            else 0
        end as is_ecr,

        case
            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (6, 7, 45)
                then 1
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id in (6, 7, 45, 72)
                then 1
            else 0
        end as is_enasf,

        case
            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (16, 17, 18, 19, 20, 21, 49)
                then 1
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id in (16, 17, 18, 19, 20, 21, 49, 76)
                then 1
            else 0
        end as is_eap,

        case
            when data_particao < date '2020-05-01'
                and tipo_equipe_id in (5, 50, 51, 52, 53, 54)
                then 1
            when data_particao >= date '2020-05-01'
                and tipo_equipe_id in (5, 50, 51, 52, 53, 54, 74)
                then 1
            else 0
        end as is_eapp,

        case
            when tipo_equipe_id in (5, 50, 51, 52, 53, 54) then 1
            when tipo_equipe_id = 74
                and not regexp_contains(upper(coalesce(nm_referencia, '')), r'PSIC')
                then 1
            else 0
        end as is_eapp_panorama,

        case
            when tipo_equipe_id in (22, 46) then 1
            else 0
        end as is_emad,

        case
            when tipo_equipe_id in (22, 46)
                and (
                    esfera_administrativa_id = 3
                    or regexp_contains(upper(coalesce(nome_unidade, '')), r'^SMS PADI')
                )
                then 1
            else 0
        end as is_emad_panorama,

        case
            when tipo_equipe_id = 23 then 1
            else 0
        end as is_emap,

        case
            when tipo_equipe_id in (70, 76, 73) then 1
            else 0
        end as is_equipe_aps_painel,

        case
            when tipo_equipe_id in (70, 76, 73) then 1
            else 0
        end as is_equipe_aps_cobertura,

        case
            when tipo_equipe_id in (
                1, 2, 3, 12, 13, 14, 15, 24, 25, 26, 27, 28, 29, 30, 31, 32,
                33, 34, 35, 36, 37, 38, 70,
                4, 10, 11,
                43, 44, 71,
                40, 41, 42, 73,
                16, 17, 18, 19, 20, 21, 49, 76
            ) then 1
            else 0
        end as is_aps

    from tratado t
),

com_flags_historicas as (
    select
        *,

        case
            when data_particao < date '2020-05-01'
                and (
                    is_esf = 1
                    or is_eacs = 1
                    or is_ecr = 1
                )
                then 1

            when data_particao >= date '2020-05-01'
                and (
                    is_esf = 1
                    or is_eap = 1
                    or is_ecr = 1
                )
                then 1

            else 0
        end as is_equipe_aps_historico,

        case
            when data_particao < date '2020-05-01'
                and (
                    is_esf = 1
                    or is_eacs = 1
                    or is_ecr = 1
                )
                then 1

            when data_particao >= date '2020-05-01'
                and is_esf = 1
                then 1

            else 0
        end as is_esf_panorama_historico,

        case
            when data_particao < date '2020-05-01'
                and is_eacs = 1
                then 1
            else 0
        end as is_eacs_panorama_historico

    from classificado
),

deduplicado as (
    select *
    from com_flags_historicas
    qualify row_number() over (
        partition by data_particao, ine
        order by loaded_at desc, dt_atualiza desc
    ) = 1
)

select
    ine,
    cnes,
    unidade_id_original,
    nome_unidade,
    ap,
    ap_formatada,
    is_municipio_rio,

    cod_mun,
    cod_area,
    seq_equipe,
    nm_referencia,

    tipo_equipe_id,
    tipo_equipe_descricao,
    grupo_equipe_id,
    subtipo_equipe_id,
    classificacao_equipe,
    classificacao_equipe_temporal,
    classificacao_equipe_painel,

    equipe_ativa,
    is_esf,
    is_esf_panorama_historico,
    is_eacs,
    is_eacs_panorama_historico,
    is_esb,
    is_ecr,
    is_enasf,
    is_eap,
    is_eapp,
    is_eapp_panorama,
    is_emad,
    is_emad_panorama,
    is_emap,
    is_equipe_aps_painel,
    is_equipe_aps_cobertura,
    is_equipe_aps_historico,
    is_aps,

    tipo_unidade_sms,
    is_unidade_aps_panorama,

    motivo_desativacao_equipe_id,
    motivo_desativacao_equipe,
    tipo_desativacao_id,
    dt_ativacao,
    dt_desativacao,
    dt_atualiza,

    pop_assist_quilomb_original,
    pop_assist_assent_original,
    pop_assist_geral_original,
    pop_assist_escola_original,
    pop_assist_pronasci_original,
    pop_assist_indigena_original,
    pop_assist_ribeirinha_original,
    pop_assist_situacao_rua_original,
    pop_assist_priv_liberdade_original,
    pop_assist_conflito_lei_original,
    pop_assist_adol_conf_lei_original,

    pop_assist_quilomb,
    pop_assist_assent,
    pop_assist_geral,
    pop_assist_escola,
    pop_assist_pronasci,
    pop_assist_indigena,
    pop_assist_ribeirinha,
    pop_assist_situacao_rua,
    pop_assist_priv_liberdade,
    pop_assist_conflito_lei,
    pop_assist_adol_conf_lei,

    cnes_unidade_odontologica_movel,
    carga_horaria_ambulatorial_uom,
    co_prof_sus_preceptor,
    cnes_preceptor,
    usuario_atualizacao,
    status_movimento,
    nu_seq_processo,
    periodo_regra_calculo,
    dt_atualizacao_origem,
    dt_cmtp_inicio,
    dt_cmtp_fim,

    competencia_mes,
    data_particao,
    ano_particao,
    mes_particao,
    loaded_at,
    _source_file

from deduplicado
