{{
    config(
        alias="equipe",
    )
}}

with source as (select * from {{ source("brutos_cnes_web_staging", "tbEquipe") }})

select
    safe_cast(co_equipe as string) as id_equipe,
    safe_cast(seq_equipe as string) as equipe_sequencial,
    safe_cast(no_referencia as string) as equipe_nome,
    safe_cast(co_sub_tipo_equipe as string) as id_subtipo_equipe,
    safe_cast(tp_equipe as string) as id_tipo_equipe,
    safe_cast(co_unidade as string) as id_unidade,
    safe_cast(co_municipio as string) as id_municipio,
    safe_cast(co_area as string) as id_area,
    safe_cast(dt_ativacao as date) as data_ativacao,
    safe_cast(dt_desativacao as date) as data_desativacao,
    safe_cast(tp_pop_assist_quilomb as string) as atende_pop_quilombola,
    safe_cast(tp_pop_assist_assent as string) as atende_pop_assentados,
    safe_cast(tp_pop_assist_geral as string) as atende_pop_geral,
    safe_cast(tp_pop_assist_escola as string) as atende_pop_escola,
    safe_cast(tp_pop_assist_pronasci as string) as atende_pop_pronasci,
    safe_cast(tp_pop_assist_indigena as string) as atende_pop_indigena,
    safe_cast(tp_pop_assist_ribeirinha as string) as atende_pop_ribeirinha,
    safe_cast(tp_pop_assist_situacao_rua as string) as atende_pop_situacao_rua,
    safe_cast(tp_pop_assist_priv_liberdade as string) as atende_pop_privada_liberdade,
    safe_cast(tp_pop_assist_conflito_lei as string) as atende_pop_conflito_lei,
    safe_cast(
        tp_pop_assist_adol_conf_lei as string
    ) as atende_pop_adolescente_conflito_lei,
    safe_cast(co_cnes_uom as string) as id_cnes_uom,
    safe_cast(nu_ch_amb_uom as string) as carga_horaria_uom,
    safe_cast(cd_motivo_desativ as string) as id_motivacao_desativacao_equipe,
    safe_cast(cd_tp_desativ as string) as id_tipo_desativacao_equipe,
    safe_cast(co_prof_sus_preceptor as string) as id_profissional_preceptor,
    safe_cast(co_cnes_preceptor as string) as id_cnes_preceptor,
    safe_cast(dt_atualizacao as date format 'DD/MM/YYYY') as data_atualizacao,
    safe_cast(
        dt_atualizacao_origem as date format 'DD/MM/YYYY'
    ) as data_atualizacao_origem,
    safe_cast(no_usuario as string) as usuario,
    safe_cast(_data_carga as date format 'DD/MM/YYYY') as data_carga,
    safe_cast(_data_snapshot as date format 'DD/MM/YYYY') as data_snapshot,
    safe_cast(ano_particao as string) ano_particao,
    concat(ano_particao, '-', mes_particao, '-', '01') as data_particao,
from source
