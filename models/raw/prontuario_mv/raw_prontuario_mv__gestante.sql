{{
    config(
        alias="gestante",
        materialized="incremental",
        schema="brutos_prontuario_mv",
        unique_key="id_hci",
        incremental_strategy="merge",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=["mv"],
    )
}}


with

    source as (
        select *
        from {{ source("brutos_prontuario_mv_api_staging", "gestante_continuo") }}
        {% if is_incremental() %}
            where
                timestamp_trunc(datalake_loaded_at, day) >= timestamp(
                    date_sub(current_date('America/Sao_Paulo'), interval 7 day)
                )
        {% endif %}
    ),

    gestante_json as (
        select
            payload_cnes as id_cnes,
            json_extract_scalar(data, '$.data_aborto') as data_aborto,
            json_extract_scalar(data, '$.cd_aviso_aborto') as cd_aviso_aborto,
            json_extract_scalar(
                data, '$.ava_sob_ventre_materno'
            ) as ava_sob_ventre_materno,
            json_extract_scalar(data, '$.reanimacao') as reanimacao,
            json_extract_scalar(data, '$.utero') as utero,
            json_extract_scalar(data, '$.perineo') as perineo,
            json_extract_scalar(data, '$.perda_sangue') as perda_sangue,
            json_extract_scalar(data, '$.pa') as pa,
            json_extract_scalar(data, '$.nome') as nome,
            json_extract_scalar(data, '$.horario') as horario,
            json_extract_scalar(data, '$.sn_acompanhante') as sn_acompanhante,
            json_extract_scalar(data, '$.reanim_vent_press') as reanim_vent_press,
            json_extract_scalar(data, '$.periodo_expulsivo') as periodo_expulsivo,
            json_extract_scalar(data, '$.class_robson') as class_robson,
            json_extract_scalar(data, '$.tec_houve') as tec_houve,
            json_extract_scalar(data, '$.tec_respiracao') as tec_respiracao,
            json_extract_scalar(data, '$.tec_rebozo') as tec_rebozo,
            json_extract_scalar(data, '$.tec_penumbra') as tec_penumbra,
            json_extract_scalar(data, '$.tec_outros') as tec_outros,
            json_extract_scalar(data, '$.tec_oleo_perineal') as tec_oleo_perineal,
            json_extract_scalar(data, '$.tec_musica') as tec_musica,
            json_extract_scalar(data, '$.tec_recusa') as tec_recusa,
            json_extract_scalar(data, '$.tec_mov_pelvicos') as tec_mov_pelvicos,
            json_extract_scalar(data, '$.tec_massagem') as tec_massagem,
            json_extract_scalar(data, '$.tec_lateral') as tec_lateral,
            json_extract_scalar(data, '$.tec_imersao_4') as tec_imersao_4,
            json_extract_scalar(data, '$.tec_imersao_1_1') as tec_imersao_1_1,
            json_extract_scalar(data, '$.tec_imersao_1') as tec_imersao_1,
            json_extract_scalar(data, '$.tec_genupeitoral') as tec_genupeitoral,
            json_extract_scalar(data, '$.tec_escaldapes') as tec_escaldapes,
            json_extract_scalar(data, '$.tec_deambulacao') as tec_deambulacao,
            json_extract_scalar(data, '$.tec_compressa_morna') as tec_compressa_morna,
            json_extract_scalar(data, '$.tec_cavalinho') as tec_cavalinho,
            json_extract_scalar(data, '$.sn_tec_bola') as sn_tec_bola,
            json_extract_scalar(data, '$.tec_banqueta') as tec_banqueta,
            json_extract_scalar(data, '$.tec_aromaterapia') as tec_aromaterapia,
            json_extract_scalar(data, '$.tec_4_apoios') as tec_4_apoios,
            json_extract_scalar(data, '$.sedacao') as sedacao,
            json_extract_scalar(data, '$.cardiotoco') as cardiotoco,
            json_extract_scalar(data, '$.raquidian') as raquidian,
            json_extract_scalar(data, '$.peridural') as peridural,
            json_extract_scalar(data, '$.locoregional') as locoregional,
            json_extract_scalar(data, '$.anestesia_geral') as anestesia_geral,
            json_extract_scalar(data, '$.tp_vasos') as tp_vasos,
            json_extract_scalar(data, '$.solicitado_anatomo') as solicitado_anatomo,
            json_extract_scalar(data, '$.tipo_rotura') as tipo_rotura,
            json_extract_scalar(data, '$.pediatra') as pediatra,
            json_extract_scalar(data, '$.obstetra') as obstetra,
            json_extract_scalar(data, '$.circulante') as circulante,
            json_extract_scalar(data, '$.placenta') as placenta,
            json_extract_scalar(data, '$.momento_obito') as momento_obito,
            json_extract_scalar(data, '$.mgso4') as mgso4,
            json_extract_scalar(data, '$.anatomo') as anatomo,
            json_extract_scalar(data, '$.face_materna') as face_materna,
            json_extract_scalar(data, '$.face_fetal') as face_fetal,
            json_extract_scalar(data, '$.evolucao_parto') as evolucao_parto,
            json_extract_scalar(data, '$.episiotomia') as episiotomia,
            json_extract_scalar(data, '$.diu') as diu,
            json_extract_scalar(data, '$.diag_cirurgico') as diag_cirurgico,
            json_extract_scalar(data, '$.dequitacao') as dequitacao,
            json_extract_scalar(data, '$.contato_pele') as contato_pele,
            json_extract_scalar(data, '$.clampeamento') as clampeamento,
            json_extract_scalar(data, '$.cirurgia') as cirurgia,
            json_extract_scalar(data, '$.insercao_velamentosa') as insercao_velamentosa,
            json_extract_scalar(data, '$.circular') as circular,
            json_extract_scalar(data, '$.amamentacao') as amamentacao,
            json_extract_scalar(data, '$.acompanhante') as acompanhante,
            json_extract_scalar(data, '$.ds_rotura_perineal') as ds_rotura_perineal,
            json_extract_scalar(data, '$.ds_episiotomia') as ds_episiotomia,
            json_extract_scalar(data, '$.ds_tecno_houve') as ds_tecno_houve,
            json_extract_scalar(data, '$.ds_proc_outros') as ds_proc_outros,
            json_extract_scalar(
                data, '$.ds_outro_clampeamento'
            ) as ds_outro_clampeamento,
            json_extract_scalar(data, '$.nm_acompanhante') as nm_acompanhante,
            json_extract_scalar(data, '$.ds_intercorrencia') as ds_intercorrencia,
            json_extract_scalar(
                data, '$.ds_duracao_parto_assist'
            ) as ds_duracao_parto_assist,
            json_extract_scalar(data, '$.ds_duracao_parto') as ds_duracao_parto,
            json_extract_scalar(data, '$.ds_diag_pre_op') as ds_diag_pre_op,
            json_extract_scalar(data, '$.ds_dequitacao_outro') as ds_dequitacao_outro,
            json_extract_scalar(data, '$.ds_dequitacao_apos') as ds_dequitacao_apos,
            json_extract_scalar(data, '$.ds_vasos_cordao') as ds_vasos_cordao,
            json_extract_scalar(data, '$.ds_causa_obito') as ds_causa_obito,
            json_extract_scalar(
                data, '$.ds_apresentacao_outros'
            ) as ds_apresentacao_outros,
            json_extract_scalar(
                data, '$.ds_anomalia_congenita'
            ) as ds_anomalia_congenita,
            json_extract_scalar(data, '$.posicao_vertical') as posicao_vertical,
            json_extract_scalar(
                data, '$.posicao_semi_vertical'
            ) as posicao_semi_vertical,
            json_extract_scalar(data, '$.outras_posicoes') as outras_posicoes,
            json_extract_scalar(data, '$.posicao_lateral') as posicao_lateral,
            json_extract_scalar(data, '$.posicao_horizontal') as posicao_horizontal,
            json_extract_scalar(data, '$.posicao_cocoras') as posicao_cocoras,
            json_extract_scalar(data, '$.posicao_cavalinho') as posicao_cavalinho,
            json_extract_scalar(data, '$.posicao_cadeira') as posicao_cadeira,
            json_extract_scalar(data, '$.posicao_banqueta') as posicao_banqueta,
            json_extract_scalar(data, '$.posicao_banheira') as posicao_banheira,
            json_extract_scalar(data, '$.posicao_4_apoios') as posicao_4_apoios,
            json_extract_scalar(data, '$.parto_ppp') as parto_ppp,
            json_extract_scalar(data, '$.parto_maca') as parto_maca,
            json_extract_scalar(data, '$.parto_chuveiro') as parto_chuveiro,
            json_extract_scalar(data, '$.parto_cama') as parto_cama,
            json_extract_scalar(data, '$.reanim_med') as reanim_med,
            json_extract_scalar(data, '$.reanim_o2') as reanim_o2,
            json_extract_scalar(data, '$.reanim_intub') as reanim_intub,
            json_extract_scalar(data, '$.reanim_massagem') as reanim_massagem,
            json_extract_scalar(data, '$.reanim_ventilatoria') as reanim_ventilatoria,
            json_extract_scalar(data, '$.doula') as doula,
            json_extract_scalar(data, '$.tocotraumatismo') as tocotraumatismo,
            json_extract_scalar(data, '$.anomalia_congenita') as anomalia_congenita,
            json_extract_scalar(data, '$.outros_procedimentos') as outros_procedimentos,
            json_extract_scalar(data, '$.krause') as krause,
            json_extract_scalar(data, '$.us') as us,
            json_extract_scalar(data, '$.proc_antihipert') as proc_antihipert,
            json_extract_scalar(data, '$.transfusao') as transfusao,
            json_extract_scalar(data, '$.dilapam') as dilapam,
            json_extract_scalar(data, '$.miso') as miso,
            json_extract_scalar(data, '$.amniotomia') as amniotomia,
            json_extract_scalar(data, '$.ocitocina') as ocitocina,
            json_extract_scalar(data, '$.tp_raca') as tp_raca,
            json_extract_scalar(data, '$.tp_sexo_rn') as tp_sexo_rn,
            json_extract_scalar(data, '$.tp_apresentacao') as tp_apresentacao,
            json_extract_scalar(data, '$.tp_parto') as tp_parto,
            json_extract_scalar(data, '$.peso_natimorto') as peso_natimorto,
            json_extract_scalar(data, '$.peso_placenta') as peso_placenta,
            json_extract_scalar(data, '$.vl_peso_rn') as vl_peso_rn,
            json_extract_scalar(data, '$.apgar_5') as apgar_5,
            json_extract_scalar(data, '$.apgar_1') as apgar_1,
            json_extract_scalar(data, '$.capurro') as capurro,
            json_extract_scalar(data, '$.ds_cirurgia') as ds_cirurgia,
            json_extract_scalar(data, '$.ds_conduta') as ds_conduta,
            json_extract_scalar(data, '$.dt_parto') as dt_parto,
            json_extract_scalar(data, '$.nm_paciente') as nm_paciente,
            json_extract_scalar(data, '$.dh_fechamento') as dh_fechamento,
            json_extract_scalar(data, '$.tp_status') as tp_status,
            json_extract_scalar(data, '$.cd_prestador') as cd_prestador,
            json_extract_scalar(data, '$.cd_atendimento_doc') as cd_atendimento_doc,
            json_extract_scalar(data, '$.cd_paciente') as cd_paciente,
            json_extract_scalar(data, '$.cd_documento_clinico') as cd_documento_clinico,
            json_extract_scalar(
                data, '$.especialidadeAtendimento'
            ) as especialidadeatendimento,
            json_extract_scalar(data, '$.tipoAtendimento') as tipoatendimento,
            json_extract_scalar(data, '$.nome_mae') as nome_mae,
            json_extract_scalar(data, '$.pcd') as pcd,
            json_extract_scalar(data, '$.sexo_paciente') as sexo_paciente,
            json_extract_scalar(data, '$.data_atendimento') as data_atendimento,
            json_extract_scalar(
                data, '$.data_nascimento_paciente'
            ) as data_nascimento_paciente,
            json_extract_scalar(data, '$.nome_social_paciente') as nome_social_paciente,
            json_extract_scalar(
                data, '$.data_hora_alta_medica'
            ) as data_hora_alta_medica,
            json_extract_scalar(data, '$.cns_paciente') as cns_paciente,
            json_extract_scalar(data, '$.cpf_paciente') as cpf_paciente,
            json_extract_scalar(data, '$.nrcnes') as nrcnes,
            json_extract_scalar(
                data, '$.unidade_atendimento_nome'
            ) as unidade_atendimento_nome,
            json_extract_scalar(
                data, '$.profissional_saude_nome'
            ) as profissional_saude_nome,
            datalake_loaded_at,
            source_updated_at
        from source
    ),

    gestante_renomeado as (
        select
            -- Atendimento
            {{ process_null("cd_atendimento_doc") }} as id_atendimento,
            {{ process_null("especialidadeAtendimento") }} as atendimento_especialidade,
            case
                when tipoatendimento like 'A'
                then 'AMBULATORIAL'
                when tipoatendimento like 'E'
                then 'EXTERNO'
                when tipoatendimento like 'I'
                then 'INTERNAÇÃO'
                when tipoatendimento like 'U'
                then 'URGÊNCIA'
                else tipoatendimento
            end as atendimento_tipo,
            safe.parse_datetime(
                '%Y/%m/%d %H:%M:%S', data_atendimento
            ) as atendimento_datahora,
            {{ process_null("unidade_atendimento_nome") }} as estabelecimento_nome,
            {{ process_null("profissional_saude_nome") }} as profissional_nome,

            -- Paciente
            {{ process_null("id_cnes") }} as id_cnes,
            {{ process_null("nm_paciente") }} as paciente_nome,
            {{ process_null("nome_mae") }} as paciente_mae_nome,
            {{ process_null("pcd") }} as paciente_pcd,
            {{ process_null("sexo_paciente") }} as paciente_sexo,
            safe.parse_date(
                '%Y/%m/%d', data_nascimento_paciente
            ) as paciente_data_nascimento,
            {{ process_null("nome_social_paciente") }} as paciente_nome_social,
            {{ process_null("cns_paciente") }} as paciente_cns,
            {{ process_null("cpf_paciente") }} as paciente_cpf,

            -- Gestante
            safe.parse_date('%Y/%m/%d', data_aborto) as aborto_data,
            {{ process_null("cd_aviso_aborto") }} as aborto_aviso,
            {{ process_null("ava_sob_ventre_materno") }} as ava_sob_ventre_materno,
            {{ process_null("reanimacao") }} as reanimacao,
            {{ process_null("utero") }} as utero,
            {{ process_null("perineo") }} as perineo,
            {{ process_null("perda_sangue") }} as perda_sangue,
            {{ process_null("pa") }} as pa,
            {{ process_null("nome") }} as nome,
            {{ process_null("horario") }} as horario,
            {{ process_null("sn_acompanhante") }} as sn_acompanhante,
            {{ process_null("reanim_vent_press") }} as reanim_vent_press,
            {{ process_null("periodo_expulsivo") }} as periodo_expulsivo,
            {{ process_null("class_robson") }} as class_robson,
            {{ process_null("tec_houve") }} as tec_houve,
            {{ process_null("tec_respiracao") }} as tec_respiracao,
            {{ process_null("tec_rebozo") }} as tec_rebozo,
            {{ process_null("tec_penumbra") }} as tec_penumbra,
            {{ process_null("tec_outros") }} as tec_outros,
            {{ process_null("tec_oleo_perineal") }} as tec_oleo_perineal,
            {{ process_null("tec_musica") }} as tec_musica,
            {{ process_null("tec_recusa") }} as tec_recusa,
            {{ process_null("tec_mov_pelvicos") }} as tec_mov_pelvicos,
            {{ process_null("tec_massagem") }} as tec_massagem,
            {{ process_null("tec_lateral") }} as tec_lateral,
            {{ process_null("tec_imersao_4") }} as tec_imersao_4,
            {{ process_null("tec_imersao_1_1") }} as tec_imersao_1_1,
            {{ process_null("tec_imersao_1") }} as tec_imersao_1,
            {{ process_null("tec_genupeitoral") }} as tec_genupeitoral,
            {{ process_null("tec_escaldapes") }} as tec_escaldapes,
            {{ process_null("tec_deambulacao") }} as tec_deambulacao,
            {{ process_null("tec_compressa_morna") }} as tec_compressa_morna,
            {{ process_null("tec_cavalinho") }} as tec_cavalinho,
            {{ process_null("sn_tec_bola") }} as sn_tec_bola,
            {{ process_null("tec_banqueta") }} as tec_banqueta,
            {{ process_null("tec_aromaterapia") }} as tec_aromaterapia,
            {{ process_null("tec_4_apoios") }} as tec_4_apoios,
            {{ process_null("sedacao") }} as sedacao,
            {{ process_null("cardiotoco") }} as cardiotoco,
            {{ process_null("raquidian") }} as raquidian,
            {{ process_null("peridural") }} as peridural,
            {{ process_null("locoregional") }} as locoregional,
            {{ process_null("anestesia_geral") }} as anestesia_geral,
            {{ process_null("tp_vasos") }} as tp_vasos,
            {{ process_null("solicitado_anatomo") }} as solicitado_anatomo,
            {{ process_null("tipo_rotura") }} as tipo_rotura,
            {{ process_null("pediatra") }} as pediatra,
            {{ process_null("obstetra") }} as obstetra,
            {{ process_null("circulante") }} as circulante,
            {{ process_null("placenta") }} as placenta,
            {{ process_null("momento_obito") }} as momento_obito,
            {{ process_null("mgso4") }} as mgso4,
            {{ process_null("anatomo") }} as anatomo,
            {{ process_null("face_materna") }} as face_materna,
            {{ process_null("face_fetal") }} as face_fetal,
            {{ process_null("evolucao_parto") }} as evolucao_parto,
            {{ process_null("episiotomia") }} as episiotomia,
            {{ process_null("diu") }} as diu,
            {{ process_null("diag_cirurgico") }} as diag_cirurgico,
            {{ process_null("dequitacao") }} as dequitacao,
            {{ process_null("contato_pele") }} as contato_pele,
            {{ process_null("clampeamento") }} as clampeamento,
            {{ process_null("cirurgia") }} as cirurgia,
            {{ process_null("insercao_velamentosa") }} as insercao_velamentosa,
            {{ process_null("circular") }} as circular,
            {{ process_null("amamentacao") }} as amamentacao,
            {{ process_null("acompanhante") }} as acompanhante,
            {{ process_null("ds_rotura_perineal") }} as rotura_perineal,
            {{ process_null("ds_episiotomia") }} as episiotomia_descricao,
            {{ process_null("ds_tecno_houve") }} as tecno_houve,
            {{ process_null("ds_proc_outros") }} as proc_outros,
            {{ process_null("ds_outro_clampeamento") }} as outro_clampeamento,
            {{ process_null("nm_acompanhante") }} as numero_acompanhante,
            {{ process_null("ds_intercorrencia") }} as intercorrencia,
            {{ process_null("ds_duracao_parto_assist") }} as duracao_parto_assistido,
            {{ process_null("ds_duracao_parto") }} as duracao_parto,
            {{ process_null("ds_diag_pre_op") }} as diagnostico_pre_operatorio,
            {{ process_null("ds_dequitacao_outro") }} as dequitacao_outro,
            {{ process_null("ds_dequitacao_apos") }} as dequitacao_apos,
            {{ process_null("ds_vasos_cordao") }} as vasos_cordao,
            {{ process_null("ds_causa_obito") }} as causa_obito,
            {{ process_null("ds_apresentacao_outros") }} as apresentacao_outros,
            {{ process_null("ds_anomalia_congenita") }} as ds_anomalia_congenita,
            {{ process_null("posicao_vertical") }} as posicao_vertical,
            {{ process_null("posicao_semi_vertical") }} as posicao_semi_vertical,
            {{ process_null("outras_posicoes") }} as outras_posicoes,
            {{ process_null("posicao_lateral") }} as posicao_lateral,
            {{ process_null("posicao_horizontal") }} as posicao_horizontal,
            {{ process_null("posicao_cocoras") }} as posicao_cocoras,
            {{ process_null("posicao_cavalinho") }} as posicao_cavalinho,
            {{ process_null("posicao_cadeira") }} as posicao_cadeira,
            {{ process_null("posicao_banqueta") }} as posicao_banqueta,
            {{ process_null("posicao_banheira") }} as posicao_banheira,
            {{ process_null("posicao_4_apoios") }} as posicao_4_apoios,
            {{ process_null("parto_ppp") }} as parto_ppp,
            {{ process_null("parto_maca") }} as parto_maca,
            {{ process_null("parto_chuveiro") }} as parto_chuveiro,
            {{ process_null("parto_cama") }} as parto_cama,
            {{ process_null("reanim_med") }} as reanimacao_medicamentosa,
            {{ process_null("reanim_o2") }} as reanimacao_o2,
            {{ process_null("reanim_intub") }} as reanimacao_intubacao,
            {{ process_null("reanim_massagem") }} as reanimacao_massagem,
            {{ process_null("reanim_ventilatoria") }} as reanimacao_ventilatoria,
            {{ process_null("doula") }} as doula,
            {{ process_null("tocotraumatismo") }} as tocotraumatismo,
            {{ process_null("anomalia_congenita") }} as anomalia_congenita,
            {{ process_null("outros_procedimentos") }} as outros_procedimentos,
            {{ process_null("krause") }} as krause,
            {{ process_null("us") }} as us,
            {{ process_null("proc_antihipert") }} as procedimento_antihipertensivo,
            {{ process_null("transfusao") }} as transfusao,
            {{ process_null("dilapam") }} as dilapam,
            {{ process_null("miso") }} as miso,
            {{ process_null("amniotomia") }} as amniotomia,
            {{ process_null("ocitocina") }} as ocitocina,
            {{ process_null("tp_raca") }} as raca,
            {{ process_null("tp_sexo_rn") }} as sexo_rn,
            {{ process_null("tp_apresentacao") }} as apresentacao,
            {{ process_null("tp_parto") }} as tipo_parto,
            safe_cast(peso_natimorto as int64) as peso_natimorto,
            safe_cast(peso_placenta as int64) as peso_placenta,
            safe_cast(vl_peso_rn as int64) as peso_rn,
            safe_cast(apgar_5 as int64) as apgar_5,
            safe_cast(apgar_1 as int64) as apgar_1,
            {{ process_null("capurro") }} as capurro,
            {{ process_null("ds_cirurgia") }} as ds_cirurgia,
            {{ process_null("ds_conduta") }} as ds_conduta,
            safe.parse_datetime('%Y/%m/%d %H:%M:%S', dt_parto) as dt_parto,
            safe_cast(dh_fechamento as datetime) as fechamento_datahora,
            {{ process_null("tp_status") }} as tp_status,
            {{ process_null("cd_prestador") }} as id_prestador,
            {{ process_null("cd_paciente") }} as id_paciente,
            {{ process_null("cd_documento_clinico") }} as documento_clinico,
            safe.parse_datetime(
                '%Y/%m/%d %H:%M:%S', data_hora_alta_medica
            ) as alta_medica_datahora,
            {{ process_null("nrcnes") }} as cnes,

            -- Metadados
            datetime(datalake_loaded_at, 'America/Sao_Paulo') as loaded_at,
            safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at) as updated_at,
            cast(datalake_loaded_at as date) as data_particao
        from gestante_json
    ),

    gestante_deduplicado as (
        select *
        from gestante_renomeado
        qualify
            row_number() over (
                partition by id_atendimento, id_cnes order by loaded_at desc
            )
            = 1
    )

select
    {{ dbt_utils.generate_surrogate_key(["id_atendimento", "id_cnes"]) }} as id_hci, *
from gestante_deduplicado
