{{
    config(
        alias="paciente_historico",
        materialized="incremental",
        unique_key="paciente_cpf",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    events_from_window as (
        select *
        -- rj-sms-dev.brutos_prontuario_vitacare_staging.paciente_historico_eventos
        from {{ source("brutos_prontuario_vitacare_staging", "paciente_historico_eventos") }}
        {% if is_incremental() %} where data_particao > '{{seven_days_ago}}' {% endif %}
    ),
    events_ranked_by_freshness as (
        select *, row_number() over (partition by patient_cpf order by source_updated_at desc) as rank
        from events_from_window
    ),
    latest_events as (select * from events_ranked_by_freshness where rank = 1)
select
    safe_cast(patient_cpf as string) as paciente_cpf,
    safe_cast(source_id as string) as id,
    safe_cast(data__AP as string) as ap,
    safe_cast(data__SEXO as string) as sexo,
    safe_cast(data__HIST_CID as string) as hist_cid,
    safe_cast(data__RACA_COR as string) as raca_cor,
    safe_cast(data__RELIGIAO as string) as religiao,
    safe_cast(data__ESCOLARIDADE as string) as escolaridade,
    safe_cast(data__dataConsulta as string) as data_consulta,
    safe_cast(data__NACIONALIDADE as string) as nacionalidade,
    safe_cast(data__FREQUENTA_ESCOLA as string) as frequenta_escola,
    safe_cast(data__SITUACAO_USUARIO as string) as situacao_usuario,
    safe_cast(data__TELEFONE_CONTATO as string) as telefone_contato,
    safe_cast(data__dataNascPaciente as string) as data_nasc_paciente,
    safe_cast(data__SITUACAO_FAMILIAR as string) as situacao_familiar,
    safe_cast(data__TERRITORIO_SOCIAL as string) as territorio_social,
    safe_cast(data__NUMERO_CNES_UNIDADE as string) as numero_cnes_unidade,
    safe_cast(data__N_DE_CONSULTAS_2018 as string) as n_de_consultas_2018,
    safe_cast(data__N_DE_CONSULTAS_2019 as string) as n_de_consultas_2019,
    safe_cast(data__N_DE_CONSULTAS_2020 as string) as n_de_consultas_2020,
    safe_cast(data__N_DE_CONSULTAS_2021 as string) as n_de_consultas_2021,
    safe_cast(data__N_DE_CONSULTAS_2022 as string) as n_de_consultas_2022,
    safe_cast(data__N_DE_CONSULTAS_2023 as string) as n_de_consultas_2023,
    safe_cast(data__PACIENTE_TEMPORARIO as string) as paciente_temporario,
    safe_cast(data__NOME_UNIDADE_DE_SAUDE as string) as nome_unidade_de_saude,
    safe_cast(data__POSSUI_PLANO_DE_SAUDE as string) as possui_plano_de_saude,
    safe_cast(data__SITUACAO_PROFISSIONAL as string) as situacao_profissional,
    safe_cast(data__MUNICIPIO_DE_NASCIMENTO as string) as municipio_de_nascimento,
    safe_cast(data__N_DE_PROCEDIMENTOS_2018 as string) as n_de_procedimentos_2018,
    safe_cast(data__N_DE_PROCEDIMENTOS_2019 as string) as n_de_procedimentos_2019,
    safe_cast(data__N_DE_PROCEDIMENTOS_2020 as string) as n_de_procedimentos_2020,
    safe_cast(data__N_DE_PROCEDIMENTOS_2021 as string) as n_de_procedimentos_2021,
    safe_cast(data__N_DE_PROCEDIMENTOS_2022 as string) as n_de_procedimentos_2022,
    safe_cast(data__N_DE_PROCEDIMENTOS_2023 as string) as n_de_procedimentos_2023,
    safe_cast(data__PACIENTE_SITUACAO_RUA as string) as paciente_situacao_rua,
    safe_cast(data__CODIGO_DA_EQUIPE_DE_SAUDE as string) as codigo_da_equipe_de_saude,
    safe_cast(data__NOME_DA_PESSOA_CADASTRADA as string) as nome_da_pessoa_cadastrada,
    safe_cast(data__N_CNS_DA_PESSOA_CADASTRADA as string) as n_cns_da_pessoa_cadastrada,
    safe_cast(data__NOME_DA_MAE_PESSOA_CADASTRADA as string) as nome_da_mae_pessoa_cadastrada,
    safe_cast(data_particao as date) as data_particao,
    safe_cast(source_updated_at as string) as updated_at,
from latest_events


