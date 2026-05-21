{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__profissionais_unidades',
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
    from {{ source("brutos_gdb_cnes_staging", "LFCES021") }}
),

profissionais_lookup as (
    select
        safe_cast(data_particao as date) as data_particao,
        nullif(json_value(json, '$.PROF_ID'), '') as profissional_id_original,
        lpad(nullif(json_value(json, '$.CPF_PROF'), ''), 11, '0') as cpf,
        lpad(nullif(json_value(json, '$.COD_CNS'), ''), 15, '0') as cns,
        nullif(json_value(json, '$.NOME_PROF'), '') as nome_profissional,
        safe_cast(nullif(json_value(json, '$.DATA_ATU'), '') as date) as dt_atualiza_profissional,
        safe_cast(_loaded_at as timestamp) as loaded_at_profissional
    from {{ source("brutos_gdb_cnes_staging", "LFCES018") }}
    qualify row_number() over (
        partition by safe_cast(data_particao as date), nullif(json_value(json, '$.PROF_ID'), '')
        order by safe_cast(_loaded_at as timestamp) desc, safe_cast(nullif(json_value(json, '$.DATA_ATU'), '') as date) desc
    ) = 1
),

unidades as (
    select
        data_particao,
        unidade_id_original,
        cnes,
        ap,
        ap_formatada,
        nome_fanta as nome_unidade
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

        -- chaves originais CNES/GDB
        nullif(json_value(json, '$.PROF_ID'), '') as profissional_id_original,
        nullif(json_value(json, '$.UNIDADE_ID'), '') as unidade_id_original,
        nullif(json_value(json, '$.COD_CBO'), '') as cbo_id_original,

        -- carga horária
        safe_cast(nullif(json_value(json, '$.CG_HORAAMB'), '') as int64) as cg_horaamb,
        safe_cast(nullif(json_value(json, '$.CGHORAHOSP'), '') as int64) as cg_horahosp,
        safe_cast(nullif(json_value(json, '$.CGHORAOUTR'), '') as int64) as cg_horaoutr,

        -- conselho / registro
        nullif(json_value(json, '$.N_REGISTRO'), '') as numero_registro,
        nullif(json_value(json, '$.SG_UF_CRM'), '') as uf_registro,
        nullif(json_value(json, '$.CONSELHOID'), '') as conselho_id_original,

        -- vínculo
        nullif(json_value(json, '$.IND_VINC'), '') as vinculacao_id_original,
        nullif(json_value(json, '$.NU_CNPJ_DET_VINC'), '') as cnpj_detalhe_vinculo,
        nullif(json_value(json, '$.TP_SUS_NAO_SUS'), '') as tipo_sus_nao_sus,

        -- flags CNES
        nullif(json_value(json, '$.TP_PRECEPTOR'), '') as tp_preceptor_original,
        nullif(json_value(json, '$.TP_RESIDENTE'), '') as tp_residente_original,

        -- status/datas
        nullif(json_value(json, '$.STATUS'), '') as status,
        nullif(json_value(json, '$.STATUSMOV'), '') as status_movimento,
        safe_cast(nullif(json_value(json, '$.DT_INIATIV'), '') as date) as dt_inicio_atividade,
        safe_cast(nullif(json_value(json, '$.DATA_ATU'), '') as date) as dt_atualiza

    from fonte
),

tratado as (
    select
        e.*,

        -- profissional resolvido pela LFCES018
        p.cpf,
        p.cns,
        p.nome_profissional,

        -- unidade resolvida pelo int de unidades
        u.cnes,
        u.ap,
        u.ap_formatada,
        u.nome_unidade,

        -- CBO em formato texto/código
        nullif(e.cbo_id_original, '') as cod_cbo,

        -- chave compatível com a lógica legada:
        -- CPF + CNES + CBO
        concat(
            coalesce(p.cpf, ''),
            coalesce(u.cnes, ''),
            coalesce(e.cbo_id_original, '')
        ) as chave_profissional_unidade_cbo,

        coalesce(e.cg_horaamb, 0)
            + coalesce(e.cg_horahosp, 0)
            + coalesce(e.cg_horaoutr, 0) as carga_horaria_total,

        case
            when coalesce(e.cg_horaamb, 0) >= 40 or coalesce(e.cg_horaoutr, 0) >= 40 then '40'
            when coalesce(e.cg_horaamb, 0) >= 20 or coalesce(e.cg_horaoutr, 0) >= 20 then '20'
            when coalesce(e.cg_horaamb, 0) > 0 or coalesce(e.cg_horaoutr, 0) > 0 then 'OUTRA'
            else null
        end as carga_horaria_classificacao,

        -- No dicionário CNES: 1 = sim, 2 = não.
        case
            when e.tp_preceptor_original = '1' then 1
            when e.tp_preceptor_original = '2' then 0
            else null
        end as tp_preceptor,

        case
            when e.tp_residente_original = '1' then 1
            when e.tp_residente_original = '2' then 0
            else null
        end as tp_residente,

        case when p.cpf is not null then 1 else 0 end as profissional_encontrado,
        case when u.cnes is not null then 1 else 0 end as unidade_encontrada

    from extraido e
    left join profissionais_lookup p
        on e.data_particao = p.data_particao
        and e.profissional_id_original = p.profissional_id_original
    left join unidades u
        on e.data_particao = u.data_particao
        and e.unidade_id_original = u.unidade_id_original
),

deduplicado as (
    select *
    from tratado
    where cpf is not null
      and cnes is not null
      and cod_cbo is not null
    qualify row_number() over (
        partition by data_particao, cpf, cnes, cod_cbo
        order by loaded_at desc, dt_atualiza desc
    ) = 1
)

select *
from deduplicado
