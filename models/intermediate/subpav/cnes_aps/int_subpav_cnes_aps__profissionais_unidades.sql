{{
    config(
        schema = 'intermediario_plataforma_subpav',
        alias = 'cnes_aps__profissionais_unidades',
        materialized = "table",
        partition_by = {
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by = ["cpf", "cnes", "cod_cbo"],
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

        UNIDADE_ID,
        PROF_ID,
        COD_CBO,
        TP_SUS_NAO_SUS,
        IND_VINC,
        D_TERCSIH,
        CG_HORAAMB,
        CGHORAHOSP,
        CGHORAOUTR,
        CONSELHOID,
        N_REGISTRO,
        SG_UF_CRM,
        STATUS,
        STATUSMOV,
        TP_PRECEPTOR,
        TP_RESIDENTE,
        NU_CNPJ_DET_VINC,
        DATA_ATU,
        USUARIO,
        CHKSUM,
        DT_ATUALIZACAO_ORIGEM,
        DT_CMTP_INICIO,
        DT_CMTP_FIM,
        NU_SEQ_PROCESSO

    from {{ ref("raw_gdb_cnes__lfces021") }}
),

profissionais_lookup as (
    select
        safe_cast(data_particao as date) as data_particao,
        nullif(cast(PROF_ID as string), '') as profissional_id_original,
        lpad(
            nullif(regexp_replace(cast(CPF_PROF as string), r'[^0-9]', ''), ''),
            11,
            '0'
        ) as cpf,
        lpad(
            nullif(regexp_replace(cast(COD_CNS as string), r'[^0-9]', ''), ''),
            15,
            '0'
        ) as cns,
        nullif(cast(NOME_PROF as string), '') as nome_profissional,
        safe_cast(nullif(cast(DATA_ATU as string), '') as date) as dt_atualiza_profissional,
        safe_cast(_loaded_at as timestamp) as loaded_at_profissional

    from {{ ref("raw_gdb_cnes__lfces018") }}

    qualify row_number() over (
        partition by
            safe_cast(data_particao as date),
            nullif(cast(PROF_ID as string), '')
        order by
            safe_cast(_loaded_at as timestamp) desc,
            safe_cast(nullif(cast(DATA_ATU as string), '') as date) desc
    ) = 1
),

unidades as (
    select
        data_particao,
        unidade_id_original,
        cnes,
        nome_fanta as nome_unidade,
        ap,
        ap_formatada,
        is_municipio_rio,
        tipo_unidade_sms,
        is_unidade_aps_panorama,
        unidade_ativa
    from {{ ref("int_subpav_cnes_aps__unidades") }}
),

extraido as (
    select
        
        nullif(cast(PROF_ID as string), '') as profissional_id_original,
        nullif(cast(UNIDADE_ID as string), '') as unidade_id_original,
        upper(nullif(cast(COD_CBO as string), '')) as cod_cbo,

        
        nullif(cast(IND_VINC as string), '') as vinculacao_id_original,
        nullif(cast(TP_SUS_NAO_SUS as string), '') as tipo_sus_nao_sus,
        nullif(cast(D_TERCSIH as string), '') as detalhe_terceirizado_sih,
        nullif(cast(NU_CNPJ_DET_VINC as string), '') as cnpj_detalhe_vinculo,

        
        safe_cast(nullif(cast(CG_HORAAMB as string), '') as int64) as cg_horaamb,
        safe_cast(nullif(cast(CGHORAHOSP as string), '') as int64) as cg_horahosp,
        safe_cast(nullif(cast(CGHORAOUTR as string), '') as int64) as cg_horaoutr,

        
        nullif(cast(CONSELHOID as string), '') as conselho_id_original,
        nullif(cast(N_REGISTRO as string), '') as numero_registro,
        nullif(cast(SG_UF_CRM as string), '') as uf_registro,

        
        nullif(cast(TP_PRECEPTOR as string), '') as tp_preceptor_original,
        nullif(cast(TP_RESIDENTE as string), '') as tp_residente_original,

        
        nullif(cast(STATUS as string), '') as status,
        nullif(cast(STATUSMOV as string), '') as status_movimento,
        safe_cast(nullif(cast(DATA_ATU as string), '') as date) as dt_atualiza,
        nullif(cast(USUARIO as string), '') as usuario_atualizacao,
        nullif(cast(CHKSUM as string), '') as checksum,
        safe_cast(nullif(cast(DT_ATUALIZACAO_ORIGEM as string), '') as date) as dt_atualizacao_origem,
        safe_cast(nullif(cast(DT_CMTP_INICIO as string), '') as date) as dt_cmtp_inicio,
        safe_cast(nullif(cast(DT_CMTP_FIM as string), '') as date) as dt_cmtp_fim,
        nullif(cast(NU_SEQ_PROCESSO as string), '') as nu_seq_processo,

        
        format_date('%Y-%m', data_particao) as competencia_mes,
        data_particao,
        ano_particao,
        mes_particao,
        loaded_at,
        _source_file

    from fonte
),

tratado as (
    select
        e.*,

        
        p.cpf,
        p.cns,
        p.nome_profissional,
        p.dt_atualiza_profissional,

        
        u.cnes,
        u.nome_unidade,
        u.ap,
        u.ap_formatada,
        u.is_municipio_rio,
        u.tipo_unidade_sms,
        u.is_unidade_aps_panorama,
        u.unidade_ativa,

        
        concat(
            coalesce(p.cpf, ''),
            coalesce(u.cnes, ''),
            coalesce(e.cod_cbo, '')
        ) as chave_profissional_unidade_cbo,

        coalesce(e.cg_horaamb, 0)
        + coalesce(e.cg_horahosp, 0)
        + coalesce(e.cg_horaoutr, 0) as carga_horaria_total,

        case
            when coalesce(e.cg_horaamb, 0) >= 40
                or coalesce(e.cg_horaoutr, 0) >= 40
                then '40'
            when coalesce(e.cg_horaamb, 0) >= 20
                or coalesce(e.cg_horaoutr, 0) >= 20
                then '20'
            when coalesce(e.cg_horaamb, 0) > 0
                or coalesce(e.cg_horaoutr, 0) > 0
                or coalesce(e.cg_horahosp, 0) > 0
                then 'OUTRA'
            else null
        end as carga_horaria_classificacao,

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
        case when u.cnes is not null then 1 else 0 end as unidade_encontrada,

        case
            when coalesce(e.cg_horaamb, 0)
                + coalesce(e.cg_horahosp, 0)
                + coalesce(e.cg_horaoutr, 0) > 0
                then 1
            else 0
        end as possui_carga_horaria

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
        order by
            loaded_at desc,
            dt_atualiza desc,
            carga_horaria_total desc,
            profissional_id_original desc
    ) = 1
)

select
    
    cpf,
    cns,
    nome_profissional,
    profissional_id_original,

    
    cnes,
    unidade_id_original,
    nome_unidade,
    ap,
    ap_formatada,
    is_municipio_rio,

    
    cod_cbo,
    vinculacao_id_original,
    tipo_sus_nao_sus,
    detalhe_terceirizado_sih,
    cnpj_detalhe_vinculo,

    
    cg_horaamb,
    cg_horahosp,
    cg_horaoutr,
    carga_horaria_total,
    carga_horaria_classificacao,
    possui_carga_horaria,

    
    conselho_id_original,
    numero_registro,
    uf_registro,

    
    tp_preceptor_original,
    tp_preceptor,
    tp_residente_original,
    tp_residente,

    
    tipo_unidade_sms,
    is_unidade_aps_panorama,
    unidade_ativa,

    
    chave_profissional_unidade_cbo,
    profissional_encontrado,
    unidade_encontrada,

    
    status,
    status_movimento,
    dt_atualiza,
    dt_atualiza_profissional,
    dt_atualizacao_origem,
    dt_cmtp_inicio,
    dt_cmtp_fim,
    usuario_atualizacao,
    checksum,
    nu_seq_processo,

    
    competencia_mes,
    data_particao,
    ano_particao,
    mes_particao,
    loaded_at,
    _source_file

from deduplicado
