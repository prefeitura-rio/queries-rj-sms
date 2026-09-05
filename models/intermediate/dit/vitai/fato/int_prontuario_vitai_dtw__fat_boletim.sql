{{
    config(
        alias="fat_boletim",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_boletim as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_boletim") }}
    ),

    int_paciente_rede as (
        select *
        from {{ ref("int_prontuario_vitai_dtw__fat_paciente_rede") }}
    ),

    dim_especialidade as (
        select esp_id, esp_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_especialidade") }}
    ),

    dim_cbo as (
        select cbo_id, cbo_codigo, cbo_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_cbo") }}
    ),

    dim_tipo_atendimento as (
        select tpa_id, tpa_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_tipo_atendimento") }}
    ),

    dim_tipo_entrada as (
        select tpe_id, tpe_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_tipo_entrada") }}
    ),

    dim_tipo_unidade_entrada as (
        select tpu_id, tpu_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_tipo_unidade_entrada") }}
    ),

    dim_prioridade as (
        select pri_id, pri_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_prioridade") }}
    ),

    dim_risco as (
        select ris_id, ris_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_risco") }}
    ),

    dim_faixa_etaria as (
        select fae_id, fae_descricao, fae_idade_inicial, fae_idade_final
        from {{ ref("raw_prontuario_vitai_dtw_dim_faixa_etaria") }}
    ),

    final as (
        select
            -- Chave primária
            b.boletim_gid,

            -- Chaves estrangeiras (mantidas para rastreabilidade)
            b.fat_paciente_rede_id,
            b.estabelecimento_gid,
            b.esp_id,
            b.cbo_id,
            b.tpa_id,
            b.tpe_id,
            b.tpu_id,
            b.pri_id,
            b.ris_id,
            b.fae_id,
            b.dea_id,
            b.mob_id,
            b.met_id,
            b.orp_id,

            -- Dados do boletim
            b.numero_be,
            b.idade,
            upper(trim(b.interno)) as interno,

            -- Descrições dimensionais
            esp.esp_descricao,
            cbo.cbo_codigo,
            cbo.cbo_descricao,
            tpa.tpa_descricao,
            tpe.tpe_descricao,
            tpu.tpu_descricao,
            pri.pri_descricao,
            ris.ris_descricao,
            fae.fae_descricao,
            fae.fae_idade_inicial,
            fae.fae_idade_final,

            -- Dados do paciente
            b.paciente_gid,
            pac.cpf as paciente_cpf,
            pac.cns as paciente_cns,
            pac.nome as paciente_nome,
            pac.nome_alternativo as paciente_nome_alternativo,
            pac.nomemae as paciente_nomemae,
            pac.nomepai as paciente_nomepai,
            pac.data_nascimento as paciente_data_nascimento,
            pac.sexo as paciente_sexo,
            pac.transex as paciente_transex,
            pac.naturalidade as paciente_naturalidade,
            pac.raca_id as paciente_raca_id,
            pac.raca_descricao as paciente_raca_descricao,
            pac.telefone as paciente_telefone,
            pac.celular as paciente_celular,
            pac.telefone_extra_um as paciente_telefone_extra_um,
            pac.telefone_extra_dois as paciente_telefone_extra_dois,
            pac.email as paciente_email,
            pac.cep as paciente_cep,
            pac.tipologradouro as paciente_tipologradouro,
            pac.nomelogradouro as paciente_nomelogradouro,
            pac.bai_id as paciente_bai_id,
            pac.bai_descricao as paciente_bai_descricao,
            pac.mun_id as paciente_mun_id,
            pac.mun_descricao as paciente_mun_descricao,
            pac.mun_uf as paciente_mun_uf,

            -- Datas
            b.data_entrada,
            b.data_alta,
            b.data_internacao,
            b.datahora,

            -- Metadados
            b.created_at,
            b.updated_at,
            b.loaded_at,
            b.data_particao
        from raw_boletim as b
        left join int_paciente_rede as pac on b.fat_paciente_rede_id = pac.fat_paciente_rede_id
        left join dim_especialidade as esp on b.esp_id = esp.esp_id
        left join dim_cbo as cbo on b.cbo_id = cbo.cbo_id
        left join dim_tipo_atendimento as tpa on b.tpa_id = tpa.tpa_id
        left join dim_tipo_entrada as tpe on b.tpe_id = tpe.tpe_id
        left join dim_tipo_unidade_entrada as tpu on b.tpu_id = tpu.tpu_id
        left join dim_prioridade as pri on b.pri_id = pri.pri_id
        left join dim_risco as ris on b.ris_id = ris.ris_id
        left join dim_faixa_etaria as fae on b.fae_id = fae.fae_id
        where b.boletim_gid is not null
    )

select *
from final
