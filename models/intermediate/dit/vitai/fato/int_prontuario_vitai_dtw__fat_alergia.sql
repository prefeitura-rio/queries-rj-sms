{{
    config(
        alias="fat_alergia",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_alergia as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_alergia") }}
    ),

    int_paciente_rede as (
        select *
        from {{ ref("int_prontuario_vitai_dtw__fat_paciente_rede") }}
    ),

    dim_tipo_alergia as (
        select tal_id, tal_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_tipo_alergia") }}
    ),

    dim_severidade as (
        select sev_id, sev_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_severidade") }}
    ),

    final as (
        select
            -- Chave primária
            a.alergia_gid,

            -- Chaves estrangeiras
            a.fat_paciente_rede_id,
            a.boletim_gid,
            a.estabelecimento_gid,
            a.tal_id,
            a.sev_id,

            -- Dados da alergia
            nullif(trim(a.reacao), '') as reacao,
            nullif(trim(a.observacao), '') as observacao,
            upper(trim(a.ativo)) as ativo,

            -- Descrições dimensionais
            tal.tal_descricao,
            sev.sev_descricao,

            -- Dados do paciente
            a.paciente_gid,
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
            a.data_especifica,
            a.data_registro,
            a.data_inclusao,

            -- Metadados
            a.created_at,
            a.updated_at,
            a.loaded_at,
            a.data_particao
        from raw_alergia as a
        left join int_paciente_rede as pac on a.fat_paciente_rede_id = pac.fat_paciente_rede_id
        left join dim_tipo_alergia as tal on a.tal_id = tal.tal_id
        left join dim_severidade as sev on a.sev_id = sev.sev_id
        where a.alergia_gid is not null
    )

select *
from final
