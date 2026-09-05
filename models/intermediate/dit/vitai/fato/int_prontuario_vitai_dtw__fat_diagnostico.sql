{{
    config(
        alias="fat_diagnostico",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_diagnostico as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_diagnostico") }}
    ),

    int_paciente_rede as (
        select *
        from {{ ref("int_prontuario_vitai_dtw__fat_paciente_rede") }}
    ),

    dim_cid as (
        select cid_id, cid_codigo, cid_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_cid") }}
    ),

    dim_tipo_diagnostico as (
        select tpd_id, tpd_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_tipo_diagnostico") }}
    ),

    final as (
        select
            -- Chave primária
            d.diagnostico_gid,

            -- Chaves estrangeiras
            d.boletim_gid,
            d.fat_paciente_rede_id,
            d.estabelecimento_gid,
            d.cid_id,
            d.tpd_id,

            -- Descrições dimensionais
            cid.cid_codigo,
            cid.cid_descricao,
            tpd.tpd_descricao,

            -- Dados do paciente
            d.paciente_gid,
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
            d.data_diagnostico,

            -- Metadados
            d.created_at,
            d.updated_at,
            d.loaded_at,
            d.data_particao
        from raw_diagnostico as d
        left join int_paciente_rede as pac on d.fat_paciente_rede_id = pac.fat_paciente_rede_id
        left join dim_cid as cid on d.cid_id = cid.cid_id
        left join dim_tipo_diagnostico as tpd on d.tpd_id = tpd.tpd_id
        where d.diagnostico_gid is not null
    )

select *
from final
