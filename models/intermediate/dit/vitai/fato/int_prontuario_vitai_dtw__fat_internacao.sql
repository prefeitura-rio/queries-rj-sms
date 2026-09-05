{{
    config(
        alias="fat_internacao",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_internacao as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_internacao") }}
    ),

    int_paciente_rede as (
        select *
        from {{ ref("int_prontuario_vitai_dtw__fat_paciente_rede") }}
    ),

    int_profissional as (
        select prf_id, nome, cpf, cns, numero_conselho, uf_conselho
        from {{ ref("int_prontuario_vitai_dtw__fat_profissional") }}
    ),

    dim_especialidade as (
        select esp_id, esp_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_especialidade") }}
    ),

    dim_cid as (
        select cid_id, cid_codigo, cid_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_cid") }}
    ),

    dim_tipo_internacao as (
        select tin_id, tin_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_tipo_internacao") }}
    ),

    dim_procedimento as (
        select prc_id, prc_codigo, prc_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_procedimento") }}
    ),

    final as (
        select
            -- Chave primária
            i.internacao_gid,

            -- Chaves estrangeiras
            i.boletim_gid,
            i.fat_paciente_rede_id,
            i.estabelecimento_gid,
            i.prf_id,
            i.esp_id,
            i.cid_id_internacao,
            i.tin_id,
            i.prc_id,

            -- Descrições dimensionais
            esp.esp_descricao,
            cid.cid_codigo as cid_internacao_codigo,
            cid.cid_descricao as cid_internacao_descricao,
            tin.tin_descricao,
            prc.prc_codigo,
            prc.prc_descricao,

            -- Dados do profissional
            prf.nome as profissional_nome,
            prf.cpf as profissional_cpf,
            prf.cns as profissional_cns,
            prf.numero_conselho as profissional_numero_conselho,
            prf.uf_conselho as profissional_uf_conselho,

            -- Dados do paciente
            i.paciente_gid,
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
            i.data_entrada,
            i.data_saida,

            -- Metadados
            i.created_at,
            i.updated_at,
            i.loaded_at,
            i.data_particao
        from raw_internacao as i
        left join int_paciente_rede as pac on i.fat_paciente_rede_id = pac.fat_paciente_rede_id
        left join int_profissional as prf on i.prf_id = prf.prf_id
        left join dim_especialidade as esp on i.esp_id = esp.esp_id
        left join dim_cid as cid on i.cid_id_internacao = cid.cid_id
        left join dim_tipo_internacao as tin on i.tin_id = tin.tin_id
        left join dim_procedimento as prc on i.prc_id = prc.prc_id
        where i.internacao_gid is not null
    )

select *
from final
