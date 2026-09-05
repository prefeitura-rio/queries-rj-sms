{{
    config(
        alias="fat_prescricao",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_prescricao as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_prescricao") }}
    ),

    int_paciente_rede as (
        select *
        from {{ ref("int_prontuario_vitai_dtw__fat_paciente_rede") }}
    ),

    int_profissional as (
        select prf_id, nome, cpf, cns, numero_conselho, uf_conselho
        from {{ ref("int_prontuario_vitai_dtw__fat_profissional") }}
    ),

    final as (
        select
            -- Chave primária
            p.prescricao_gid,

            -- Chaves estrangeiras
            p.boletim_gid,
            p.fat_paciente_rede_id,
            p.estabelecimento_gid,
            p.prf_id,
            p.tpr_id,

            -- Dados da prescrição
            upper(trim(p.pre_urgencia)) as pre_urgencia,
            upper(trim(p.pre_rotinaenfermagem)) as pre_rotinaenfermagem,

            -- Dados do profissional
            prf.nome as profissional_nome,
            prf.cpf as profissional_cpf,
            prf.cns as profissional_cns,
            prf.numero_conselho as profissional_numero_conselho,
            prf.uf_conselho as profissional_uf_conselho,

            -- Dados do paciente
            p.paciente_gid,
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
            p.data_prescricao,

            -- Metadados
            p.created_at,
            p.updated_at,
            p.loaded_at,
            p.data_particao
        from raw_prescricao as p
        left join int_paciente_rede as pac on p.fat_paciente_rede_id = pac.fat_paciente_rede_id
        left join int_profissional as prf on p.prf_id = prf.prf_id
        where p.prescricao_gid is not null
    )

select *
from final
