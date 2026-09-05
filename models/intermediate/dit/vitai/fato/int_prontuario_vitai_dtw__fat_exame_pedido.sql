{{
    config(
        alias="fat_exame_pedido",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_exame_pedido as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_exame_pedido") }}
    ),

    int_paciente_rede as (
        select *
        from {{ ref("int_prontuario_vitai_dtw__fat_paciente_rede") }}
    ),

    int_profissional as (
        select prf_id, nome, cpf, cns, numero_conselho, uf_conselho
        from {{ ref("int_prontuario_vitai_dtw__fat_profissional") }}
    ),

    dim_tipo_exame as (
        select tex_id, tex_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_tipo_exame") }}
    ),

    dim_status_pedido_exame as (
        select spe_id, spe_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_status_pedido_exame") }}
    ),

    final as (
        select
            -- Chaves primárias
            ep.exame_id,
            ep.pedido_id,

            -- Chaves estrangeiras
            ep.boletim_gid,
            ep.fat_paciente_rede_id,
            ep.estabelecimento_gid,
            ep.prf_id,
            ep.tex_id,
            ep.spe_id,

            -- Descrições dimensionais
            tex.tex_descricao,
            spe.spe_descricao,

            -- Dados do profissional
            prf.nome as profissional_nome,
            prf.cpf as profissional_cpf,
            prf.cns as profissional_cns,
            prf.numero_conselho as profissional_numero_conselho,
            prf.uf_conselho as profissional_uf_conselho,

            -- Dados do paciente
            ep.paciente_gid,
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
            ep.data_pedido,

            -- Metadados
            ep.created_at,
            ep.updated_at,
            ep.loaded_at,
            ep.data_particao
        from raw_exame_pedido as ep
        left join int_paciente_rede as pac on ep.fat_paciente_rede_id = pac.fat_paciente_rede_id
        left join int_profissional as prf on ep.prf_id = prf.prf_id
        left join dim_tipo_exame as tex on ep.tex_id = tex.tex_id
        left join dim_status_pedido_exame as spe on ep.spe_id = spe.spe_id
        where ep.exame_id is not null
    )

select *
from final
