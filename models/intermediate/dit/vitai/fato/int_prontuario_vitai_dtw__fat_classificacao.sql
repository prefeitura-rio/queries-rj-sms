{{
    config(
        alias="fat_classificacao",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_classificacao as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_classificacao") }}
    ),

    int_paciente_rede as (
        select *
        from {{ ref("int_prontuario_vitai_dtw__fat_paciente_rede") }}
    ),

    dim_risco as (
        select ris_id, ris_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_risco") }}
    ),

    final as (
        select
            -- Chave primária
            c.classificacao_gid,

            -- Chaves estrangeiras
            c.boletim_gid,
            c.fat_paciente_rede_id,
            c.estabelecimento_gid,
            c.prf_id,
            c.ris_id,

            -- Dados da classificação
            nullif(trim(c.descritor), '') as descritor,
            nullif(trim(c.asv_queixa_principal), '') as asv_queixa_principal,
            upper(trim(c.primeira)) as primeira,
            upper(trim(c.ultima)) as ultima,
            c.meta,

            -- Descrições dimensionais
            ris.ris_descricao,

            -- Dados do paciente
            c.paciente_gid,
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
            c.data_inicio,
            c.data_fim,

            -- Metadados
            c.created_at,
            c.updated_at,
            c.loaded_at,
            c.data_particao
        from raw_classificacao as c
        left join int_paciente_rede as pac on c.fat_paciente_rede_id = pac.fat_paciente_rede_id
        left join dim_risco as ris on c.ris_id = ris.ris_id
        where c.classificacao_gid is not null
    )

select *
from final
