{{
    config(
        alias="fat_atendimento",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_atendimento as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_atendimento") }}
    ),

    int_paciente_rede as (
        select *
        from {{ ref("int_prontuario_vitai_dtw__fat_paciente_rede") }}
    ),

    dim_especialidade as (
        select esp_id, esp_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_especialidade") }}
    ),

    dim_cid as (
        select cid_id, cid_codigo, cid_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_cid") }}
    ),

    dim_momento_atendimento as (
        select matd_id, matd_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_momento_atendimento") }}
    ),

    final as (
        select
            -- Chave primária
            a.atendimento_gid,

            -- Chaves estrangeiras
            a.boletim_gid,
            a.fat_paciente_rede_id,
            a.estabelecimento_gid,
            a.esp_id,
            a.cid_id,
            a.prf_id,
            a.matd_id,

            -- Dados do atendimento
            nullif(trim(a.queixa), '') as queixa,
            upper(trim(a.primeiro_atendimento)) as primeiro_atendimento,
            upper(trim(a.ultimo_atendimento)) as ultimo_atendimento,

            -- Descrições dimensionais
            esp.esp_descricao,
            cid.cid_codigo,
            cid.cid_descricao,
            matd.matd_descricao,

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
            a.data_inicio,
            a.data_fim,

            -- Metadados
            a.created_at,
            a.updated_at,
            a.loaded_at,
            a.data_particao
        from raw_atendimento as a
        left join int_paciente_rede as pac on a.fat_paciente_rede_id = pac.fat_paciente_rede_id
        left join dim_especialidade as esp on a.esp_id = esp.esp_id
        left join dim_cid as cid on a.cid_id = cid.cid_id
        left join dim_momento_atendimento as matd on a.matd_id = matd.matd_id
        where a.atendimento_gid is not null
    )

select *
from final
