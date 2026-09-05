{{
    config(
        alias="fat_paciente_regulado",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_paciente_regulado as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_paciente_regulado") }}
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

    dim_secao as (
        select sec_id, secao_gid, sec_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_secao") }}
    ),

    final as (
        select
            -- Chave primária
            pr.paciente_regulado_gid,

            -- Chaves estrangeiras
            pr.boletim_gid,
            pr.fat_paciente_rede_id,
            pr.estabelecimento_gid,
            pr.prf_id,
            pr.esp_id,
            pr.sec_id,

            -- Dados da regulação
            nullif(trim(pr.unidade_destino), '') as unidade_destino,
            nullif(trim(pr.protocolo_regulacao), '') as protocolo_regulacao,
            nullif(trim(pr.sistema_regulacao), '') as sistema_regulacao,

            -- Descrições dimensionais
            esp.esp_descricao,
            sec.secao_gid,
            sec.sec_descricao,

            -- Dados do profissional
            prf.nome as profissional_nome,
            prf.cpf as profissional_cpf,
            prf.cns as profissional_cns,
            prf.numero_conselho as profissional_numero_conselho,
            prf.uf_conselho as profissional_uf_conselho,

            -- Dados do paciente
            pr.paciente_gid,
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
            safe_cast(pr.data_regulacao as datetime) as data_regulacao,
            safe_cast(pr.data_exclusao as datetime) as data_exclusao,
            pr.data_cadastro,

            -- Metadados
            pr.created_at,
            pr.updated_at,
            pr.loaded_at,
            pr.data_particao
        from raw_paciente_regulado as pr
        left join int_paciente_rede as pac on pr.fat_paciente_rede_id = pac.fat_paciente_rede_id
        left join int_profissional as prf on pr.prf_id = prf.prf_id
        left join dim_especialidade as esp on pr.esp_id = esp.esp_id
        left join dim_secao as sec on pr.sec_id = sec.sec_id
        where pr.paciente_regulado_gid is not null
    )

select *
from final
