{{
    config(
        alias="fat_sinais_vitais",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_sinais_vitais as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_sinais_vitais") }}
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
            sv.sinais_vitais_gid,

            -- Chaves estrangeiras
            sv.boletim_gid,
            sv.fat_paciente_rede_id,
            sv.estabelecimento_gid,
            sv.prf_id,

            -- Sinais vitais
            sv.asv_pa_sistolica,
            sv.asv_pa_diastolica,
            sv.asv_freq_cardiaca,
            sv.asv_freq_respiratoria,
            sv.asv_temp_corporal,
            sv.asv_saturacao,
            sv.asv_peso,
            sv.asv_altura,
            sv.asv_hemoglicoteste,
            sv.asv_escala_dor,
            sv.asv_indice_glasgow,
            nullif(trim(sv.cor_pele), '') as cor_pele,
            sv.codigo,

            -- Dados do profissional
            prf.nome as profissional_nome,
            prf.cpf as profissional_cpf,
            prf.cns as profissional_cns,
            prf.numero_conselho as profissional_numero_conselho,
            prf.uf_conselho as profissional_uf_conselho,

            -- Dados do paciente
            sv.paciente_gid,
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
            sv.data_afericao,
            sv.datahora,
            sv.datahora_cadastro,

            -- Metadados
            sv.created_at,
            sv.updated_at,
            sv.loaded_at,
            sv.data_particao
        from raw_sinais_vitais as sv
        left join int_paciente_rede as pac on sv.fat_paciente_rede_id = pac.fat_paciente_rede_id
        left join int_profissional as prf on sv.prf_id = prf.prf_id
        where sv.sinais_vitais_gid is not null
    )

select *
from final
