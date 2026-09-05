{{
    config(
        alias="fat_paciente_rede",
        materialized="table",
        schema="intermediario_prontuario_vitai_dtw",
        tags=["intermediate", "vitai"]
    )
}}

with

    raw_paciente_rede as (
        select *
        from {{ ref("raw_prontuario_vitai_dtw_fat_paciente_rede") }}
    ),

    dim_municipio as (
        select mun_id, mun_descricao, mun_uf
        from {{ ref("raw_prontuario_vitai_dtw_dim_municipio") }}
    ),

    dim_bairro as (
        select bai_id, bai_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_bairro") }}
    ),

    dim_raca as (
        select raca_id, raca_descricao
        from {{ ref("raw_prontuario_vitai_dtw_dim_raca") }}
    ),

    final as (
        select
            -- Chave primária
            p.fat_paciente_rede_id,

            -- Identificadores sensíveis
            nullif(trim(p.cpf), '') as cpf,
            nullif(trim(p.cns), '') as cns,

            -- Dados pessoais
            initcap(trim(p.nome)) as nome,
            initcap(trim(p.nome_alternativo)) as nome_alternativo,
            initcap(trim(p.nomemae)) as nomemae,
            initcap(trim(p.nomepai)) as nomepai,
            p.data_nascimento,
            upper(trim(p.sexo)) as sexo,
            upper(trim(p.transex)) as transex,
            upper(trim(p.naturalidade)) as naturalidade,

            -- Raça/cor
            p.raca_id,
            r.raca_descricao,

            -- Contato
            nullif(trim(p.telefone), '') as telefone,
            nullif(trim(p.celular), '') as celular,
            nullif(trim(p.telefone_extra_um), '') as telefone_extra_um,
            nullif(trim(p.telefone_extra_dois), '') as telefone_extra_dois,
            lower(trim(p.email)) as email,

            -- Endereço
            nullif(trim(p.cep), '') as cep,
            nullif(trim(p.tipologradouro), '') as tipologradouro,
            nullif(trim(p.nomelogradouro), '') as nomelogradouro,
            p.bai_id,
            b.bai_descricao,
            p.mun_id,
            m.mun_descricao,
            m.mun_uf,

            -- Metadados
            p.datahora,
            p.created_at,
            p.updated_at,
            p.loaded_at,
            p.data_particao
        from raw_paciente_rede as p
        left join dim_municipio as m on p.mun_id = m.mun_id
        left join dim_bairro as b on p.bai_id = b.bai_id
        left join dim_raca as r on p.raca_id = r.raca_id
        where p.fat_paciente_rede_id is not null
    )

select *
from final
