{{
    config(
        schema="projeto_pic",
        alias="paciente_aps",
        materialized="table",
        tags=["daily"],
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222}
        }
    )
}}

with base as (

    select *
    from {{ ref('int_prontuario_vitacare__paciente') }}
    where situacao = 'Ativo'
      and cpf_valido_indicador is true

),

pacientes as (

    select
        cpf,
        cns,
        telefone,
        email,
        obito_indicador,
        cadastro_permanente_indicador,
        data_ultima_atualizacao_cadastral,
        data_atualizacao_vinculo_equipe,
        updated_at_rank,
        row_number() over (
            partition by cpf
            order by
                cast(data_ultima_atualizacao_cadastral as timestamp) desc nulls last,
                cast(data_atualizacao_vinculo_equipe as timestamp) desc nulls last,
                cadastro_permanente_indicador desc,
                updated_at_rank desc
        ) as rn
    from base

),

clinicas_ativas_dedup as (

    select
        cpf,
        id_cnes,
        situacao,
        cadastro_permanente_indicador,
        data_cadastro_inicial,
        data_ultima_atualizacao_cadastral,
        equipe_familia_indicador,
        id_ine,
        data_atualizacao_vinculo_equipe,
        updated_at_rank,
        row_number() over (
            partition by cpf, id_cnes, id_ine
            order by
                cast(data_ultima_atualizacao_cadastral as timestamp) desc nulls last,
                cast(data_atualizacao_vinculo_equipe as timestamp) desc nulls last,
                cadastro_permanente_indicador desc,
                updated_at_rank desc
        ) as rn
    from base

),

clinicas_ativas as (

    select
        cpf,
        array_agg(
            struct(
                id_cnes,
                situacao,
                cadastro_permanente_indicador,
                data_cadastro_inicial,
                data_ultima_atualizacao_cadastral,
                struct(
                    equipe_familia_indicador,
                    id_ine,
                    data_atualizacao_vinculo_equipe
                ) as equipe_familia
            )
            order by
                cast(data_ultima_atualizacao_cadastral as timestamp) desc nulls last,
                cast(data_atualizacao_vinculo_equipe as timestamp) desc nulls last,
                id_cnes,
                id_ine
        ) as clinicas_cadastro_ativo
    from clinicas_ativas_dedup
    where rn = 1
    group by cpf

),

enderecos_dedup as (

    select
        cpf,
        struct(
            endereco_cep as cep,
            endereco_tipo_logradouro as tipo_logradouro,
            endereco_logradouro as logradouro,
            endereco_numero as numero,
            endereco_complemento as complemento,
            endereco_bairro as bairro,
            endereco_municipio as municipio,
            endereco_estado as estado
        ) as endereco,
        row_number() over (
            partition by cpf
            order by
                cast(data_ultima_atualizacao_cadastral as timestamp) desc nulls last,
                cadastro_permanente_indicador desc,
                updated_at_rank desc
        ) as rn
    from base
    where endereco_logradouro is not null
       or endereco_cep is not null
       or endereco_bairro is not null
       or endereco_municipio is not null
       or endereco_estado is not null

),

final as (

    select
        p.cpf,
        p.cns,
        p.obito_indicador,
        struct(
            p.telefone,
            p.email
        ) as dados_contato,
        e.endereco,
        c.clinicas_cadastro_ativo,
        cast(p.cpf as int64) as cpf_particao
    from pacientes p
    left join clinicas_ativas c using (cpf)
    left join enderecos_dedup e
        on p.cpf = e.cpf
       and e.rn = 1
    where p.rn = 1

)

select *
from final