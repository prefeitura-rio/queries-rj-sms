{{
    config(
        alias="paciente",
        materialized="table",
    )
}}

-- dbt run --select raw_prontuario_vitacare__paciente
with
    paciente as (
        select *, 'rotineiro' as tipo,
        from {{ ref("base_prontuario_vitacare__paciente_rotineiro") }}
        union all
        select *, 'historico' as tipo,
        from {{ ref("base_prontuario_vitacare__paciente_historico") }}
    ),

    paciente_deduplicado as (
        select *,
        from paciente
        qualify
            row_number() over (
                partition by cnes_unidade, cpf, cns order by updated_at desc
            )
            = 1
    ),

    corrige_cadastro as (
        select

            * except (cadastro_permanente, nome_social, sexo, raca_cor, nome_mae),

            case when nome_social in ('') then null else nome_social end as nome_social,

            case
                when sexo in ("M", "MALE")
                then initcap("MASCULINO")
                when sexo in ("F", "FEMALE")
                then initcap("FEMININO")
                else null
            end as sexo,

            case
                when trim(raca_cor) in ("", "NAO INFORMADO", "SEM INFORMACAO")
                then null
                else initcap(raca_cor)
            end as raca_cor,

            case
                when data_obito is null
                then false
                when data_obito is not null
                then true
                else null
            end as obito_indicador,

            case when nome_mae in ("NONE") then null else nome_mae end as nome_mae,

            case
                when cadastro_permanente = "True" or codigo_ine_equipe_saude is not null
                then true
                when cadastro_permanente = "False"
                then false
                else false
            end as cadastro_permanente_indicador,

            case
                when codigo_ine_equipe_saude is not null then true else false
            end as equipe_familia_indicador,

        from paciente_deduplicado
    ),

    renomeado as (
        select
            {{ remove_accents_upper("id") }} as id_paciente,
            {{ remove_accents_upper("cnes_unidade") }} as id_cnes,
            null as id_prontuario,  # TODO: adicionar o identificador do prontuario
            {{ remove_accents_upper("cpf") }} as cpf,
            {{ validate_cpf(remove_accents_upper("cpf")) }} as cpf_valido_indicador,
            {{ remove_accents_upper("cns") }} as cns,
            {{ remove_accents_upper("nome") }} as nome,
            {{ remove_accents_upper("nome_social") }} as nome_social,
            {{ remove_accents_upper("sexo") }} as genero,
            date(data_nascimento) as data_nascimento,
            obito_indicador,
            date(data_obito) as obito_data,
            {{ remove_accents_upper("orientacao_sexual") }} as orientacao_sexual,
            {{ remove_accents_upper("identidade_genero") }} as identidade_genero,
            {{ remove_accents_upper("nome_mae") }} as mae_nome,
            {{ remove_accents_upper("nome_pai") }} as pai_nome,
            {{ remove_accents_upper("raca_cor") }} as raca,

            {{ remove_accents_upper("telefone") }} as telefone,
            {{ remove_accents_upper("email") }} as email,
            {{ padronize_cep(remove_accents_upper("endereco_cep")) }} as cep,
            {{ remove_accents_upper("endereco_tipo_logradouro") }} as tipo_logradouro,
            {{
                remove_accents_upper(
                    'REGEXP_EXTRACT(endereco_logradouro, r"^(.*?)(?:\d+.*)?$")'
                )
            }} as logradouro,
            {{
                remove_accents_upper(
                    'REGEXP_EXTRACT(endereco_logradouro, r"\b(\d+)\b")'
                )
            }} as numero,
            {{
                remove_accents_upper(
                    'REGEXP_REPLACE(endereco_logradouro, r"^.*?\d+\s*(.*)$", r"\1")'
                )
            }} as complemento,
            {{ remove_accents_upper("endereco_bairro") }} as bairro,
            {{ remove_accents_upper("endereco_municipio") }} as cidade,
            {{ remove_accents_upper("endereco_estado") }} as estado,
            cadastro_permanente_indicador,
            equipe_familia_indicador,
            {{ remove_accents_upper("codigo_ine_equipe_saude") }} as id_ine,
            data_atualizacao_vinculo_equipe,
            data_ultima_atualizacao_cadastral,
            updated_at,

        from corrige_cadastro
    ),

    pacientes_validos as (
        select * from renomeado where cpf is not null and id_cnes is not null
    )

select *
from pacientes_validos
