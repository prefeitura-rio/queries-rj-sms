{{ config(
    schema = "projeto_subhue",
    alias = "paciente",
    materialized = "table",
    tags = ['daily']
) }}

with base as (
    select *
    from {{ ref('raw_prontuario_vitacare__paciente') }}

),

paciente as (
    select
        regexp_replace({{ normalize_null('cpf') }}, r'[^0-9]', '') as cpf,

        case
            when regexp_contains(nome, r'^[^A-Za-zÀ-ÖØ-öø-ÿ\s]+$') then null
            when upper(nome) in ('NAO POSSUI', 'NAO INFORMADO', 'SEM INFORMACAO') then null
            else {{ normalize_null('nome') }}
        end as nome,

        case
            when regexp_contains(nome, r'^[^A-Za-zÀ-ÖØ-öø-ÿ\s]+$') then null
            when upper(nome_social) in ('NAO POSSUI', 'NAO INFORMADO', 'SEM INFORMACAO') then null
            else  {{ normalize_null('nome_social') }}
        end as nome_social,

        data_nascimento,

        case
            when upper(sexo) in ('FEMININO', 'FEMALE') then 'Feminino'
            when upper(sexo) in ('MASCULINO', 'MALE') then 'Masculino'
            when upper(sexo) = 'UNKNOWN' then null
            else null
        end as sexo,

        case
            when upper({{ normalize_null('orientacao_sexual') }}) = 'HETEROSSEXUAL' then 'Heterossexual'
            when upper({{ normalize_null('orientacao_sexual') }}) = 'GAY' then 'Gay'
            when upper({{ normalize_null('orientacao_sexual') }}) in ('LA©SBICA', 'LESBICA') then 'Lésbica'
            when upper({{ normalize_null('orientacao_sexual') }}) = 'BISSEXUAL' then 'Bissexual'
            when upper({{ normalize_null('orientacao_sexual') }}) = 'PANSSEXUAL' then 'Pansexual'
            when upper({{ normalize_null('orientacao_sexual') }}) = 'ASSEXUAL' then 'Assexual'
            when upper({{ normalize_null('orientacao_sexual') }}) = 'OUTRO' then 'Outro'
            else null
        end as orientacao_sexual,

        case
            when upper({{ normalize_null('identidade_genero') }}) = 'HOMEM CISGAªNERO' then 'Homem cisgênero'
            when upper({{ normalize_null('identidade_genero') }}) = 'MULHER CISGAªNERO' then 'Mulher cisgênero'
            when upper({{ normalize_null('identidade_genero') }}) = 'HOMEM TRANSGAªNERO' then 'Homem transgênero'
            when upper({{ normalize_null('identidade_genero') }}) = 'MULHER TRANSGAªNERO' then 'Mulher transgênero'

            when upper({{ normalize_null('identidade_genero') }}) = 'CIS' then 'Cisgênero'
            when upper({{ normalize_null('identidade_genero') }}) = 'OUTRO' then 'Outro'

            when upper({{ normalize_null('identidade_genero') }}) in ('NAO', 'HETEROSSEXUAL') then null

            else null
        end as identidade_genero,


        case
            when upper({{ normalize_null('raca') }}) = 'BRANCA' then 'Branca'
            when upper({{ normalize_null('raca') }}) = 'PRETA' then 'Preta'
            when upper({{ normalize_null('raca') }}) = 'PARDA' then 'Parda'
            when upper({{ normalize_null('raca') }}) = 'AMARELA' then 'Amarela'
            when upper({{ normalize_null('raca') }}) = 'INDIGENA' then 'Indígena'
            when upper({{ normalize_null('raca') }}) in ('N/R', 'NAO') then null
            else null
        end as raca,

        regexp_replace({{ normalize_null('cns') }}, r'[^0-9]', '') as cns,

        case
            when regexp_contains(mae_nome, r'^[^A-Za-zÀ-ÖØ-öø-ÿ\s]+$') then null
            when upper(mae_nome) in (
                'NAO POSSUI',
                'NAO INFORMADO',
                'SEM INFORMACAO',
                'SEM INFORMAAAO',
                'DESCONHECIDO',
                'MAE DESCONHECIDA',
                'IGNORADO'
            ) then null
            else {{ normalize_null('mae_nome') }}
        end as mae_nome,


        regexp_replace({{ normalize_null('endereco_cep') }}, r'[^0-9]', '') as endereco_cep,
        {{ normalize_null('endereco_estado') }} as endereco_estado,

        trim(
            regexp_replace(
                {{ normalize_null('endereco_municipio') }},
                r'\s*\[IBGE:\s*\d+\]', 
                ''
            )
        ) as endereco_municipio,

        {{ normalize_null('endereco_bairro') }} as endereco_bairro,
        {{ normalize_null('endereco_logradouro') }} as endereco_logradouro,

        case
            when {{ normalize_null('endereco_numero') }} is null then null
            when upper({{ normalize_null('endereco_numero') }}) in ('S/N', 'SN') then null
            else regexp_replace({{ normalize_null('endereco_numero') }}, r'[^0-9]', '')
        end as endereco_numero,

        {{ normalize_null('endereco_complemento') }} as endereco_complemento
    from base
)

select *
from paciente