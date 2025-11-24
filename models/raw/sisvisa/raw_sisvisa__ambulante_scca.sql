{{
    config(
        schema="brutos_sisvisa",
        alias="ambulante_scca",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sisvisa_staging", "Vw_Ambulante_Datalake") }}
    ),

    dedup as (
        select *
        from source
        qualify 
            row_number() over (
                partition by cpfCnpj, numeroPortaPonto, logradouroPonto
                order by _airbyte_extracted_at desc
            ) = 1
    ),

    renamed as (
        select

            -- ===========================
            -- IDENTIFICAÇÃO DO AMBULANTE
            -- ===========================
            {{ process_null('t.nome') }}                 as nome,
            {{ process_null('t.tipoPessoa') }}           as tipo_pessoa,
            {{ process_null('t.status') }}               as status,
            {{ process_null('t.inscricaoMunicipal') }}   as inscricao_municipal,
            {{ process_null('t.permanente') }}           as permanente,
            {{ process_null('t.processaConcessaoIM') }}  as processa_concessao_im,

            -- CPF / CNPJ
            case when length({{ process_null('t.cpfCnpj') }}) = 11
                then {{ process_null('t.cpfCnpj') }}
            end                                         as cpf,

            case when length({{ process_null('t.cpfCnpj') }}) > 11
                then {{ process_null('t.cpfCnpj') }}
            end                                         as cnpj,

            -- ===========================
            -- CONTATO
            -- ===========================
            {{ process_null('t.email') }}               as email,
            {{ process_null('t.celular') }}             as celular,
            {{ process_null('t.telefone') }}            as telefone,

            -- ===========================
            -- ENDEREÇO DO TITULAR
            -- ===========================
            cast(t.CEP as string)                       as cep,
            {{ process_null('t.bairro') }}              as bairro,
            {{ process_null('t.logradouro') }}          as logradouro,
            {{ process_null('t.complemento') }}         as complemento,
            {{ process_null('t.numeroPorta') }}         as numero_porta,
            {{ process_null('t.codigoBairro') }}        as codigo_bairro,
            {{ process_null('t.codigoLogradouro') }}    as codigo_logradouro,

            -- ===========================
            -- LOCALIZAÇÃO DO PONTO
            -- ===========================
            {{ process_null('t.bairroPonto') }}           as bairro_ponto,
            {{ process_null('t.logradouroPonto') }}       as logradouro_ponto,
            {{ process_null('t.complementoPonto') }}      as complemento_ponto,
            {{ process_null('t.numeroPortaPonto') }}      as numero_porta_ponto,
            {{ process_null('t.refernciaPonto') }}        as referencia_ponto,
            {{ process_null('t.equipamento') }}           as equipamento,
            {{ process_null('t.codigoBairroPonto') }}     as codigo_bairro_ponto,
            {{ process_null('t.codigoLogradouroPonto') }} as codigo_logradouro_ponto,

            -- ===========================
            -- HORÁRIOS DO PONTO
            -- ===========================
            {{ process_null('t.horaInicialPonto') }}    as hora_inicial_ponto,
            {{ process_null('t.horaFinalPonto') }}      as hora_final_ponto,

            -- ===========================
            -- IDENTIFICAÇÃO ADMINISTRATIVA
            -- ===========================
            t.id_Autorizacao                            as id_autorizacao

        from dedup t
    )

select *
from renamed