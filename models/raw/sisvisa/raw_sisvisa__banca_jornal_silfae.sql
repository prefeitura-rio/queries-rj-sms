{{
    config(
        schema="brutos_sisvisa",
        alias="banca_jornal_silfae",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sisvisa_staging", "Vw_BancaJornal_Datalake") }}
    ),

    dedup as (
        select *
        from source
        qualify
            row_number() over (
                partition by cpfTitular, numeroPortaBanca
                order by _airbyte_extracted_at desc
            ) = 1
    ),

    renamed as (
        select
            -- =====================================
            -- TITULAR
            -- =====================================
            {{ process_null('t.nomeTitular') }}             as nome_titular,
            {{ process_null('t.cpfTitular') }}              as cpf_titular,
            case 
                when t.emailTitular = 'a@b.c' then 
                    null 
                else 
                    {{ process_null('t.emailTitular') }} 
            end                                             as email_titular,
            {{ process_null('t.celularTitular') }}          as celular_titular,
            {{ process_null('t.telefoneTitular') }}         as telefone_titular,

            {{ process_null('t.ufTitular') }}               as uf_titular,
            {{ process_null('t.municipioTitular') }}        as municipio_titular,
            {{ process_null('t.bairroTitular') }}           as bairro_titular,
            {{ process_null('t.logradouroTitular') }}       as logradouro_titular,
            {{ process_null('t.complementoTitular') }}      as complemento_titular,
            {{ process_null('t.numeroPortaTitular') }}      as numero_porta_titular,
            {{ process_null('t.cepTitular') }}              as cep_titular,
            {{ process_null('t.codigoLogradouroTitular') }} as codigo_logradouro_titular,

            -- =====================================
            -- BANCA
            -- =====================================
            {{ process_null('t.bairroBanca') }}             as bairro_banca,
            {{ process_null('t.logradouroBanca') }}         as logradouro_banca,
            {{ process_null('t.complementoBanca') }}        as complemento_banca,
            {{ process_null('t.numeroPortaBanca') }}        as numero_porta_banca,
            {{ process_null('t.referenciaBanca') }}         as referencia_banca,
            {{ process_null('t.codigoLogradouroBanca') }}   as codigo_logradouro_banca,

            t.dimensaoAlturaBanca_CM                        as dimensao_altura_banca_cm,
            t.dimensaoFrenteBanca_CM                        as dimensao_frente_banca_cm,
            t.dimensaoLateralBanca_CM                       as dimensao_lateral_banca_cm,

            -- =====================================
            -- IDENTIFICAÇÃO ADMINISTRATIVA
            -- =====================================
            t.autorizacao_id                                as autorizacao_id,
            t.inscricaoMunicipal                            as inscricao_municipal,
            {{ process_null('t.situacao') }}                as situacao

        from dedup t
    )

select *
from renamed