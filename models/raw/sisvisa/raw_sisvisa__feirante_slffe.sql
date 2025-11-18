{{
    config(
        schema="brutos_sisvisa",
        alias="feirante_slffe",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sisvisa_staging", "Vw_Feirante_Datalake") }}
    ),

    dedup as (
        select *
        from source
        qualify
            row_number() over (
                partition by inscricaoMunicipal, Id_Pessoa
                order by _airbyte_extracted_at desc
            ) = 1
    ),

    renamed as (
        select

            -- =====================================
            -- IDENTIFICAÇÃO DO FEIRANTE
            -- =====================================
            t.Id_Pessoa                                   as id_pessoa,
            {{ process_null('t.nome') }}                  as nome,
            {{ process_null('t.tipoPessoa') }}            as tipo_pessoa,
            {{ process_null('t.inscricaoMunicipal') }}    as inscricao_municipal,

            -- separar cpf e cnpj
            case when length({{ process_null('t.cpfCnpj') }}) = 11
                then {{ process_null('t.cpfCnpj') }}
            end                                           as cpf,

            case when length({{ process_null('t.cpfCnpj') }}) > 11
                then {{ process_null('t.cpfCnpj') }}
            end                                           as cnpj,

            -- =====================================
            -- ENDEREÇO
            -- =====================================
            {{ process_null('t.uf') }}                    as uf,
            {{ process_null('t.municipio') }}             as municipio,
            {{ process_null('t.bairro') }}                as bairro,
            {{ process_null('t.logradouro') }}            as logradouro,
            {{ process_null('t.numeroPorta') }}           as numero_porta,
            {{ process_null('t.complemento') }}           as complemento,
            {{ process_null('t.cep') }}                   as cep,
            {{ process_null('t.codigoLogradouro') }}      as codigo_logradouro,

            -- =====================================
            -- CONTATO E SITUAÇÃO
            -- =====================================
            {{ process_null('t.email') }}                 as email,
            {{ process_null('t.invalido') }}              as invalido,
            {{ process_null('t.afastado') }}              as afastado,
            {{ process_null('t.isentoTuap') }}            as isento_tuap,
            {{ process_null('t.situacao') }}              as situacao,

            -- =====================================
            -- CLASSIFICAÇÃO DA FEIRA
            -- =====================================
            {{ process_null('t.codigoAtividade') }}       as codigo_atividade,
            {{ process_null('t.descricaoAtividade') }}    as descricao_atividade,

            {{ process_null('t.codigoCategoria') }}       as codigo_categoria,
            {{ process_null('t.descricaoCategoria') }}    as descricao_categoria,

            {{ process_null('t.codigoTipoFeira') }}       as codigo_tipo_feira,
            {{ process_null('t.descricaoTipoFeira') }}    as descricao_tipo_feira,

            {{ process_null('t.codigoEquipamento') }}     as codigo_equipamento,
            {{ process_null('t.descricaoEquipamento') }}  as descricao_equipamento

        from dedup t
    )

select *
from renamed