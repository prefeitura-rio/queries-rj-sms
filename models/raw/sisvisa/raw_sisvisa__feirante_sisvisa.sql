{{
    config(
        schema="brutos_sisvisa",
        alias="feirante_sisvisa",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sisvisa_staging", "Feirante") }}
    ),

    dedup as (
        select *
        from source
        qualify 
            row_number() over (
                partition by Id
                order by _airbyte_extracted_at desc
            ) = 1
    ),

    renamed as (
        select
            -- =====================================
            -- 1. IDENTIFICAÇÃO DO FEIRANTE / EMPRESA
            -- =====================================
            t.Id                                          as id,
            {{ process_null('t.TipoPessoa') }}            as tipo_pessoa,
            {{ process_null('t.RazaoSocial') }}           as razao_social,
            {{ process_null('t.NumeroProcesso') }}        as numero_processo,
            {{ process_null('t.NumeroLicenca') }}         as numero_licenca,
            {{ process_null('t.NumeroDeAutenticacao') }}  as numero_de_autenticacao,
            {{ process_null('t.InscricaoMunicipal') }}    as inscricao_municipal,
            t.SegmentoId                                  as segmento_id,
            t.TipoLicenca                                 as tipo_licenca,
            t.Complexidade                                as complexidade,

            -- separação CPF/CNPJ
            case when length({{ process_null('t.CpfCnpj') }}) = 11
                then {{ process_null('t.CpfCnpj') }}
            end                                           as cpf,

            case when length({{ process_null('t.CpfCnpj') }}) > 11
                then {{ process_null('t.CpfCnpj') }}
            end                                           as cnpj,

            -- =====================================
            -- 2. DADOS FISCAIS / CLASSIFICAÇÕES DA FEIRA
            -- =====================================
            {{ process_null('t.CodigoAtividade') }}        as codigo_atividade,
            {{ process_null('t.CodigoCategoria') }}        as codigo_categoria,
            {{ process_null('t.DescricaoCategoria') }}     as descricao_categoria,
            {{ process_null('t.CodigoTipoFeira') }}        as codigo_tipo_feira,
            {{ process_null('t.DescricaoTipoFeira') }}     as descricao_tipo_feira,
            {{ process_null('t.CodigoEquipamento') }}      as codigo_equipamento,
            {{ process_null('t.DescricaoEquipamento') }}   as descricao_equipamento,

            -- =====================================
            -- 3. ENDEREÇO
            -- =====================================
            {{ process_null('t.Uf') }}                     as uf,
            {{ process_null('t.Municipio') }}              as municipio,
            {{ process_null('t.Bairro') }}                 as bairro,
            {{ process_null('t.Logradouro') }}             as logradouro,
            {{ process_null('t.NumeroPorta') }}            as numero_porta,
            {{ process_null('t.Complemento') }}            as complemento,
            {{ process_null('t.Cep') }}                    as cep,
            {{ process_null('t.AreaUtil') }}               as area_util,

            -- =====================================
            -- 4. CONTATO
            -- =====================================
            {{ process_null('t.Email') }}                  as email,
            {{ process_null('t.Telefone') }}               as telefone,
            {{ process_null('t.Telefone2') }}              as telefone2,

            {{ process_null('t.Invalido') }}               as invalido,
            {{ process_null('t.IsentoTuap') }}             as isento_tuap,

            -- =====================================
            -- 5. INDICADORES SOCIAIS / AGRÍCOLAS
            -- =====================================
            t.PequenosAgricultores                         as pequenos_agricultores,
            t.ProdutoresQuilombolas                        as produtores_quilombolas,
            t.AgricultoresFamiliares                       as agricultores_familiares,
            t.ProdutoresAgroecologicos                     as produtores_agroecologicos,

            -- =====================================
            -- 6. SITUAÇÃO / FUNCIONAMENTO
            -- =====================================
            t.Risco                                        as risco,
            {{ process_null('t.Motivo') }}                 as motivo,
            {{ process_null('t.Afastado') }}               as afastado,

            -- =====================================
            -- 7. LICENCIAMENTO SANITÁRIO
            -- =====================================
            {{ process_null('t.SituacaoDoAlvara') }}        as situacao_do_alvara,
            t.SituacaoDaEmissaoDaLicenca                   as situacao_da_emissao_da_licenca,
            t.SituacaoDaLicencaSanitaria                   as situacao_da_licenca_sanitaria,
            t.SituacaoValidacaoDaLicencaSanitaria          as situacao_validacao_da_licenca_sanitaria,
            t.TermoDeResponsabilidade                      as termo_de_responsabilidade,
            t.TermoDeCienciaDaLegislacao                   as termo_de_ciencia_da_legislacao,

            -- =====================================
            -- 8. DATAS ADMINISTRATIVAS
            -- =====================================
            t.DataCriacao                                  as data_criacao,
            t.DataInclusao                                 as data_inclusao,
            t.DataAlteracao                                as data_alteracao,
            t.DataReativacao                               as data_reativacao,
            t.DataValidade                                 as data_validade,
            t.DataRevogacao                                as data_revogacao,
            t.DataCancelamento                             as data_cancelamento,
            t.DataAnulacao                                 as data_anulacao,
            t.DataAnulacaoLimite                           as data_anulacao_limite,
            t.DataCassacao                                 as data_cassacao

        from dedup t
    )

select *
from renamed