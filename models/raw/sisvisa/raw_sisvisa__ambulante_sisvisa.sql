{{
    config(
        schema="brutos_sisvisa",
        alias="ambulante_sisvisa",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sisvisa_staging", "Ambulante") }}
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
            -- ================================
            -- 1. IDENTIFICAÇÃO DO AMBULANTE
            -- ================================
            t.Id                                          as id,
            {{ process_null('t.NumeroProcesso') }}        as numero_processo,
            {{ process_null('t.NumeroLicenca') }}         as numero_licenca,
            t.TipoLicenca                                 as tipo_licenca,
            t.Complexidade                                as complexidade,
            t.SegmentoId                                  as segmento_id,
            {{ process_null('t.InscricaoMunicipal') }}    as inscricao_municipal,
            {{ process_null('t.NumeroDeAutenticacao') }}  as numero_de_autenticacao,
            t.AutorizacaoId                               as autorizacao_id,
            t.RequerimentoId                              as requerimento_id,
            t.JustificativaId                             as justificativa_id,

            -- ================================
            -- 2. IDENTIFICAÇÃO DO TITULAR
            -- ================================
            {{ process_null('t.NomeTitular') }}           as nome_titular,
            {{ process_null('t.EmailTitular') }}          as email_titular,
            {{ process_null('t.CelularTitular') }}        as celular_titular,
            {{ process_null('t.TelefoneTitular') }}       as telefone_titular,

            case when length({{ process_null('t.CpfCnpj') }}) = 11
                then {{ process_null('t.CpfCnpj') }}
            end                                           as cpf,

            case when length({{ process_null('t.CpfCnpj') }}) > 11
                then {{ process_null('t.CpfCnpj') }}
            end                                           as cnpj,

            -- ================================
            -- 3. LOCALIZAÇÃO
            -- ================================
            {{ process_null('t.Logradouro') }}            as logradouro,
            {{ process_null('t.CodigoLogradouro') }}      as codigo_logradouro,
            {{ process_null('t.Numero') }}                as numero,
            {{ process_null('t.Complemento') }}           as complemento,
            {{ process_null('t.Bairro') }}                as bairro,
            {{ process_null('t.CEP') }}                   as cep,
            {{ process_null('t.AreaUtil') }}              as area_util,

            {{ process_null('t.BairroPonto') }}           as bairro_ponto,
            {{ process_null('t.LogradouroPonto') }}       as logradouro_ponto,
            {{ process_null('t.CodigoLogradouroPonto') }} as codigo_logradouro_ponto,
            {{ process_null('t.ComplementoPonto') }}      as complemento_ponto,
            {{ process_null('t.ReferenciaPonto') }}       as referencia_ponto,
            {{ process_null('t.Equipamento') }}           as equipamento,

            -- ================================
            -- 4. FUNCIONAMENTO
            -- ================================
            t.Segunda                                     as segunda,
            t.Terca                                       as terca,
            t.Quarta                                      as quarta,
            t.Quinta                                      as quinta,
            t.Sexta                                       as sexta,
            t.Sabado                                      as sabado,
            t.Domingo                                     as domingo,

            t.Manha                                       as manha,
            t.Tarde                                       as tarde,
            t.Noite                                       as noite,
            t.Feriados                                    as feriados,
            {{ process_null('t.OutrosHorarios') }}        as outros_horarios,
            t.Risco                                       as risco,

            -- ================================
            -- 5. LICENCIAMENTO SANITÁRIO
            -- ================================
            {{ process_null('t.SituacaoDoAlvara') }}      as situacao_do_alvara,
            t.SituacaoDaEmissaoDaLicenca                 as situacao_da_emissao_da_licenca,
            t.SituacaoDaLicencaSanitaria                 as situacao_da_licenca_sanitaria,
            t.SituacaoValidacaoDaLicencaSanitaria        as situacao_validacao_da_licenca_sanitaria,
            t.TermoDeResponsabilidade                    as termo_de_responsabilidade,
            t.TermoDeCienciaDaLegislacao                 as termo_de_ciencia_da_legislacao,

            -- ================================
            -- 6. DATAS ADMINISTRATIVAS
            -- ================================
            t.DataCriacao                                 as data_criacao,
            t.DataInclusao                                as data_inclusao,
            t.DataAlteracao                               as data_alteracao,
            t.DataReativacao                              as data_reativacao,
            t.DataValidade                                as data_validade,
            t.DataRevogacao                               as data_revogacao,
            t.DataCancelamento                            as data_cancelamento,
            t.DataAnulacao                                as data_anulacao,
            t.DataAnulacaoLimite                          as data_anulacao_limite,
            t.DataCassacao                                as data_cassacao,

            -- ================================
            -- 7. INDICADORES SOCIAIS / AGRÍCOLAS
            -- ================================
            t.Mei                                         as mei,
            t.PequenosAgricultores                        as pequenos_agricultores,
            t.ProdutoresQuilombolas                       as produtores_quilombolas,
            t.AgricultoresFamiliares                      as agricultores_familiares,
            t.ProdutoresAgroecologicos                    as produtores_agroecologicos

        from dedup t
    )

select *
from renamed