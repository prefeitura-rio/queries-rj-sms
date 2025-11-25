{{
    config(
        schema="brutos_sisvisa",
        alias="banca_jornal_sisvisa",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sisvisa_staging", "BancaJornal") }}
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
            -- IDENTIFICAÇÃO DA BANCA
            -- =====================================
            t.Id                                          as id,
            t.Ativo                                       as ativo,
            t.TipoLicenca                                 as tipo_licenca,
            t.Complexidade                                as complexidade,
            {{ process_null('t.NumeroProcesso') }}        as numero_processo,
            {{ process_null('t.NumeroLicenca') }}         as numero_licenca,
            {{ process_null('t.InscricaoMunicipal') }}    as inscricao_municipal,
            {{ process_null('t.NumeroDeAutenticacao') }}  as numero_de_autenticacao,
            t.AutorizacaoId                               as autorizacao_id,
            t.requerimentoId                              as requerimento_id,
            t.JustificativaId                             as justificativa_id,
            t.AtividadeId                                 as atividade_id,
            {{ process_null('t.Operacao') }}              as operacao,
            {{ process_null('t.Situacao') }}              as situacao,
            {{ process_null('t.Motivo') }}                as motivo,
            {{ process_null('t.MotivoAlteracao') }}       as motivo_alteracao,

            -- =====================================
            -- TITULAR
            -- =====================================
            {{ process_null('t.NomeTitular') }}           as nome_titular,
            {{ process_null('t.CPFTitular') }}            as cpf_titular,
            {{ process_null('t.EmailTitular') }}          as email_titular,
            {{ process_null('t.CelularTitular') }}        as celular_titular,
            {{ process_null('t.TelefoneTitular') }}       as telefone_titular,
            {{ process_null('t.UFTitular') }}             as uf_titular,
            {{ process_null('t.MunicipioTitular') }}      as municipio_titular,
            {{ process_null('t.CEPTitular') }}            as cep_titular,
            {{ process_null('t.BairroTitular') }}         as bairro_titular,
            {{ process_null('t.LogradouroTitular') }}     as logradouro_titular,
            {{ process_null('t.ComplementoTitular') }}    as complemento_titular,
            {{ process_null('t.NumeroPortaTitular') }}    as numero_porta_titular,

            -- =====================================
            -- LOCALIZAÇÃO DA BANCA
            -- =====================================
            {{ process_null('t.LogradouroBanca') }}       as logradouro_banca,
            {{ process_null('t.CodigoLogradouroBanca') }} as codigo_logradouro_banca,
            {{ process_null('t.NumeroPortaBanca') }}      as numero_porta_banca,
            {{ process_null('t.ComplementoBanca') }}      as complemento_banca,
            {{ process_null('t.BairroBanca') }}           as bairro_banca,
            {{ process_null('t.ReferenciaBanca') }}       as referencia_banca,
            {{ process_null('t.CEPBanca') }}              as cep_banca,
            {{ process_null('t.AreaUtil') }}              as area_util,

            -- =====================================
            -- DIMENSÕES FÍSICAS
            -- =====================================
            {{ process_null('t.DimensaoAlturaBanca_CM') }}   as dimensao_altura_banca_cm,
            {{ process_null('t.DimensaoFrenteBanca_CM') }}   as dimensao_frente_banca_cm,
            {{ process_null('t.DimensaoLateralBanca_CM') }}  as dimensao_lateral_banca_cm,

            -- =====================================
            -- FUNCIONAMENTO
            -- =====================================
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

            -- =====================================
            -- LICENCIAMENTO SANITÁRIO
            -- =====================================
            {{ process_null('t.SituacaoDoAlvara') }}      as situacao_do_alvara,
            t.SituacaoDaEmissaoDaLicenca                 as situacao_da_emissao_da_licenca,
            t.SituacaoDaLicencaSanitaria                 as situacao_da_licenca_sanitaria,
            t.SituacaoValidacaoDaLicencaSanitaria        as situacao_validacao_da_licenca_sanitaria,
            t.TermoDeResponsabilidade                    as termo_de_responsabilidade,
            t.TermoDeCienciaDaLegislacao                 as termo_de_ciencia_da_legislacao,

            -- =====================================
            -- DATAS ADMINISTRATIVAS
            -- =====================================
            t.DataCriacao                                 as data_criacao,
            t.DataInclusao                                as data_inclusao,
            t.DataAlteracao                               as data_alteracao,
            t.DataReativacao                              as data_reativacao,
            t.DataDaOndaParaLicenca                       as data_da_onda_para_licenca,
            t.DataValidade                                as data_validade,
            t.DataRevogacao                               as data_revogacao,
            t.DataCancelamento                            as data_cancelamento,
            t.DataAnulacao                                as data_anulacao,
            t.DataAnulacaoLimite                          as data_anulacao_limite,
            t.DataCassacao                                as data_cassacao,

            -- =====================================
            -- INDICADORES
            -- =====================================
            t.Mei                                         as mei

        from dedup t
    )

select *
from renamed 