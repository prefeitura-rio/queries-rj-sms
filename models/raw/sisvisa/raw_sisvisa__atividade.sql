{{
    config(
        schema="brutos_sisvisa",
        alias="atividade",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sisvisa_staging", "Atividade") }}
    ),

    dedup as (
        select
            *
        from source
        qualify
            row_number() over (
                partition by Id
                order by _airbyte_extracted_at desc
            ) = 1
    ),

    renamed as (
        select

            -- 1. IDENTIFICAÇÃO PRINCIPAL
            Id                                   as id,
            {{ process_null('t.Codigo') }}       as codigo,
            {{ process_null('t.Descricao') }}    as descricao,
            t.Grupo                              as grupo,
            t.SegmentoId                          as segmento_id,
            t.LicenciamentoId                     as licenciamento_id,
            t.TipoDeAtividade                     as tipo_de_atividade,
            t.ComplexidadeREPA                    as complexidade_repa,
            t.ComplexidadeParaAbate               as complexidade_para_abate,
            t.FatorMultiplicador                  as fator_multiplicador,
            t.Fator                              as fator,

            -- 2. INDICADORES SANITÁRIOS / CLASSIFICAÇÕES
            t.RiscoId                             as risco_id,
            t.RiscoREPA                           as risco_repa,
            t.RiscoParaAbate                      as risco_para_abate,
            t.Internacao                          as internacao,
            t.ComInternacao                       as com_internacao,
            t.ProcedimentoFarmacia                as procedimento_farmacia,
            t.ProcedimentoInvasivo                as procedimento_invasivo,

            -- 3. TIPOS DE SERVIÇOS E ATIVIDADES
            t.Afe                                 as afe,
            t.Asp                                 as asp,
            t.Mei                                 as mei,
            t.Repa                                as repa,
            t.Inspecao                            as inspecao,
            t.Regulada                            as regulada,
            t.Industria                           as industria,
            t.Autosservico                        as autosservico,
            t.RealizaAbate                        as realiza_abate,
            t.TipoDeAtividadeParaAbate            as tipo_de_atividade_para_abate,

            -- 4. ATIVIDADES RELACIONADAS A SAÚDE
            t.EspecialidadeMedica                 as especialidade_medica,
            t.ServicoAmbulanciaDistribuidora      as servico_ambulancia_distribuidora,

            -- 5. FARMÁCIA E MANIPULAÇÃO
            t.TipoSegmentoFarmaciaManipulacao      as tipo_segmento_farmacia_manipulacao,

            -- 6. SUPERMERCADOS E ALIMENTAÇÃO
            t.Supermercado                        as supermercado,
            t.SupermercadoObrigatorio             as supermercado_obrigatorio,
            t.SupermercadoOpcional                as supermercado_opcional,

            -- 7. VEÍCULOS / LOGÍSTICA
            t.TerceirizaVeiculo                   as terceiriza_veiculo,
            t.QuantidadeVeiculos                  as quantidade_veiculos,

            -- 8. ATRIBUTOS COMPLEMENTARES
            {{ process_null('t.ComplementoAtividade') }} as complemento_atividade,
            t.NecessitaComplemento                as necessita_complemento,
            t.Verificada                          as verificada,
            t.ExigeResponsavelTecnico             as exige_responsavel_tecnico,

            -- 9. DATAS
            t.DtCadastro                          as data_cadastro,
            t.DtAtualizacao                       as data_atualizacao,
            t.DtDaValidadeDaLicenca               as data_validade_licenca,

            -- 10. OUTROS
            t.Ativo                               as ativo

        from dedup t
    )

select *
from renamed