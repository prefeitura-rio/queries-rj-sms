{{
    config(
        schema="brutos_sisvisa",
        alias="estabelecimento",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sisvisa_staging", "Estabelecimento") }}
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
            -- 1. IDENTIFICAÇÃO PRIMÁRIA
            t.Id                                as id,
            t.Ativo                             as ativo,

            {{process_null('t.RazaoSocial')}} as razao_social,
            {{process_null('t.NomeFantasia')}} as nome_fantasia,
            {{process_null('cast(t.InscricaoMunicipal as string)')}} as inscricao_municipal,
            case when length({{process_null('t.CpfCnpj')}}) > 11 then {{process_null('t.CpfCnpj')}} else null end as cnpj,
            case when length({{process_null('t.CpfCnpj')}}) = 11 then {{process_null('t.CpfCnpj')}} else null end as cpf,

            {{process_null('cast(t.SegmentoId as string)')}} as segmento_id,
            {{process_null('cast(t.Complexidade as string)')}} as complexidade,
            {{process_null('cast(t.TipoLicenca as string)')}} as tipo_licenca,

            -- 4. ENDEREÇO / LOCALIZAÇÃO
            {{process_null('t.Logradouro')}} as logradouro,
            {{process_null('t.TipoLogradouro')}} as tipo_logradouro,
            {{process_null('t.CodigoLogradouro')}} as codigo_logradouro,
            {{process_null('t.Numero')}} as numero,
            {{process_null('t.Complemento')}} as complemento,
            {{process_null('t.Bairro')}} as bairro,
            {{process_null('t.Cidade')}} as cidade,
            cast(nullif(t.CepNumero, 0) as string) as cep_numero,
            cast(nullif(t.CepComplemento, 0) as string) as cep_complemento,
            {{process_null('t.BairroDaLocalizacao')}} as bairro_da_localizacao,
            nullif(t.Latitude, 0) as latitude,
            nullif(t.Longitude, 0) as longitude,

            -- 5. CONTATO GERAL
            {{process_null('t.Email')}} as email,
            {{process_null('t.TelefoneDeContato')}} as telefone_principal,
            {{process_null('t.Telefone2')}} as telefone_secundario,

            -- 7. DADOS DE LICENCIAMENTO SANITÁRIO
            t.SituacaoDoAlvara                  as situacao_do_alvara,
            t.SituacaoDaEmissaoDaLicenca        as situacao_da_emissao_da_licenca,
            t.SituacaoDaLicencaSanitaria        as situacao_da_licenca_sanitaria,
            t.SituacaoValidacaoDaLicencaSanitaria as situacao_validacao_da_licenca_sanitaria,
            t.TermoAceite                       as termo_aceite,
            t.TermoDeResponsabilidade           as termo_de_responsabilidade,
            t.TermoDeCienciaDaLegislacao        as termo_de_ciencia_da_legislacao,
            t.PreenchidoPeloServico             as preenchido_pelo_servico,

        from dedup t
    )

select *
from renamed