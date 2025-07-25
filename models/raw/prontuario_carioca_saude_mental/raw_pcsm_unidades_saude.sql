{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="unidades_saude",
        materialized="table",
        tags=["raw", "pcsm", "unidades_saude"],
        description="Unidades de saúde da prefeitura do Rio de Janeiro."
    )
}}

select
    safe_cast(sequs as int64) as id_unidade_saude,
    safe_cast(dscus as string) as nome_unidade_saude,
    safe_cast(dscender as string) as endereco_unidade_saude,
    safe_cast(siguf as string) as unidade_federativa,
    safe_cast(dsccidade as string) as cidade_unidade_saude,
    safe_cast(dscbairro as string) as bairro_unidade_saude,
    safe_cast(codcep as string) as cep_unidade_saude,
    safe_cast(codcnpj as string) as cnpj_unidade_saude,
    safe_cast(codcnes as string) as codigo_nacional_estabelecimento_saude,
    safe_cast({{ process_null('dsctel') }} as string) as telefone_unidade_saude,
    safe_cast(indpernoite as string) as pernoite_unidade_saude,
    case trim(safe_cast(indpernoite as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_pernoite_unidade_saude,
    safe_cast(ind24h as string) as atendimento24h_unidade_saude,
    case trim(safe_cast(ind24h as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_atendimento24h_unidade_saude,
    safe_cast(indleitnum as string) as leito_numerado_unidade_saude,
    case trim(safe_cast(indleitnum as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_leito_numerado_unidade_saude,
    safe_cast(indacolhinotur as string) as acolhimento_noturno_unidade_saude,
    case trim(safe_cast(indacolhinotur as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_acolhimento_noturno_unidade_saude,
    safe_cast(indutilproced as string) as utilizacao_procedimento,
    case trim(safe_cast(indutilproced as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_utilizacao_procedimento,
    safe_cast(indatend8as5 as string) as atendimento8as5h_unidade_saude,
    case trim(safe_cast(indatend8as5 as string))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_atendimento8as5h_unidade_saude,
    safe_cast(seqtipous as int64) as id_tipo_unidade_saude,
    safe_cast(esfera as string) as tipo_esfera_administrativa,
    safe_cast(apus as string) as area_progamatica_servico,
    safe_cast(indinativo as string) as status_registro, 
    case trim(safe_cast(indinativo as string))
        when 'N' then 'Ativo'
        when 'S' then 'Inativo'
        when '' then 'Ativo'
        when null then 'Ativo'
        else 'Não classificado'
    end as descricao_status_registro,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_us') }}