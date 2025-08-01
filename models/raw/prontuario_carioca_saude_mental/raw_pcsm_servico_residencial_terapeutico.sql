{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="servico_residencial_terapeutico",
        materialized="table",
        tags=["raw", "pcsm", "servico_residencial_terapeutico"],
        description="Locais onde são oferecidos o Serviço Residencial Terapêutico a disposição da prefeitura do Rio de Janeiro. Um SRT é uma residência onde são tratados pacientes de saúde mental."
    )
}}

select
    safe_cast(seqsrt as int64) as id_servico_residencial,
    safe_cast({{ process_null('apsrt') }} as string) as area_programatica_servico,
    safe_cast(sequs as int64) as id_unidade_saude,
    safe_cast({{ process_null('idsegmento') }} as string) as id_segmento_servico,
    safe_cast(srtend as string) as endereco_servico,
    safe_cast(srtbairro as string) as bairro_servico,
    safe_cast({{ process_null('tphabil') }} as string) as tipo_habilitacao_servico,
    safe_cast(sthabil as string) as servico_habilitado,
    safe_cast(tpatual as string) as tipo_habilitacao_servico_atual,
    safe_cast(dt_implant as date) as data_implantacao_servico,
    safe_cast(dt_habil as date) as data_habilitacao_servico,
    safe_cast(codsrt as int64) as codigo_servico,
    safe_cast({{ process_null('porthabil') }} as string) as portaria_habilitacao_servico,
    safe_cast(srttiposrv as string) as tipo_servico_srt,
    case trim(safe_cast(srttiposrv as string))
        when 'P' then 'SRT'
        when 'A' then 'UAA'
        when 'I' then 'UAI'
        when 'R' then 'República'        
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Tipo de serviço não classificado'
    end as descricao_tipo_servico,
    safe_cast({{ process_null('srtnome') }} as string) as nome_servico,
    safe_cast(numleitos as int64) as numero_leitos_servico,
    safe_cast(numleitosprev as int64) as numero_leitos_previstos,
    safe_cast(numleitbloq as int64) as numero_leitos_bloqueados,
    safe_cast(numleitosindic as int64) as numero_leitos_indicados,
    safe_cast({{ process_null('dscpacindic') }} as string) as detalhes_pacientes_indicados,
    safe_cast({{ process_null('numcepender') }} as string) as cep_endereco_srt,
    safe_cast({{ process_null('numend') }} as string) as numero_porta_endereco,
    safe_cast({{ process_null('complend') }} as string) as complemento_endereco,
    safe_cast({{ process_null('muniend') }} as string) as municipio_endereco,
    safe_cast({{ process_null('sigufender') }} as string) as uf_endereco,
    safe_cast(codsrtant as int64) as codigo_anterior_servico,
    safe_cast(indativo as string) as status_ativo,
    case trim(safe_cast(indativo as string))
        when 'N' then 'Inativo'
        when 'S' then 'Ativo'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Status não classificado'
    end as descricao_status_ativo,
    safe_cast(dt_inativa as date) as data_inativacao,
    safe_cast(seqprofacomp as int64) as id_profissional_acompanhante,
    safe_cast(seqprofcoord as int64) as id_profissional_coordenador,
    safe_cast({{ process_null('numms') }} as string) as codigo_cnes,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_srt') }}