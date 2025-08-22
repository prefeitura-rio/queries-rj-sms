{{
    config(
        schema="brutos_prontuario_carioca_saude_mental_prescricao",
        alias="prescricoes",
        materialized="table",
        tags=["raw", "pcsm", "prescricoes"],
        description="Ordens médicas para tratamento, medicamentos ou procedimentos, emitidas pelos profissionais de saúde nos hospitais e clínicas da rede municipal do Rio de Janeiro."
    )
}}

select
    safe_cast(id_prescricao as int64) as id_prescricao,                           -- Identificador único para cada prescrição médica
    safe_cast(id_atendimento as int64) as id_atendimento,                         -- Identificador do atendimento ao qual esta prescrição está vinculada
    safe_cast(impressao_farmacia as int64) as impressao_farmacia,                 -- Indica se a prescrição foi impressa para a farmácia (1=sim, 0=não)
    case trim(safe_cast(impressao_farmacia as string))
        when '1' then 'Sim'
        when '0' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_impressao_farmacia,                                         -- Descrição da impressão da prescrição para a farmácia
    safe_cast(impressao_nutricao as int64) as impressao_nutricao,                 -- Indica se a prescrição foi impressa para a nutrição (1=sim, 0=não)
    case trim(safe_cast(impressao_nutricao as string))
        when '1' then 'Sim'
        when '0' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_impressao_nutricao,                                         -- Descrição da impressão da prescrição para a nutrição
    safe_cast(status_impressao as int64) as status_impressao,                     -- Status geral da impressão da prescrição
    case trim(safe_cast(status_impressao as string))
        when '1' then 'Pronto para imprimir'
        when '0' then 'Não pronto para imprimir'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_status_impressao,                                           -- Descrição do status da impressão da prescrição
    safe_cast(aprasado as int64) as prescricao_aprazada,                          -- Indica se a prescrição foi aprazada/programada (1=sim, 0=não)
    case trim(safe_cast(aprasado as string))
        when '1' then 'Sim'
        when '0' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_prescricao_aprazada,                                        -- Descrição da prescrição aprazada
    safe_cast(etiqueta_aprasar_impressa as int64) as etiqueta_aprazada_impressa,  -- Indica se a etiqueta de aprazamento foi impressa (1=sim, 0=não)
    case trim(safe_cast(etiqueta_aprasar_impressa as string))
        when '1' then 'Sim'
        when '0' then 'Não'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_etiqueta_aprazada_impressa,                                  -- Descrição da impressão da etiqueta de aprazamento
    safe_cast(ativo_aprasamento as int64) as aprazamento_ativo,                   -- Indica se o aprazamento da prescrição está ativo (1=ativo, 0=inativo)
    case trim(safe_cast(ativo_aprasamento as string))
        when '1' then 'Aprazado'
        when '0' then 'Inativo'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_aprazamento_ativo,                                          -- Descrição do status do aprazamento da prescrição
    safe_cast(status_aprasado as int64) as status_aprazado,                       -- Status atual do aprazamento da prescrição (1=aprazado, 0=pendente)
    case trim(safe_cast(status_aprasado as string))
        when '1' then 'Aprazado'
        when '0' then 'Pendente'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_status_aprazado,                                            -- Descrição do status do aprazamento da prescrição
    safe_cast(indtipreceita as string) as tipo_receita,                           -- Tipo da receita ou prescrição
    (
        select array_agg(
        case
            {% for tipo, descricao in {
                '1': 'Receituário comum',
                '2': 'Receituário Controlado',
                '3': 'Receituário com Imagens',
                '4': 'Receituário Excepcional'
            }.items() %}
            when t = '{{ tipo }}' then '{{ descricao }}'
            {% endfor %}
            else 'Desconhecido'
        end
        )
        from unnest(split(safe_cast(indtipreceita as string), ',')) as t
    ) as descricao_tipo_receita,                                                  -- Descrição do tipo de receita associada à prescrição
    safe_cast(numvalidreceit as int64) as validade_receita,                       -- Número de dias de validade da receita
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_prescricao_staging', 'fa_prescricao') }}
