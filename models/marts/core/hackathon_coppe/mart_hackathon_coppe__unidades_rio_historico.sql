with

unidades_rio_historico as (
    select
        -- identificacao
        ano_competencia as ano, -- obs: a competencia do cnes equivale ao mes anterior do sisreg (?) confirmar
        mes_competencia as mes, 
        id_cnes as unidade_id_cnes,
        nome_fantasia as unidade_nome,

        -- atributos gerais
        esfera as unidade_esfera,
        tipo_unidade_alternativo as unidade_tipo,
        tipo_unidade_agrupado as unidade_tipo_agrupado,
        tipo_gestao_descr as unidade_gestao,
        natureza_juridica_descr as unidade_nat_jur,
        turno_atendimento as unidade_turno_atendimento,

        -- localizacao
        ap as unidade_area_programatica,
        endereco_bairro as unidade_bairro,
        endereco_latitude as unidade_latitude,
        endereco_longitude as unidade_longitude,

        -- indicadores
        ativa as indicador_unidade_ativa,
        estabelecimento_sms_indicador as indicador_unidade_sms,
        vinculo_sus_indicador as indicador_unidade_sus,
        atendimento_internacao_sus_indicador as indicador_unidade_sus_internacao,
        atendimento_ambulatorial_sus_indicador as indicador_unidade_sus_ambulatorial,
        atendimento_sadt_sus_indicador as indicador_unidade_sus_sadt,
        atendimento_urgencia_sus_indicador as indicador_unidade_sus_urgencia,
        atendimento_vigilancia_sus_indicador as indicador_unidade_sus_vigilancia,
        atendimento_regulacao_sus_indicador as indicador_unidade_sus_regulacao,
        atendimento_outros_sus_indicador as indicador_unidade_sus_outros

    from {{ ref("dim_estabelecimento_sus_rio_historico")}}
    where 1 = 1
        and ano_competencia >= 2022
        and ano_competencia < 2025
)

select * from unidades_rio_historico
