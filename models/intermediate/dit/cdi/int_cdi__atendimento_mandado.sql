{{
    config(
        schema = 'intermediario_cdi',
        alias = 'atendimento_mandado',  
        materialized = 'table'
    )
}}

with base as (

    select *
    from {{ ref('raw_cdi__atendimento_mandado') }}

),

base_tratada as (

    select
        * except(pacientes_novos, tipo_de_documento, tipo_de_solicitacao, direcionamento_interno, sem_deferimento_ou_extinto, sexo, estadual_federal, advogado_ou_defensoria, multas, responsavel_pela_saida_juridico),

        case 
            when tipo_de_documento is null or trim(tipo_de_documento) = '' then 'Sem informação'
            when trim(upper(tipo_de_documento)) like '%BUSCA%' then 'Mandado de Busca e Apreensão'
            when trim(upper(tipo_de_documento)) like '%CITAÇ%' or trim(upper(tipo_de_documento)) like '%CITAC%' then 'Ofício com Mandado de Citação'
            when trim(upper(tipo_de_documento)) like '%INTIMAÇ%' or trim(upper(tipo_de_documento)) like '%INTIMAC%' then 'Mandado de Intimação'
            when trim(upper(tipo_de_documento)) like '%MANDADO%' then 'Mandado'
            when trim(upper(tipo_de_documento)) like '%OFÍCIO%' or trim(upper(tipo_de_documento)) like '%OFICIO%' then 'Ofício'
            else trim(tipo_de_documento)
        end as tipo_de_documento,

        case 
            when tipo_de_solicitacao is null or trim(tipo_de_solicitacao) = '' then 'Sem informação'
            when trim(upper(tipo_de_solicitacao)) like '%BOMBA%' then 'Bomba de Insulina/Insumos'
            when trim(upper(tipo_de_solicitacao)) = 'INSUMO' or trim(upper(tipo_de_solicitacao)) = 'INSUMOS' then 'Insumo'
            when trim(upper(tipo_de_solicitacao)) like '%MEDICAMENTO/INSUMO%' then 'Medicamento/Insumo'
            when trim(upper(tipo_de_solicitacao)) like '%OXIFGEN%' or trim(upper(tipo_de_solicitacao)) like '%HIPERBARICA%' then 'Oxigenoterapia Hiperbárica'
            when trim(upper(tipo_de_solicitacao)) like '%DOMICLIAR%' or trim(upper(tipo_de_solicitacao)) = 'OXIGENOTERAPIA DOMICILIAR' then 'Oxigenoterapia Domiciliar'
            when trim(upper(tipo_de_solicitacao)) like '%DOMICILIAR + PADI%' then 'Oxigenoterapia Domiciliar + PADI'
            else trim(tipo_de_solicitacao)
        end as tipo_de_solicitacao,

        case
            when trim(upper(pacientes_novos)) = 'X' then 'Sim'
            else 'Não'
        end as pacientes_novos,

        case 
            when direcionamento_interno is null or trim(direcionamento_interno) = '' then 'Sem informação'
            when trim(upper(direcionamento_interno)) in ('INSUMO', 'INSUMOS') then 'Insumos'
            when trim(upper(direcionamento_interno)) like '%FARMACEUTICO%' or trim(upper(direcionamento_interno)) like '%FARMACIA%' then 'Farmacêutico'
            when trim(upper(direcionamento_interno)) like '%FATURAMENTO/JURID%' or trim(upper(direcionamento_interno)) like '%FATURAMENTO/JURÍD%' then 'Faturamento/Jurídico'
            when trim(upper(direcionamento_interno)) = 'FATURAMENTO' then 'Faturamento'
            when trim(upper(direcionamento_interno)) like '%INSUMOS/FARMAC%' then 'Insumos/Farmacêutico'
            when trim(upper(direcionamento_interno)) like '%INSUMOS/JURID%' or trim(upper(direcionamento_interno)) like '%INSUMOS/JURÍD%' then 'Insumos/Jurídico'
            when trim(upper(direcionamento_interno)) like '%JURIDICO%' or trim(upper(direcionamento_interno)) like '%JURÍDICO%' then 'Jurídico'
            else trim(direcionamento_interno)
        end as direcionamento_interno,

        case 
            when sem_deferimento_ou_extinto is null or trim(sem_deferimento_ou_extinto) = '' then 'Sem informação'
            else trim(upper(sem_deferimento_ou_extinto))
        end as sem_deferimento_ou_extinto,

        case 
            when trim(upper(sexo)) in ('MASCULINO', 'MASCULNO') then 'Masculino'
            when trim(upper(sexo)) = 'FEMININO' then 'Feminino'
            else 'Sem informação'
        end as sexo,

        case 
            when estadual_federal is null or trim(estadual_federal) = '' then 'Sem informação'
            else trim(estadual_federal)
        end as estadual_federal,

        case 
            when advogado_ou_defensoria is null or trim(advogado_ou_defensoria) = '' then 'Sem informação'
            when trim(upper(advogado_ou_defensoria)) like '%DEFENS%' then 'Defensoria Pública'
            when trim(upper(advogado_ou_defensoria)) like '%ADVOG%' or trim(upper(advogado_ou_defensoria)) like '%PARTIC%' then 'Advogado Particular'
            when trim(upper(advogado_ou_defensoria)) like '%AGUARDANDO%' then 'Aguardando Correção'
            when trim(upper(advogado_ou_defensoria)) like '%SEGREDO%' then 'Segredo de Justiça'
            else trim(advogado_ou_defensoria)
        end as advogado_ou_defensoria,

        case 
            when trim(upper(multas)) like '%MULTA%' then 'Sim'
            else 'Não'
        end as multas,

        case 
            when responsavel_pela_saida_juridico is null or trim(responsavel_pela_saida_juridico) = '' then 'Sem informação'
            when trim(upper(responsavel_pela_saida_juridico)) like 'FLAVI%' then 'Flávia'
            when trim(upper(responsavel_pela_saida_juridico)) like 'VALERI%' then 'Valéria'
            else trim(responsavel_pela_saida_juridico)
        end as responsavel_pela_saida_juridico,

        CASE
            WHEN prazos LIKE '%/%'
                THEN PARSE_DATE('%d/%m/%Y', TRIM(REGEXP_EXTRACT(prazos, r'(\d{2}/\d{2}/\d{4})')))
            WHEN TRIM(UPPER(prazos)) = 'IMEDIATO'                THEN data_entrada
            WHEN TRIM(UPPER(prazos)) = 'URGENTE'                 THEN DATE_ADD(data_entrada, INTERVAL 1 DAY)
            WHEN TRIM(UPPER(prazos)) = '48 HORAS'                THEN DATE_ADD(data_entrada, INTERVAL 2 DAY)
            WHEN TRIM(UPPER(prazos)) = '72 HORAS'                THEN DATE_ADD(data_entrada, INTERVAL 3 DAY)
            WHEN TRIM(UPPER(prazos)) = 'MAIS BREVE POSSÍVEL'     THEN DATE_ADD(data_entrada, INTERVAL 3 DAY)
            WHEN TRIM(UPPER(prazos)) IN ('PRESENCIAL', 'PRSENCIAL', 'MULTA')
                                                                  THEN NULL
            ELSE NULL
        END AS prazo_limite

    from base

)

select * from base_tratada