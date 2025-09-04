{{
    config(
        enabled=true,
        alias="marcos_temporais",
    )
}}

WITH 

    -- ------------------------------------------------------------
    -- Atendimentos durante Gestação
    -- ------------------------------------------------------------
    -- São atendimentos que ocorrem durante a gestação e serão utilizados para detectar os eventos de início e fim de gestação.
    -- ------------------------------------------------------------
    condicoes_relacionadas_gestacao AS (
        SELECT 
            id_prontuario_global,
            cod_cid10,
            case 
                -- Correção Hardcoded: Antes do ajuste no PEP, as vezes o valor padrão de qualquer CID era N.E, mesmo em Gestação.
                when estado = 'N.E' then 'ATIVO' 
                else estado 
            end as estado,
            data_diagnostico
        FROM {{ ref('raw_prontuario_vitacare_historico__condicao') }}
        WHERE REGEXP_CONTAINS(cod_cid10, r'^(Z321|Z34*|Z35*)$')
    ),
    atendimentos as (
        SELECT 
            id_prontuario_global,
            patient_cpf, 
            coalesce(datahora_fim_atendimento, datahora_inicio_atendimento) as dthr
        FROM {{ ref('raw_prontuario_vitacare_historico__acto') }}
        WHERE {{ validate_cpf("patient_cpf") }}
    ),
    atendimentos_durante_gravidez as (
        SELECT
            patient_cpf as cpf,
            id_prontuario_global as id_atendimento,
            dthr as dthr_atendimento,
            cod_cid10 as cid,
            estado,
            cast(data_diagnostico as date) as data_diagnostico
        FROM atendimentos 
            INNER JOIN condicoes_relacionadas_gestacao using (id_prontuario_global)
        ORDER BY 1, 3
    ),

    -- ------------------------------------------------------------
    -- Detectar Marcos de Início, Fim e Em Acompanhamento de Gestação
    -- ------------------------------------------------------------
    -- São cálculos utilizados para detectar os marcos de início e fim de gestação.
    -- ------------------------------------------------------------
    calculo_shift_de_eventos AS (
        SELECT
            *,
            LAG(data_diagnostico) OVER ( PARTITION BY cpf ORDER BY data_diagnostico) AS data_diagnostico_anterior,
            LAG(estado) OVER ( PARTITION BY cpf ORDER BY data_diagnostico) AS estado_anterior,
        FROM atendimentos_durante_gravidez
        ),
    categorizacao_marcos_temporais as (
        select
            cpf,
            id_atendimento,
            dthr_atendimento,
            cid,
            estado,
            data_diagnostico,
            case
                when (estado = 'RESOLVIDO') then 'Encerramento'
                when (estado_anterior is null) then 'Inicio de Gestação'
                when (DATE_DIFF (data_diagnostico, data_diagnostico_anterior, DAY) >= 60) then 'Inicio de Gestação'
                else 'Em Acompanhamento'
            end as tipo
        from calculo_shift_de_eventos
    )
select
    cpf,
    id_atendimento,
    dthr_atendimento,
    cid,
    estado,
    data_diagnostico,
    tipo,
    'gestacao' as linha_cuidado
from categorizacao_marcos_temporais
where tipo <> 'Em Acompanhamento'
order by 
    cpf desc,
    dthr_atendimento asc