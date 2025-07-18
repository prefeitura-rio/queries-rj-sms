{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="ciclos_tratamento_pacientes",
        materialized="table",
        tags=["raw", "pcsm", "ciclos", "tratamento", "pacientes"],
        description="Ciclos de pacientes de caps da Prefeitura do Rio de Janeiro. Ciclo é um conjunto de atendimentos feitos em um ambulatório ou em um caps. Não se pode ter mais de um ciclo aberto ao mesmo tempo nem em um ambulatório nem em um CAPS."
    )
}}

select
    safe_cast(seqciclo as int64) as id_ciclo,
    safe_cast(seqpac as int64) as id_paciente,
    safe_cast(sequs as int64) as id_unidade_saude,
    safe_cast(indciclo as string) as tipo_ciclo,
    case trim(safe_cast(indciclo as string))
        when 'C' then 'CAPS'
        when 'D' then 'Deambulatório'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_tipo_ciclo,
    safe_cast(dtentrada as date) as data_inicio_ciclo,
    safe_cast(horentrada as string) as hora_inicio_ciclo,
    safe_cast(dtsaida as date) as data_termino_ciclo,
    safe_cast(horsaida as string) as hora_termino_ciclo,
    safe_cast(indmotivsaida as string) as situacao_paciente_ciclo,
    case trim(safe_cast(ifnull(indmotivsaida,'') as string))
        when 'P' then 'Alta a pedido'
        when 'S' then 'Alta para Atenção Primária'
        when 'C' then 'Alta para CAPS de outro município'
        when 'I' then 'Alta por insucesso de busca ativa'
        when 'M' then 'Alta por melhora'
        when 'U' then 'Alta para outro ponto de atenção'
        when 'A' then 'Busca ativa - ciclo aberto'
        when 'D' then 'Desaparecido'
        when 'T' then 'Mudança para outro município'
        when 'O' then 'Óbito'
        when 'X' then 'Fechado pela unificação'
        when 'Z' then 'Finalização do programa Seguir em Frente'
        when '' then 'Em acompanhamento - ciclo aberto'
        else 'Não classificado'
    end as descricao_situacao_paciente_ciclo,
    safe_cast(seqpacorig as int64) as id_paciente_unificado,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from
    {{ source('brutos_prontuario_carioca_saude_mental_staging','gh_pac_ciclos') }}