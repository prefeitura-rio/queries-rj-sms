-- Laudos histopatológicos de mama extraídos do SISCAN
{% set data_inicio_monitoramento = "2021-01-01" %}

select
-- pk
    "SISCAN" as sistema_origem,
    "EXAME" as sistema_tipo,
    safe_cast(protocolo_id as int) as id_sistema_origem,

-- paciente
    paciente_cns,
    cast(NULL as int) as paciente_cpf_sisreg,

-- unidades
    unidade_solicitante_id_cnes as id_cnes_unidade_origem,
    unidade_prestadora_id_cnes as id_cnes_unidade_executante,

-- qualificacao
    cast(NULL as string) as cid,
    cast(NULL as string) as evento_status,
    upper(procedimento_cirurgico) as procedimento,

-- datas
    data_solicitacao,
    cast(NULL as date) as data_autorizacao,
    data_realizacao as data_execucao,

-- resultados siscan
    data_liberacao_resultado as data_exame_resultado,
    case
        when lateralidade = "Esquerda" then coalesce(lesao_neoplasico, lesao_benigno)
        else null
    end as mama_esquerda_resultado,

    case
        when lateralidade = "Direita" then coalesce(lesao_neoplasico, lesao_benigno)
        else null
    end as mama_direita_resultado,

-- indicadores
    -- casos benignos (sem neoplasia) não entram na população monitorada;
    -- quando há neoplasia, o evento é direto ao diagnóstico (sem etapa de suspeita)
    false as criterio_suspeita,

    case
        when lesao_neoplasico is not null then true
        else false
    end as criterio_diagnostico,

-- apenas para SER/SISREG
    cast(NULL as int64) as atraso_solicitacao_autorizacao,
    cast(NULL as int64) as atraso_autorizacao_execucao,
    cast(NULL as int64) as atraso_regulacao

from {{ ref("raw_siscan_web__laudos_histo_mama") }}
where 1 = 1
    and data_solicitacao >= "{{ data_inicio_monitoramento }}"
