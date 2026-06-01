-- Eventos do RUN ATUAL apenas (último episódio de cuidado de cada paciente),
-- com a data_expected pré-calculada. Fonte única compartilhada pelos 7
-- modelos de critério (gravidade_indicador/criterios/) e pelo agregador
-- int_monitora_cancer__gravidade_instancias (CTE gestantes). Ephemeral: o
-- dbt injeta esta query uma única vez como CTE nos consumidores.

select
    cpf_particao,
    fonte,
    procedimento,
    criterio_diagnostico,
    mama_esquerda_resultado,
    mama_direita_resultado,
    evento_status,
    data_solicitacao,
    data_autorizacao,
    data_execucao,
    data_referencia_evento,
    coalesce(data_execucao, data_autorizacao) as data_expected,
    risco,
    gestante
from {{ ref("int_monitora_cancer__eventos_episodios") }}
qualify run_id = max(run_id) over (partition by cpf_particao)
