-- Procedimentos do SER (regulação estadual ambulatorial) considerados de
-- interesse para o monitoramento de câncer de mama. Cada linha mapeia um
-- cod_recurso do SER (id_procedimento) ao nome amigável e aos critérios
-- clínicos (suspeita / diagnóstico).
-- Usado por int_monitora_cancer__ser_ambulatorial como fonte de verdade
-- para inclusão e classificação dos eventos.

select *
from unnest([
    struct(
        560 as id_procedimento,
        'Consulta em Mastologia' as procedimento,
        false as criterio_suspeita,
        false as criterio_diagnostico
    ),
    struct(1035, 'Consulta em Mastologia (Oncologia)', false, true),
    struct(1049, 'Consulta em Mastologia (Oncologia)', false, false)
])
