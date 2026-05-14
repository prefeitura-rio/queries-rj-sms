-- Procedimentos do SISREG (regulação ambulatorial municipal) considerados
-- de interesse para o monitoramento de câncer de mama. Cada linha mapeia
-- um id_procedimento do SISREG ao nome amigável e aos critérios clínicos
-- (suspeita / diagnóstico).
-- Usado por int_monitora_cancer__sisreg como fonte de verdade para
-- inclusão e classificação dos eventos.

select
    *,
    limite_dias_solicitacao_autorizacao + limite_dias_autorizacao_execucao as limite_dias_regulacao
from unnest ([
    struct(
        703716 as id_procedimento,
        'Mamografia de Rastreio' as procedimento,
        false as criterio_suspeita,
        false as criterio_diagnostico,
        0 as limite_dias_solicitacao_autorizacao,
        50 as limite_dias_autorizacao_execucao
    ),
    struct(2018735, 'Mamografia Diagnóstica', true, false, 5, 15),
    struct(701867, 'Consulta em Mastologia', false, false, 15, 45),
    struct(2300036, 'Consulta em Mastologia', false, false, 15, 45),
    struct(3100093, 'RNM de Mamas', false, false, 5, 5),
    struct(3105274, 'RNM de Mama Esquerda', false, false, 5, 5),
    struct(3105275, 'RNM de Mama Direita', false, false, 5, 5),
    struct(1407035, 'USG de Mamas', false, false, 15, 45),
    struct(1670021, 'USG de Mamas', false, false, 15, 45),
    struct(228009, 'USG de Mamas', false, false, 15, 45),
    struct(225039, 'USG de Mamas', false, false, 15, 45),
    struct(820029, 'USG de Mamas - para Biopsia', true, false, 5, 15),
    struct(2018205, 'Biópsia', true, false, 5, 15),
    struct(816013, 'Biópsia - USG', true, false, 5, 15),
    struct(820058, 'Biópsia - MMG', true, false, 5, 15)
])
