-- Procedimentos do SISREG (regulação ambulatorial municipal) considerados
-- de interesse para o monitoramento de câncer de mama. Cada linha mapeia
-- um id_procedimento do SISREG ao nome amigável e aos critérios clínicos
-- (suspeita / diagnóstico).
-- Usado por int_monitora_cancer__sisreg como fonte de verdade para
-- inclusão e classificação dos eventos.

select *
from unnest([
    struct(
        703716 as id_procedimento,
        'Mamografia de Rastreio' as procedimento,
        false as criterio_suspeita,
        false as criterio_diagnostico
    ),
    struct(2018735, 'Mamografia Diagnóstica', true, false),
    struct(701867, 'Consulta em Mastologia', false, false),
    struct(2300036, 'Consulta em Mastologia', false, false),
    struct(3100093, 'RNM de Mamas', false, false),
    struct(3105274, 'RNM de Mama Esquerda', false, false),
    struct(3105275, 'RNM de Mama Direita', false, false),
    struct(1407035, 'USG de Mamas', false, false),
    struct(1670021, 'USG de Mamas', false, false),
    struct(228009, 'USG de Mamas', false, false),
    struct(225039, 'USG de Mamas', false, false),
    struct(820029, 'USG de Mamas - para Biopsia', true, false),
    struct(2018205, 'Biópsia', true, false),
    struct(816013, 'Biópsia - USG', true, false),
    struct(820058, 'Biópsia - MMG', true, false)
])
