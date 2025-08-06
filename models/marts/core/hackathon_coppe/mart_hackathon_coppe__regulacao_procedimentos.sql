select
    id_procedimento as procedimento_sisreg_id,
    descricao as procedimento,
    tipo_procedimento as procedimento_tipo,
    especialidade as procedimento_especialidade,
    parametro_consultas_por_hora as vagas_esperadas_hora,
    parametro_reservas as proporcao_esperada_primeira_vez,
    parametro_retornos as proporcao_esperada_retorno,

from {{ref("raw_sheets__assistencial_procedimento")}}
where descricao is not null
