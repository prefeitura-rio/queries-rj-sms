{{
    config(
        materialized = "table",
        alias = "oferta_programada"
    )
}}
with oferta_programada as (
    select 
        id_escala_ambulatorial as escala_id,
        id_estabelecimento_executante as unidade_solicitante_id_cnes,

        to_hex(sha256(cast(profissional_executante_cpf as string))) as profissional_executante_id,

        id_procedimento_interno as procedimento_sisreg_id,
        procedimento_vigencia_inicial_data as escala_vigencia_data_inicial,
        procedimento_vigencia_final_data as escala_vigencia_data_final,
        procedimento_vigencia_ano as ano,
        procedimento_vigencia_mes as mes,
        procedimento_vigencia_data as data,
        procedimento_vigencia_dia_semana as dia_semana,

        vagas_primeira_vez_qtd as vagas_programadas_primeira_vez,
        vagas_reserva_qtd as vagas_programadas_reserva,
        vagas_retorno_qtd as vagas_programadas_retorno,
        vagas_todas_qtd as vagas_programadas_todas,

    from {{source("saude_sisreg","oferta_programada_serie_historica")}}
)

select *
from oferta_programada
where profissional_executante_id is not null
