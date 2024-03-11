{{
    config(
        alias="sisreg_oferta_programada_por_profissional",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}

with source as (
      select * from {{ source('hackathon_dados_publicos_staging', 'sisreg_oferta_programada_por_profissional') }}
),
renamed as (
    select
        cod_escala_ambulatorial as id_escala_ambulatorial,
        cpf_profissional_exec as profissional_exec_cpf,
        nome_profissional_exec as profissional_exec_nome,
        cod_cnes_exec as id_estabelecimento_exec,
        desc_cnes_exec as estabelecimento_exec_nome,
        cod_procedimento_interno as id_procedimento_interno,
        desc_procedimento_interno as procedimento_interno_descricao,
        dia_semana,
        safe_cast(primeira_vez_dia as int64) as primeira_vez_dia,
        safe_cast(retorno_dia as int64) as retorno_dia,
        safe_cast(reserva_dia as int64) as reserva_dia,
        quebra_automatica,
        agenda_local,
        vigencia_inicial,
        vigencia_final,
        hora_inicial,
        hora_final,
        ultima_alteracao,
        safe_cast(primeira_vez_mensal as int64) as primeira_vez_mensal,
        safe_cast(retorno_mensal as int64) as retorno_mensal,
        safe_cast(reserva_mensal as int64) as reserva_mensal,
        mes_de_competencia_da_oferta,
        ano_de_competencia_da_oferta

    from source
)
select * from renamed
