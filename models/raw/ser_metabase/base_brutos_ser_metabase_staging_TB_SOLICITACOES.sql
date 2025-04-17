with source as (
        select * from {{ source('brutos_ser_metabase_staging', 'TB_SOLICITACOES') }}
  ),
  renamed as (
      select
          {{ adapter.quote("data_solicitacao") }},
        {{ adapter.quote("paciente_nome") }},
        {{ adapter.quote("cns") }},
        {{ adapter.quote("paciente_datanacimento") }},
        {{ adapter.quote("municipio_paciente") }},
        {{ adapter.quote("tipo_de_leito") }},
        {{ adapter.quote("municipio_unidade_origem") }},
        {{ adapter.quote("municipio_unidade_executante") }},
        {{ adapter.quote("dt_reserva") }},
        {{ adapter.quote("estadosolicitacao") }},
        {{ adapter.quote("data_evento_desistencia") }},
        {{ adapter.quote("solicitacao_id") }},
        {{ adapter.quote("unidade_origem") }},
        {{ adapter.quote("cnes_unidade_origem") }},
        {{ adapter.quote("procedimento") }},
        {{ adapter.quote("especialidade") }},
        {{ adapter.quote("numero_cid") }},
        {{ adapter.quote("carater_internacao") }},
        {{ adapter.quote("tipointernacao") }},
        {{ adapter.quote("unidade_executante") }},
        {{ adapter.quote("cnes_unidade_executante") }},
        {{ adapter.quote("motivo_cancelamento_solicitacao") }},
        {{ adapter.quote("infarto_agudo") }},
        {{ adapter.quote("nacaojudicial") }},
        {{ adapter.quote("central_regulacao") }},
        {{ adapter.quote("data_extracao") }},
        {{ adapter.quote("ano_particao") }},
        {{ adapter.quote("mes_particao") }},
        {{ adapter.quote("data_particao") }}

      from source
  )
  select * from renamed
    