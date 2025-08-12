{{
    config(
        enabled=true,
        alias="agendamentos_sisreg",
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

SELECT
  marcacoes.paciente_cpf as cpf,  
  TO_JSON_STRING(
        struct(
            split(marcacoes.paciente_telefone, ',')[safe_offset(0)] as to_,
            struct(
                marcacoes.paciente_nome as nome,
                format_date('%d/%m/%Y', cast(marcacoes.data_marcacao as date))        as data,
                format_time('%H:%M', time(cast(marcacoes.data_marcacao as datetime))) as hora,
                -- coalesce(marcacoes.procedimento_interno,marcacoes.procedimento_sigtap) as tipo_agendamento,
                marcacoes.vaga_solicitada_tp as tipo_agendamento, 
                 coalesce(
                  initcap(estab.tipo_sms) || ' - ' || initcap(estab.nome_complemento),
                  marcacoes.unidade_executante_nome
                ) as unidade,
                initcap(LOWER(
                  array_to_string(
                      array(
                          select x from unnest([
                              marcacoes.unidade_executante_logradouro,
                              marcacoes.unidade_executante_numero,
                              marcacoes.unidade_executante_complemento,
                              marcacoes.unidade_executante_bairro,
                              marcacoes.unidade_executante_municipio
                          ]) as x
                          where x is not null and x <> ''
                      ),
                      ', '
                ) 
              )) as endereco
                
            ) as vars
        )
    ) as destination_data,
    safe_cast(marcacoes.data_marcacao as date) as data_particao
FROM {{ ref("raw_sisreg_api__marcacoes") }} marcacoes
LEFT JOIN {{ ref('dim_estabelecimento') }} estab
  ON safe_cast(marcacoes.unidade_executante_id as string) = safe_cast(estab.id_cnes as string)
WHERE 
  marcacoes.solicitacao_status in (
        'SOLICITAÇÃO / AGENDADA / COORDENADOR',
        'SOLICITAÇÃO / AGENDADA / SOLICITANTE'
  )
  and cast(marcacoes.data_marcacao as date) >= current_date()
  and marcacoes.paciente_telefone is not null
  and marcacoes.paciente_cpf is not null