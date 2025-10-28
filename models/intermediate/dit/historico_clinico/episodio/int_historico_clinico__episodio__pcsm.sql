{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_pcsm",
        materialized="table" 
    )
}}

with 
  atendimento_pcsm as (
    select
        a.id_hci,
        a.id_paciente,
        p.numero_cpf_paciente as cpf,
        p.numero_cartao_saude as cns,
        p.nome_paciente as nome,

        ta.descricao_classificacao_atendimento as tipo,
        ta.descricao_tipo_atendimento as subtipo,
        datetime(data_entrada_atendimento, parse_time('%H%M', hora_entrada_atendimento)) as entrada_datahora, 
        datetime(data_saida_atendimento, parse_time('%H%M', hora_saida_atendimento)) as saida_datahora,

        -- estabelecimento
        struct (
        u.nome_unidade_saude as nome_estabelecimento,
        u.codigo_nacional_estabelecimento_saude as cnes
        ) as estabelecimento,

        -- prontuario
        struct (
            a.id_atendimento as id_prontuario_global,
            null as id_prontuario_local,
            'pcsm' as fornecedor
        ) as prontuario,

        -- metadados
        struct (
            a.loaded_at as imported_at,
            a.transformed_at as updated_at,
            current_timestamp() as processed_at
        ) as metadados,
        cast(numero_cpf_paciente as int64) as cpf_particao,
        cast(data_entrada_atendimento as date) as data_particao

    from {{ref('raw_pcsm_atendimentos')}} a 
    left join {{ref('raw_pcsm_pacientes')}} p on a.id_paciente = p.id_paciente
    left join {{ref('raw_pcsm_tipos_atendimentos')}} ta on a.id_tipo_atendimento = ta.id_tipo_atendimento
    left join {{ref('raw_pcsm_unidades_saude')}} u on a.id_unidade_saude = u.id_unidade_saude
  )


select * from atendimento_pcsm

