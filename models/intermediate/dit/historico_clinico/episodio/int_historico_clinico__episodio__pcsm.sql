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
            {{proper_estabelecimento("nome_unidade_saude")}} as nome,
            u.codigo_nacional_estabelecimento_saude as cnes,
            e.tipo_sms as estabelecimento_tipo
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
    left join {{ref('dim_estabelecimento')}} e on u.codigo_nacional_estabelecimento_saude = e.id_cnes
  ),

-- condicoes
    cid_4_digitos as (
        select distinct 
            id_paciente as id, 
            upper(codigo_cid10_primario) as id_cid,
            c.descricao as cid_nome,
            data_cadastro_paciente as data_diagnostico -- Não temos data do cid10 primario
        from {{ref('raw_pcsm_pacientes')}} p
        left join {{ ref('dim_condicao_cid10') }} c on p.codigo_cid10_primario = c.id
        where codigo_cid10_primario is not null and length(codigo_cid10_primario) = 4
        
        union all
        
        select distinct 
            id_paciente as id, 
            upper(codigo_cid10_secundario) as id_cid,
            c.descricao as cid_nome,
            data_cid10_secundario as data_diagnostico
        from {{ref('raw_pcsm_pacientes')}} p
        left join {{ ref('dim_condicao_cid10') }} c on p.codigo_cid10_secundario = c.id
        where codigo_cid10_secundario is not null and length(codigo_cid10_secundario) = 4
    ),

    cid_3_digitos as (
        select distinct 
            id_paciente as id, 
            upper(codigo_cid10_primario) as id_cid,
            c.categoria.descricao as cid_nome,
            data_cid10_secundario as data_diagnostico
        from {{ref('raw_pcsm_pacientes')}} p
        left join {{ ref('dim_condicao_cid10') }} c on p.codigo_cid10_primario = c.categoria.id
        where codigo_cid10_primario is not null and length(codigo_cid10_primario) = 3

        union all

        select distinct 
            id_paciente as id, 
            upper(codigo_cid10_secundario) as id_cid,
            c.categoria.descricao as cid_nome,
            data_cid10_secundario as data_diagnostico
        from {{ref('raw_pcsm_pacientes')}} p
        left join {{ ref('dim_condicao_cid10') }} c on p.codigo_cid10_secundario = c.categoria.id
        where codigo_cid10_secundario is not null and length(codigo_cid10_secundario) = 3

    ),

    all_cids as (
        select * from cid_3_digitos
        union all
        select * from cid_4_digitos
    ),

    cid_grouped as (
        select
            id,
            array_agg(
                struct(
                    id_cid as id,
                    cid_nome as descricao,
                    "ATIVO" as situacao,
                    data_diagnostico as data_diagnostico
                ) ignore nulls
            ) as condicoes,
        from all_cids
        group by 1
    ),

    final as (
        select
            id_hci,
            id_paciente,
            cpf,
            cns,
            nome,
            tipo, 
            subtipo,
            entrada_datahora,
            saida_datahora,
            cg.condicoes,
            estabelecimento,
            prontuario,
            metadados,
            data_particao,
            cpf_particao
        from atendimento_pcsm a
        left join cid_grouped cg on a.id_paciente = cg.id
        
    )


select * from final


