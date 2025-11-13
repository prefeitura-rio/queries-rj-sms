{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_pcsm",
        materialized="table" 
    )
}}


with 
-- ATENDIMENTOS SIMPLIFICADOS
  atendimento_simplificado as (
    select
        a.id_hci,
        a.id_paciente,
        p.numero_cpf_paciente as cpf,
        p.numero_cartao_saude as cns,
        p.nome_paciente as nome,
        --ta.descricao_classificacao_atendimento as tipo,
        ta.descricao_tipo_atendimento as subtipo,
        datetime(a.data_entrada_atendimento, parse_time('%H%M', a.hora_entrada_atendimento)) as entrada_datahora, 
        datetime(a.data_saida_atendimento, parse_time('%H%M', a.hora_saida_atendimento)) as saida_datahora,

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
            id_paciente, 
            upper(codigo_cid10_primario) as id_cid,
            c.categoria.descricao as cid_nome,
            data_cid10_secundario as data_diagnostico
        from {{ref('raw_pcsm_pacientes')}} p
        left join {{ ref('dim_condicao_cid10') }} c on p.codigo_cid10_primario = c.categoria.id
        where codigo_cid10_primario is not null and length(codigo_cid10_primario) = 3

        union all

        select distinct 
            id_paciente, 
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
            id_paciente,
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

    simplificado_final as (
        select
            cpf,
            a.id_paciente,
            cns,
            'Simplificado' as tipo,
            subtipo,
            entrada_datahora,
            saida_datahora,
            cg.condicoes,
            estabelecimento,
            prontuario,
            metadados,
            data_particao,
            cpf_particao
        from atendimento_simplificado a
        left join cid_grouped cg on a.id_paciente = cg.id_paciente
        
    ),


-- ANTEDIMENTOS AMBULATORIAIS 
    atendimento_ambulatorial as (
        select 
            null as id_hci, -- TODO
            null as id_paciente, -- TODO
            a.id_atendimento, 
            a.id_paciente as id_paciente_local,
            p.registro_prontuario as id_paciente_global,
            a.data_atendimento as entrada_datahora,
            a.id_unidade_saude,
            u.cnes_unidade_saude as cnes, 
            u.nome_unidade_saude as estabelecimento_nome,
            e.tipo_sms as estabelecimento_tipo,
            m.numero_crm as crm,
            m.nome_medico, 
            m.cpf_medico as medico_cpf,
            a.loaded_at,
            a.transformed_at
        from {{ref('raw_prescricao_atendimentos')}} a 
        left join {{ref('raw_prescricao_pacientes')}} p on a.id_paciente = p.id_paciente
        left join {{ref('raw_prescricao_unidades_saude')}} u on a.id_unidade_saude = u.id_unidade_saude
        left join {{ref('dim_estabelecimento')}} e on u.cnes_unidade_saude = e.id_cnes
        left join {{ref('raw_prescricao_medicos')}} m on a.conselho_regional_medicina = m.numero_crm
    ),

    prescricoes_ambulatorial as (
        select 
            pp.id_prescricao
            id_atendimento, 
            id_medicamento,
            pm.nome_medicamento, 
            via_administracao,
            dose_administrada,
            intervalo_doses,
            observacao_administracao
        from {{ref('raw_prescricao_prescricoes')}} pp 
        left join {{ref('raw_prescricao_medicamentos')}} pm on cast(pm.id_prescricao as int64) = pp.id_prescricao
    ),

    prescricoes_ambulatorial_agg as  (
        select
            id_atendimento,
            array_agg(
                struct(
                    id_medicamento,
                    nome_medicamento,
                    via_administracao,
                    dose_administrada,
                    intervalo_doses,
                    observacao_administracao
                ) ignore nulls
            ) as prescricoes
        from prescricoes_ambulatorial
        group by 1
    ),



    episodio_ambulatorial as (
        select 
            a.id_paciente_global as id_paciente,
            'Ambulatorial' as tipo,
            '' as subtipo,
            a.entrada_datahora,
            null as saida_datahora,
            null as motivo_atendimento, -- Tabela de evoluções
            null as desfecho_atendimento,
            cg.condicoes,
            pa.prescricoes,
            -- medicamentos_administrados
                -- nome
                -- quantidade
                -- unidade_medida
                -- uso
                -- via_administracao
                -- prescricao_data
            struct(
                a.cnes,
                a.estabelecimento_nome as nome,
                a.estabelecimento_tipo as tipo 
            ) as estabelecimento,
            -- profissional
            struct(
                a.crm as id,
                a.medico_cpf as cpf,
                a.nome_medico as nome,
                null as cns,
                null as especialidade
            ) as profissional,
            -- prontuario
            struct(
                a.id_atendimento as id_prontuario_global,
                null as id_prontuario_local,
                'pcsm' as fornecedor
            ) as prontuario,

            struct(
                a.loaded_at as imported_at,
                a.transformed_at as updated_at,
                current_timestamp() as processed_at
            ) as metadados
        from atendimento_ambulatorial a
        left join cid_grouped cg on a.id_paciente_global = cg.id_paciente
        left join prescricoes_ambulatorial_agg pa on a.id_atendimento = pa.id_atendimento
    ) 
select * from episodio_ambulatorial


