{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_sarah",
        materialized="table",
        unique_key=['id_hci'],
        cluster_by=['id_hci'],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with bruto_atendimento as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "source_id",
                        "paciente_cpf",
                        "atendimento_subtipo",
                        "datahora_entrada",
                        "datahora_saida"
                    ]
                )
            }} as id_hci,
            * 
        from {{ ref('raw_prontuario_sarah__atendimento') }}
    ),

    -- ESTABELECIMENTO
    dim_estabelecimento as (
        select
            id_cnes as pk,
            struct(
                id_cnes,
                {{ proper_estabelecimento("nome_acentuado") }} as nome,
                tipo_sms as estabelecimento_tipo
            ) as estabelecimento
        from {{ ref("dim_estabelecimento") }}
    ),

    -- PROFISSIONAL
    dim_profissional as (
        select
            cns as pk,
            struct(
                id_profissional_sus as id,
                cpf,
                cns,
                {{ proper_br("nome") }} as nome,
                safe_cast(cbo[SAFE_OFFSET(0)].cbo as string) as especialidade
            ) as profissional_saude_responsavel
        from {{ ref("dim_profissional_saude") }}
    ),

    -- CONDICOES
    dim_condicao as (
        select
            id as pk,
            struct(
                id,
                {{ proper_br("descricao") }} as descricao
            ) as condicoes
        from {{ ref("dim_condicao_cid10") }}
    ),

    cid_descricao as (
        select distinct regexp_replace(id, r'\.', '') as id, descricao
        from {{ ref("dim_condicao_cid10") }}
        union all
        select distinct regexp_replace(categoria.id, r'\.', '') as id, categoria.descricao as descricao
        from {{ ref("dim_condicao_cid10") }}
    ),

    condicoes_unificadas as (
        select
            source_id as fk_atendimento, 
            regexp_replace(cid_principal, r'\.', '') as cid_id,
            'ATIVO' as situacao,
            safe_cast(datahora_entrada as date) as data_diagnostico
        from bruto_atendimento 
        where cid_principal is not null

        union all

        select
            source_id as fk_atendimento,
            regexp_replace(cid_secundario, r'\.', '') as cid_id,
            'ATIVO' as situacao,
            safe_cast(datahora_entrada as date) as data_diagnostico
        from bruto_atendimento
        where cid_secundario is not null
    ),


    dim_condicoes_atribuidas as (
        select
            fk_atendimento,
            array_agg(
                struct(
                    c.cid_id as id,
                    d.descricao as descricao,
                    c.situacao as situacao,
                    safe_cast(c.data_diagnostico as string) as data_diagnostico
                )
                order by c.data_diagnostico desc
            ) as condicoes
        from condicoes_unificadas c
        left join cid_descricao d on c.cid_id = d.id
        group by fk_atendimento
    ),

    ----------------------------------------------------------------------------
    -- PRESCRIÇÕES: Unificação das 3 fontes (Simples, Especializado, Controle)
    ----------------------------------------------------------------------------
    prescricoes_unificadas as (
        -- 1. Receituário Simples
        select
            source_id as fk_atendimento,
            upper(medicamento_inscricao) as nome,
            medicamento_adscricao as posologia,
            case 
                when upper(medicamento_subscricao) like '%CONTÍNUO%' then 'true' 
                else 'false' 
            end as uso_continuo
        from {{ ref('raw_prontuario_sarah__receituario_simples') }}

        union all

        -- 2. Receituário Especializado
        select
            source_id as fk_atendimento,
            upper(medicamento_nome) as nome,
            medicamento_posologia as posologia,
            safe_cast(flag_uso_continuo as string) as uso_continuo
        from {{ ref('raw_prontuario_sarah__receituario_especializado') }}

        union all

        -- 3. Receituário Controle Especial
        select
            source_id as fk_atendimento,
            upper(medicamento_inscricao) as nome,
            medicamento_adscricao as posologia,
            case 
                when upper(medicamento_adscricao) like '%CONTÍNUO%' then 'true' 
                else 'false' 
            end as uso_continuo
        from {{ ref('raw_prontuario_sarah__receituario_controle_especial') }}
    ),

    dim_prescricoes_atribuidas as (
        select
            fk_atendimento,
            array_agg(
                struct(
                    safe_cast(null as string) as id,
                    nome,
                    posologia,
                    uso_continuo
                )
            ) as prescricoes
        from prescricoes_unificadas
        group by fk_atendimento
    ),

    fato_atendimento as (
        select 
            atendimento.id_hci,
            atendimento.paciente_cpf as cpf,
            safe_cast(null as string) as gid_paciente,

            -- TIPO E SUBTIPO
            case 
                when atendimento.atendimento_tipo = 'AMBULATORIAL' then 'Consulta'
                else atendimento.atendimento_tipo 
            end as tipo,
            {{ proper_br("atendimento.atendimento_subtipo") }} as subtipo,

            -- ENTRADA E SAIDA
            safe_cast(atendimento.datahora_entrada as datetime) as entrada_datahora,
            safe_cast(atendimento.datahora_saida as datetime) as saida_datahora,

            --- MOTIVO E DESFECHO
            upper(trim(atendimento.historia_doenca_atual)) as motivo_atendimento,
            upper(trim(atendimento.conduta_imediata)) as desfecho_atendimento,

            -- CONDICOES
            dca.condicoes,

            -- MEDIDAS 
            struct(
                safe_cast(null as string) as altura, 
                safe_cast(null as float64) as circunferencia_abdominal,
                safe_cast(null as float64) as frequencia_cardiaca,
                safe_cast(null as float64) as frequencia_respiratoria,
                safe_cast(null as float64) as glicemia,
                safe_cast(null as float64) as hemoglobina_glicada,
                safe_cast(null as float64) as imc,
                safe_cast(null as float64) as peso,
                safe_cast(null as float64) as pressao_sistolica,
                safe_cast(null as float64) as pressao_diastolica,
                safe_cast(null as string) as pulso_ritmo,
                safe_cast(null as float64) as saturacao_oxigenio,
                safe_cast(null as float64) as temperatura
            ) as medidas,

            -- PROCEDIMENTOS / PRESCRICOES
            safe_cast(null as string) as procedimentos_realizados, 
            dpresc.prescricoes,

            -- ESTABELECIMENTO 
            dest.estabelecimento,

            -- PROFISSIONAIS 
            dprof.profissional_saude_responsavel,

            -- PRONTUARIO
            struct(
                concat(atendimento.id_cnes, '.', atendimento.source_id) as id_prontuario_global,
                atendimento.source_id as id_prontuario_local,
                'sarah' as fornecedor
            ) as prontuario,

            -- METADADOS
            struct(
                safe_cast(null as datetime) as imported_at, 
                safe_cast(null as datetime) as updated_at,
                datetime(current_timestamp(),'America/Sao_Paulo') as processed_at
            ) as metadados,

            safe_cast(atendimento.datahora_saida as date) as data_particao,
            safe_cast(regexp_replace(atendimento.paciente_cpf, r'\D', '') as int64) as cpf_particao

        from bruto_atendimento as atendimento
        left join dim_estabelecimento dest 
            on atendimento.id_cnes = dest.pk    
        left join dim_profissional dprof 
            on atendimento.profissional_cns = dprof.pk    
        left join dim_condicoes_atribuidas dca 
            on atendimento.source_id = dca.fk_atendimento
        left join dim_prescricoes_atribuidas dpresc 
            on atendimento.source_id = dpresc.fk_atendimento
    )

select * from fato_atendimento

