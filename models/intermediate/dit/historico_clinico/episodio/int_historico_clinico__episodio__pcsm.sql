{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_pcsm",
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


with 
  -- =============================
  -- ATENDIMENTOS SIMPLIFICADOS
  -- =============================
  atendimento_simplificado as (
    select
        a.id_atendimento,
        a.id_hci,
        a.id_paciente,
        a.id_unidade_saude,
        a.id_profissional_saude as id_profissional,
        p.numero_cpf_paciente as cpf,
        p.numero_cartao_saude as cns,
        p.nome_paciente as nome,
        ta.classificacao_atendimento as classificacao_atendimento, 
        ta.descricao_tipo_atendimento as subtipo,
        datetime(a.data_entrada_atendimento, parse_time('%H%M', a.hora_entrada_atendimento)) as entrada_datahora, 
        datetime(a.data_saida_atendimento, parse_time('%H%M', a.hora_saida_atendimento)) as saida_datahora,

        -- estabelecimento
        struct (
            u.codigo_nacional_estabelecimento_saude as cnes,
            {{proper_estabelecimento("nome_unidade_saude")}} as nome,
            e.tipo_sms as estabelecimento_tipo
        ) as estabelecimento,

        -- prontuario
        struct (
            cast(a.id_atendimento as string) as id_prontuario_global,
            cast(null as string) as id_prontuario_local,
            'pcsm' as fornecedor
        ) as prontuario,

        -- metadados
        struct (
            cast(a.loaded_at as datetime) as imported_at,
            cast(a.transformed_at as datetime) as updated_at,
            cast(current_timestamp() as datetime) as processed_at
        ) as metadados,
        cast(numero_cpf_paciente as int64) as cpf_particao,
        cast(data_entrada_atendimento as date) as data_particao

    from {{ref('raw_pcsm_atendimentos')}} a 
    left join {{ref('raw_pcsm_pacientes')}} p on a.id_paciente = p.id_paciente
    left join {{ref('raw_pcsm_tipos_atendimentos')}} ta on a.id_tipo_atendimento = ta.id_tipo_atendimento 
    left join {{ref('raw_pcsm_unidades_saude')}} u on a.id_unidade_saude = u.id_unidade_saude
    left join {{ref('dim_estabelecimento')}} e on u.codigo_nacional_estabelecimento_saude = e.id_cnes

  ),
 
  -- Condições  
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
                    cast(data_diagnostico as string) as data_diagnostico
                ) ignore nulls
            ) as condicoes,
        from all_cids
        group by 1
    ),

    -- Desfechos 
    desfechos_atendimentos_simplificados as (
        select 
            id_atendimento,
            id_paciente,
            data_evolucao_paciente as data_desfecho,
            descricao_evolucao_paciente as desfecho
        from {{ref('raw_pcsm_evolucao_paciente')}}
        union all
        select 
            id_atendimento,
            id_paciente,
            data_evolucao_internacao as data_desfecho,
            descricao_evolucao_internacao as desfecho
        from {{ref('raw_pcsm_evolucao_internacao')}}
        union all
        select 
            id_atendimento,
            id_paciente,
            data_evolucao as data_desfecho, 
            descricao_evolucao as defecho
        from {{ref('raw_pcsm_evolucao_ambulatorial')}}
    ),

    -- Profissionais
    profissionais as (
        select 
            id_profissional,
            id_unidade_saude,
            nome as nome_profissional,
            cpf as cpf_profissional,
            cns as cns_profissional,
            sigla_conselho,
            case 
                when sigla_conselho = 'CRM' then 'Médico'
                when sigla_conselho = 'CRP' then 'Psicólogo'
                when sigla_conselho = 'COREN' then 'Enfermeiro'
                when sigla_conselho = 'CRESS' then 'Assistente Social'
                when sigla_conselho = 'CRA' then 'Administrador'
                when sigla_conselho = 'CRN' then 'Nutricionista'
                when sigla_conselho = 'CRF' then 'Farmacêutico'
                when sigla_conselho = 'CRO' then 'Odontólogo'
                when sigla_conselho = 'CREFITO' then 'Fisioterapeuta/Terapeuta Ocupacional'
                when sigla_conselho = 'CREFONO' then 'Fonoaudiólogo'
                when sigla_conselho = 'CREF' then 'Educador Físico'
                when sigla_conselho = 'OBM' then 'Músico'
                else 'Ocupação Desconhecida'
            end as especialidade_profissional
        from {{ref('raw_pcsm_profissionais')}}
    ),

    simplificado_final as (
        select
            id_hci,
            cpf,
            a.id_paciente,
            cns,
            case
                when subtipo in ('Acolhimento Diurno', 'Acolhimento em 3º turno','Evolução de paciente em ACOLHIMENTO(LEITO)', 
                                'Evolução de Paciente em CONVIVÊNCIA / 3° TURNO', 'Acolhimento Noturno', 'Morador de SRT', 
                                'Paciente em UAA', 'Convivência') 
                    then 'Acolhimento e Convivência'
                when subtipo in ('Ações de Reabilitação Psicossocial', 'Acolhimento Inicial', 'Acolhimento Inicial -1º acolh. já realizado RAPS',
                                'Acompanhamento de SRT', 'Apoio a UAA/UAI', 'Atendimento à Crise', 'Atendimento ao Familiar', 'Atendimento Individual', 
                                'Busca Ativa','Coleta de SINAIS VITAIS', 'Consulta de Prof. Nível Superior (Não médico)', 'Sessão de Auriculoterapia', 
                                'Fortalecimento Protagonismo Usuários e Familiares', 'Promoção de Contratualidade no Território', 'Visita Domiciliar', 
                                'Visita Institucional', 'Medicamentos Administração', 'Medicamentos Dispensação', 'Terapia de Reidratação Oral',
                                'Curativo') 
                    then 'Consulta'
                when subtipo in ('Ações de Redução de Danos', 'Atendimento de Grupo', 'Prática expressiva e comunicativa', 'Práticas Corporais') 
                    then 'Atividades e Grupos'
                when subtipo = 'Articulação de Redes' then 'Articulação de Rede'
                when subtipo = 'Encaminhamento' then 'Encaminhamento'
                when subtipo in ('Matriciamento Atenção Básica', 'Matriciamento Urgência / Hospital') then 'Matriciamento'
                else 'Outros'
            end as tipo,
            case 
                when subtipo = 'Matriciamento Atenção Básica' then 'Matriciamento APS'
                when subtipo = 'Matriciamento Urgência / Hospital' then 'Matriciamento Urgência/Hospital'
                when subtipo in ('Acolhimento Inicial', 'Acolhimento Inicial - 1º acolh. já realizado RAPS') then 'Acolhimento Inicial'
                when subtipo in ('Atendimento Individual', 'Consulta de Prof. Nível Superior (Não médico)') then 'Atendimento Clínico'
                when subtipo = 'Coleta de SINAIS VITAIS' then 'Coleta de Sinais Vitais'
                when subtipo = 'Fortalecimento Protagonismo Usuários e Familiares' then 'Fortalecimento da Autonomia'
                when subtipo = 'Promoção de Contratualidade no Território' then 'Contratualidade no Território'
                when subtipo = 'Sessão de Auriculoterapia' then 'Auriculoterapia'
                when subtipo = 'Ações de Redução de Danos' then 'Redução de Danos'
                when subtipo = 'Atendimento de Grupo' then 'Grupo Terapêutico'
                when subtipo = 'Prática expressiva e comunicativa' then 'Prática Expressivas'
                when subtipo = 'Articulação de Redes' then 'Articulação de Redes'
                when subtipo = 'Evolução de paciente em ACOLHIMENTO(LEITO)' then 'Acolhimento em Leito'
                when subtipo = 'Evolução de Paciente em CONVIVÊNCIA / 3° TURNO' then 'Acolhimento em 3º turno'
                when subtipo = 'Morador de SRT' then 'Moradia SRT - Registro'
                when subtipo = 'Paciente em UAA' then 'Registro UAA'
                else subtipo
            end as subtipo,
            entrada_datahora,
            saida_datahora,
            {{remove_html('desfecho')}} as desfecho_atendimento,
            cg.condicoes,
            estabelecimento,
            struct(
                cast(pr.id_profissional as string) as id,
                pr.cpf_profissional as cpf,
                pr.cns_profissional as cns,
                initcap(pr.nome_profissional) as nome,
                pr.especialidade_profissional as especialidade
            ) as profissional_saude_responsavel,
            prontuario,
            metadados,
            data_particao,
            cpf_particao
        from atendimento_simplificado a
        left join cid_grouped cg on a.id_paciente = cg.id_paciente
        left join desfechos_atendimentos_simplificados d 
            on a.id_atendimento = d.id_atendimento 
            and date(a.entrada_datahora) = date(d.data_desfecho)
            and a.id_paciente = d.id_paciente
        left join profissionais pr on  a.id_profissional = pr.id_profissional and a.id_unidade_saude = pr.id_unidade_saude
        where a.classificacao_atendimento = 'CA'
    ),


  -- =============================
  -- ATENDIMENTOS AMBULATORIAIS
  -- =============================

    atendimento_ambulatorial as (
        select 
            a.id_hci, 
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

    map_uso_continuo as (
    -- Etapa 1: Resolve a lógica pesada (string matching) apenas uma vez por nome de medicamento
    select
        pm.nome_medicamento,
        logical_or(c.medicamento is not null) as uso_continuo
    from {{ref('raw_prescricao_medicamentos')}} pm
    left join {{ref('raw_sheets__medicamentos_uso_continuo')}} c 
        on strpos(upper(pm.nome_medicamento), upper(c.medicamento)) > 0
    group by 1
    ),

    prescricoes_ambulatorial as (
        select distinct
            pp.id_prescricao,
            id_atendimento, 
            id_medicamento,
            pm.nome_medicamento, 
            via_administracao,
            dose_administrada,
            intervalo_doses,
            observacao_administracao,
            coalesce(map.uso_continuo, false) as uso_continuo
        from {{ref('raw_prescricao_prescricoes')}} pp 
        left join {{ref('raw_prescricao_medicamentos')}} pm 
            on cast(pm.id_prescricao as int64) = pp.id_prescricao
        left join map_uso_continuo map 
                on pm.nome_medicamento = map.nome_medicamento
    ),

    prescricoes_ambulatorial_agg as  (
        select
            id_atendimento,
            array_agg(
                struct(
                    cast(id_prescricao as string) as id,
                    upper(nome_medicamento) as nome,
                    cast(null as string) as concentracao, -- informação contida no nome do medicamento
                    cast(uso_continuo as string) as uso_continuo
                ) ignore nulls
            ) as prescricoes
        from prescricoes_ambulatorial pa
        group by 1
    ),

    episodio_ambulatorial as (
        select 
            id_hci,
            p.numero_cpf_paciente as cpf,
            id_paciente_local as id_paciente,
            'Ambulatorial' as tipo,
            'Prescrição de medicamento' as subtipo, -- TODO
            a.entrada_datahora,
            cg.condicoes,
            pa.prescricoes,
            struct(
                a.cnes,
                a.estabelecimento_nome as nome,
                a.estabelecimento_tipo as tipo 
            ) as estabelecimento,
            -- profissional
            struct(
                cast(a.crm as string) as id,
                cast(a.medico_cpf as string) as cpf,
                cast(null as string) as cns,
                a.nome_medico as nome,
                cast(null as string) as especialidade
            ) as profissional_saude_responsavel,
            -- prontuario
            struct(
                cast(a.id_atendimento as string) as id_prontuario_global,
                cast(null as string) as id_prontuario_local,
                'pcsm' as fornecedor
            ) as prontuario,
            -- metadados
            struct(
                cast(a.loaded_at as datetime) as imported_at,
                cast(a.transformed_at as datetime) as updated_at,
                cast(current_timestamp() as datetime) as processed_at
            ) as metadados
        from atendimento_ambulatorial a
        left join cid_grouped cg on a.id_paciente_global = cg.id_paciente
        left join prescricoes_ambulatorial_agg pa on a.id_atendimento = pa.id_atendimento
        left join {{ref('raw_pcsm_pacientes')}} p on a.id_paciente_global = p.id_paciente
    ), 

merge_final as ( 
    select 
        id_hci,
        cpf,
        id_paciente,
        tipo,
        subtipo,
        safe_cast(entrada_datahora as date) as entrada_datahora,
        safe_cast(saida_datahora as date) as saida_datahora,
        desfecho_atendimento,
        condicoes,
        array<struct<id string, nome string, concentracao string, uso_continuo string>>[] as prescricoes,
        estabelecimento,
        profissional_saude_responsavel,
        prontuario,
        metadados
    from simplificado_final
    union all
    select 
        id_hci,
        cpf,
        id_paciente,
        tipo,
        subtipo,
        safe_cast(entrada_datahora as date) as entrada_datahora,
        safe_cast(null as date) as saida_datahora,
        cast(null as string) as desfecho_atendimento, -- TODO
        condicoes,
        prescricoes,
        estabelecimento,
        profissional_saude_responsavel,
        prontuario,
        metadados
    from episodio_ambulatorial
)


select 
    *,
    cast(cpf as int64) as cpf_particao,
    cast(entrada_datahora as date) as data_particao
from merge_final