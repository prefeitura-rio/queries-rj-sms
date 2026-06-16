{{ config(
    schema='intermediario_cegonha',
    alias='alerta_historico_teste',
    materialized='incremental',
    cluster_by=['fim_datahora', 'registro_identificado_na_maternidade'],
    tags=['cegonha_digital_15min']
) }}

with dados_nascimento_base as (

    -- Base auxiliar para resgatar data de nascimento por CPF.
    select
        cpf,
        safe.parse_date('%Y-%m-%d', cast(data_nascimento as string)) as data_nascimento
    from {{ source('brutos_iplanrio', 'registro_municipal_integrado') }}
    where cpf is not null
      and cpf != ''

),

sisare_flat as (

    -- Expande os telefones da gestante para criar uma chave de telefone usada no match com a base clínica.
    select distinct
        a.nome,
        t.telefone_valido_whatsapp as tel,
        a.desfecho_gestacao,
        a.data_alta_internacao,
        a.data_parto
    from {{ ref('mart_iplanrio__sisare_alta_maternidade') }} a
    cross join unnest(a.telefones_gestante) t
    where a.desfecho_gestacao is not null
      and t.telefone_valido_whatsapp is not null

),

sisare_por_cpf as (

    -- Fallback por CPF quando não houver match por telefone.
    select
        cpf,
        nome,
        desfecho_gestacao,
        data_alta_internacao,
        data_parto
    from {{ ref('mart_iplanrio__sisare_alta_maternidade') }}
    where desfecho_gestacao is not null
      and cpf is not null
      and cpf != ''
    qualify row_number() over (
        partition by cpf
        order by data_alta_internacao desc
    ) = 1

),

base_clinica_unificada as (

    -- Consolida dados clínicos e telefones de paciente/acompanhante em uma chave única de telefone.
    select
        tel_chave,
        s.nome as nome_mat,
        s.cpf as cpf_mat,
        s.nome_acompanhante,
        (
            select array_to_string(
                array_agg(distinct t.telefone_valido_whatsapp),
                ', '
            )
            from unnest(s.telefones_gestante) t
        ) as tels_gestante_str,
        s.telefone_acompanhante.telefone_valido_whatsapp as tel_acompanhante_str,
        sf.desfecho_gestacao,
        sf.data_alta_internacao,
        sf.data_parto
    from {{ ref('mart_iplanrio__siscegonha_agendamento_maternidade') }} s
    cross join unnest(
        array(
            select t.telefone_valido_whatsapp
            from unnest(s.telefones_gestante) t

            union distinct

            select s.telefone_acompanhante.telefone_valido_whatsapp
        )
    ) as tel_chave
    inner join sisare_flat sf
        on s.nome = sf.nome
       and tel_chave = sf.tel
    qualify row_number() over (
        partition by tel_chave
        order by s.data_hora_criacao_agendamento desc
    ) = 1

),

mapa_contatos as (

    -- Deduplica contato para evitar multiplicação de linhas ao explodir mensagens.
    select
        contato.id_contato,
        any_value(contato.contato_nome) as contato_nome,
        any_value(contato.cpf) as cpf_limpo,
        array_to_string(
            array_agg(distinct contato.cpf ignore nulls),
            ', '
        ) as lista_cpfs
    from {{ source('brutos_iplanrio', 'chatbot') }}
    where contato.id_contato is not null
    group by 1

),

mensagens_pre_processadas as (

    -- Expande o array de mensagens e filtra apenas interações dos disparos monitorados.
    select
        c.id_interacao,
        c.hsm.nome_hsm,
        c.contato.id_contato,
        m.contato_nome,
        m.lista_cpfs,
        m.cpf_limpo,
        c.contato.contato_telefone,
        msg.data as datahora_mensagem,
        msg.fonte as mensagem_fonte,
        msg.texto as mensagem_texto,
        c.data_processamento,
        c.fim_datahora
    from {{ source('brutos_iplanrio', 'chatbot') }} as c
    inner join mapa_contatos as m
        on c.contato.id_contato = m.id_contato
    cross join unnest(c.mensagens) as msg
    where c.hsm.nome_hsm in (
        'sms-puerperas-disparo9',
        'sms-puerperas-disparo14-homolog',
        'sms-puerperas-disparo4-v2',
        'sms-puerperas-disparo12',
        'sms-puerperas-disparo14',
        'sms-puerperas-disparo2.2',
        'sms-puerperas-disparo15.1',
        'sms-puerperas-disparo11',
        'sms_puerpera_disp1_acompanhante_v2',
        'sms-puerperas-disparo15',
        'sms-puerperas-disparo2.5',
        'sms_puerpera_disp1_gestante-v2',
        'sms-puerperas-disparo4-v3',
        'sms-puerperas-disparo6'
    )

),

contexto_calculado as (

    -- Recupera a mensagem anterior para classificar respostas do tipo "SIM".
    select
        *,
        lag(mensagem_texto) over (
            partition by id_interacao
            order by datahora_mensagem asc
        ) as texto_anterior
    from mensagens_pre_processadas
    where mensagem_fonte = 'CUSTOMER'
      and mensagem_texto not like '%sms-puerpera%'

),

classificacao_alertas as (

    -- Regras de negócio dos alertas críticos baseados nas mensagens dos usuários e mensagens anteriores.
    select
        *,
        case
            when trim(upper(mensagem_texto)) = 'AJUDA'
              or trim(upper(mensagem_texto)) like '%PASSANDO MAL%'
                then 'CRITICO_PEDIDO_DE_AJUDA'

            when trim(upper(mensagem_texto)) = 'SIM'
             and (
                 trim(upper(texto_anterior)) != 'AJUDA'
                 and trim(upper(texto_anterior)) not like '%PASSANDO MAL%'
                 and trim(upper(texto_anterior)) != 'SIM'
             )
                then case
                    when nome_hsm like '%disparo2%'
                     and (upper(texto_anterior) != 'PARE' or texto_anterior is null)
                        then 'CRITICO_PEDIDO_DE_AJUDA_PÓS_ALTA'

                    when nome_hsm like '%disparo15%'
                     and (
                         upper(texto_anterior) not in (
                             'BEM, APESAR DO CANSAÇO',
                             'FELIZ E COM APOIO'
                         )
                     )
                        then 'CRITICO_NECESSITA_DE_APOIO_PSICOLOGICO'

                    when (
                        nome_hsm like '%disp%'
                        and nome_hsm not like '%disparo6'
                        and nome_hsm not like '%disparo11'
                    )
                     and (upper(texto_anterior) != 'NÃO SENTI NADA DISSO.')
                        then 'CRITICO_SINTOMAS'

                    else null
                end

            else null
        end as tipo_alerta
    from contexto_calculado

),

alertas_rankeados as (

    -- Mantém só o primeiro evento relevante por interação e tipo de alerta.
    select
        *,
        row_number() over (
            partition by id_interacao, tipo_alerta
            order by datahora_mensagem asc
        ) as ranking_alerta
    from classificacao_alertas
    where tipo_alerta is not null

),

final as (

    select
        a.id_interacao,
        a.tipo_alerta,
        a.mensagem_texto as ultima_resposta_usuario,
        a.texto_anterior as penultima_resposta_usuario,
        case
            when trim(upper(a.mensagem_texto)) = 'SIM'
                then a.texto_anterior
            else a.mensagem_texto
        end as motivo,
        a.contato_nome as nome,
        a.contato_telefone as whatsapp,
        dn.data_nascimento,
        coalesce(m.cpf_mat, sp.cpf, a.cpf_limpo) as cpf_paciente,
        coalesce(m.nome_mat, sp.nome) as nome_paciente,
        m.nome_acompanhante,
        m.tels_gestante_str as telefones_paciente,
        m.tel_acompanhante_str as telefone_acompanhante,
        coalesce(m.desfecho_gestacao, sp.desfecho_gestacao) as desfecho_gestacao,
        coalesce(m.data_alta_internacao, sp.data_alta_internacao) as data_alta_internacao,
        coalesce(m.data_parto, sp.data_parto) as data_parto,
        (
            m.tel_chave is not null
            or sp.cpf is not null
        ) as registro_identificado_na_maternidade,
        a.fim_datahora,
        a.id_contato
    from alertas_rankeados as a
    left join base_clinica_unificada as m
        on a.contato_telefone = m.tel_chave
    left join sisare_por_cpf as sp
        on coalesce(m.cpf_mat, a.cpf_limpo) = sp.cpf
    left join dados_nascimento_base as dn
        on coalesce(m.cpf_mat, sp.cpf, a.cpf_limpo) = dn.cpf
    where a.ranking_alerta = 1

    {% if is_incremental() %}
      -- Incremental simples: só traz alertas com fim_datahora maior que o já carregado.
      and a.fim_datahora > (
          select coalesce(max(fim_datahora), datetime('1900-01-01'))
          from {{ this }}
      )
    {% endif %}

)

select *
from final