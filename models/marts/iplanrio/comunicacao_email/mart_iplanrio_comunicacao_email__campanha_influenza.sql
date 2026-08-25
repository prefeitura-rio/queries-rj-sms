{{ 
    config(
        materialized = 'table',
        schema = 'projeto_comunicacao_email',
        alias = 'campanha_influenza',
        meta = {"owner": "karen"},

        partition_by = {
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "day"
        },

        cluster_by = [
            "cpf",
            "grupo_elegibilidade",
            "origem_email"
        ]
    ) 
}}

with paciente as (

    select
        cpf,
        dados.nome as nome,
        safe_cast(dados.data_nascimento as date) as data_nascimento,
        contato.email as emails,
        prontuario as prontuarios,
        metadados.processed_at as datahora_atualizacao_cadastro
    from {{ ref('mart_historico_clinico__paciente') }}
    where cpf is not null
      and dados.nome is not null
      and dados.data_nascimento is not null
      and coalesce(dados.obito_indicador, false) is false

),

vacinacao_influenza_2026 as (

    select distinct
        paciente_cpf as cpf
    from {{ ref('mart_cie__vacinacao') }}
    where paciente_cpf is not null

      -- Usa o campo de partição pra otimizar leitura na tabela de vacinação
      and particao_aplicacao_vacinacao >= date('2026-01-01')
      and particao_aplicacao_vacinacao < date('2027-01-01')

      -- Filtra por data de aplicação no ano de 2026.
      and vacina_aplicacao_data >= date('2026-01-01')
      and vacina_aplicacao_data < date('2027-01-01')

      -- Remove registros explicitamente marcados como não aplicada.
      and lower(coalesce(vacina_registro_tipo, '')) != 'nao aplicada'

      -- Identifica vacinas de Influenza/Gripe.
      and regexp_contains(
          lower(coalesce(vacina_descricao, '')),
          r'vacina influenza|influenza trivalente|h1n1|h3n2|gripe a'
      )

      -- Evita falso positivo de Haemophilus influenzae B,
      -- aparece em vacinas pentavalente/tetravalente, mas não é a vacina de Influenza da campanha
      and not regexp_contains(
          lower(coalesce(vacina_descricao, '')),
          r'haemophilus|pentavalente|tetravalente'
      )

),

paciente_prontuario_vitacare as (

    select distinct
        p.cpf,
        safe_cast(prontuario.id_cnes as string) as id_cnes,
        safe_cast(prontuario.id_paciente as string) as id_prontuario_local
    from paciente as p
    cross join unnest(p.prontuarios) as prontuario
    where lower(prontuario.sistema) = 'vitacare'
      and prontuario.id_cnes is not null
      and prontuario.id_paciente is not null

),

condicoes_ativas_episodio as (

    select distinct
        paciente_cpf as cpf,

        -- Normaliza o CID
        -- Exemplo: 'I10.0' vira 'I100'; 'D 80' vira 'D80'.
        regexp_replace(
            upper(trim(condicao.id)),
            r'[^A-Z0-9]',
            ''
        ) as cid
    from {{ ref('mart_historico_clinico__episodio') }}
    cross join unnest(condicoes) as condicao
    where paciente_cpf is not null
      and condicao.id is not null
      and upper(trim(coalesce(condicao.situacao, ''))) = 'ATIVO'
      and regexp_contains(
          regexp_replace(
              upper(trim(condicao.id)),
              r'[^A-Z0-9]',
              ''
          ),
          r'^(I10|I11|I12|I13|I15|E10|E11|E12|E13|E14|D80|D81|D82|D83|D84|D86|Z34|Z35|N17|N18|N19)'
      )

),

condicoes_ativas_vitacare_api as (

    select distinct
        pp.cpf,

        -- Normaliza o CID vindo da API do Vitacare.
        regexp_replace(
            upper(trim(c.cod_cid10)),
            r'[^A-Z0-9]',
            ''
        ) as cid
    from {{ ref('raw_prontuario_vitacare_api__condicao') }} as c
    inner join paciente_prontuario_vitacare as pp
        on safe_cast(c.id_cnes as string) = pp.id_cnes
       and safe_cast(c.id_prontuario_local as string) = pp.id_prontuario_local
    where c.cod_cid10 is not null
      and lower(trim(coalesce(c.estado, ''))) in ('ativo', 'active')
      and regexp_contains(
          regexp_replace(
              upper(trim(c.cod_cid10)),
              r'[^A-Z0-9]',
              ''
          ),
          r'^(I10|I11|I12|I13|I15|E10|E11|E12|E13|E14|D80|D81|D82|D83|D84|D86|Z34|Z35|N17|N18|N19)'
      )

),

condicoes_ativas as (

    select distinct
        cpf,
        cid
    from (
        select
            cpf,
            cid
        from condicoes_ativas_episodio

        union all

        select
            cpf,
            cid
        from condicoes_ativas_vitacare_api
    )

),

grupos_idade as (

    select
        cpf,
        'Idoso' as grupo,
        1 as prioridade
    from paciente
    where
        -- Pessoas com idade maior ou igual a 60 anos na data da extração
        -- Remove datas de nascimento muito antigas possivelmente inválidas
        data_nascimento > date('1900-01-01')
        and data_nascimento <= date_sub(current_date('America/Sao_Paulo'), interval 60 year)

    union all

    select
        cpf,
        'Criança' as grupo,
        2 as prioridade
    from paciente
    where
        -- Crianças com idade entre 0 e 5 anos, 11 meses e 29 dias na data da extração.
        data_nascimento <= current_date('America/Sao_Paulo')
        and data_nascimento > date_sub(current_date('America/Sao_Paulo'), interval 6 year)

),

grupos_cid as (

    select
        cpf,
        grupo,
        prioridade
    from (

        select
            cpf,

            case
                when regexp_contains(cid, r'^I(10|11|12|13|15)')
                    then 'Hipertensão'

                when regexp_contains(cid, r'^E1[0-4]')
                    then 'Diabetes'

                when regexp_contains(cid, r'^D(80|81|82|83|84|86)')
                    then 'Imunossupressão'

                when regexp_contains(cid, r'^Z3[45]')
                    then 'Gestante'

                when regexp_contains(cid, r'^N1[7-9]')
                    then 'Renal crônico'
            end as grupo,

            case
                when regexp_contains(cid, r'^I(10|11|12|13|15)')
                    then 3

                when regexp_contains(cid, r'^E1[0-4]')
                    then 4

                when regexp_contains(cid, r'^D(80|81|82|83|84|86)')
                    then 5

                when regexp_contains(cid, r'^Z3[45]')
                    then 6

                when regexp_contains(cid, r'^N1[7-9]')
                    then 7
            end as prioridade
        from condicoes_ativas

    )
    where grupo is not null

),

grupos_publico_alvo as (

    select
        cpf,
        grupo,
        prioridade
    from grupos_idade

    union all

    select
        cpf,
        grupo,
        prioridade
    from grupos_cid

),

grupos_elegibilidade as (

    select
        cpf,
        string_agg(grupo, '; ' order by prioridade, grupo) as grupo_elegibilidade
    from (
        select distinct
            cpf,
            grupo,
            prioridade
        from grupos_publico_alvo
    )
    group by cpf

),

publico_alvo as (

    select
        p.cpf,
        p.nome,
        p.data_nascimento,
        p.emails,
        p.datahora_atualizacao_cadastro,
        g.grupo_elegibilidade,
        current_date('America/Sao_Paulo') as data_extracao
    from paciente as p
    inner join grupos_elegibilidade as g
        on p.cpf = g.cpf

),

publico_sem_vacina_influenza_2026 as (

    select
        p.*
    from publico_alvo as p
    left join vacinacao_influenza_2026 as v
        on p.cpf = v.cpf
    where v.cpf is null

),

emails_tratados as (

    select
        cpf,
        nome,
        trim(lower(email.valor)) as email_original,
        {{ remove_invalid_email('email.valor') }} as email_tratado,
        grupo_elegibilidade,
        lower(email.sistema) as origem_email,
        email.rank as email_rank,
        data_extracao,
        datahora_atualizacao_cadastro
    from publico_sem_vacina_influenza_2026
    cross join unnest(emails) as email

),

emails_validos as (

    select
        cpf,
        nome,
        email_original,
        email_tratado,
        grupo_elegibilidade,
        origem_email,
        email_rank,
        data_extracao,
        datahora_atualizacao_cadastro
    from emails_tratados
    where email_tratado is not null

),

email_frequencia as (

    select
        email_tratado,
        count(distinct cpf) as qtd_cpfs_por_email
    from emails_validos
    group by email_tratado

),

emails_com_frequencia as (

    select
        e.*
    from emails_validos as e
    left join email_frequencia as f
        on e.email_tratado = f.email_tratado
    where f.qtd_cpfs_por_email < 5

),

deduplicado as (

    select
        cpf,
        nome,
        email_original,
        email_tratado,
        grupo_elegibilidade,
        origem_email,
        data_extracao,
        datahora_atualizacao_cadastro
    from emails_com_frequencia
    qualify row_number() over (
        partition by cpf
        order by
            -- Prioriza e-mails com rank preenchido:
            -- quando o rank é nulo, o e-mail vai para o final da ordenação.
            -- Depois, escolhe o menor rank e usa o próprio e-mail limpo como critério de desempate.
            case when email_rank is null then 1 else 0 end,
            email_rank,
            email_tratado
    ) = 1

)

select
    cpf,
    nome,
    email_original,
    email_tratado,
    grupo_elegibilidade,
    origem_email,
    data_extracao,
    datahora_atualizacao_cadastro
from deduplicado