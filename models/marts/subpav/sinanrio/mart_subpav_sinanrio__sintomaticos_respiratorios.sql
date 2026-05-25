{{
    config(
        materialized = 'table',
        alias        = "sintomaticos_respiratorios_dia"
    )
}}

{% set tz = 'America/Sao_Paulo' %}
{% set janela_notif_dias = 30 %}
{% set janela_sint_dias = 15 %}
{% set data_ref_var = var('data_ref', none) %}

with

-- =============================
-- Parâmetros padronizados
-- =============================
params as (
    select
        '{{ tz }}' as tz,
        {% if data_ref_var %}
        date('{{ data_ref_var }}') as data_ref,
        timestamp_sub(
            timestamp(date_add(date('{{ data_ref_var }}'), interval 1 day), '{{ tz }}'),
            interval {{ janela_sint_dias }} day
        ) as ts_sint_min,
        timestamp(date_add(date('{{ data_ref_var }}'), interval 1 day), '{{ tz }}') as ts_ref_fim
        {% else %}
        date_sub(current_date('{{ tz }}'), interval 1 day) as data_ref,
        timestamp_sub(
            timestamp_trunc(current_timestamp(), day, '{{ tz }}'),
            interval {{ janela_sint_dias }} day
        ) as ts_sint_min,
        timestamp_trunc(current_timestamp(), day, '{{ tz }}') as ts_ref_fim
        {% endif %}
),

-- =============================
-- Episódios (ontem)
-- =============================
episodios_ontem_base as (
    select
        regexp_replace(paciente_cpf, r'\D', '') as cpf,
        prontuario.fornecedor                  as prontuario_fornecedor,
        condicoes,
        cast(entrada_datahora as timestamp) as entrada_datahora,
        cast(saida_datahora   as timestamp) as saida_datahora,
        estabelecimento,
        profissional_saude_responsavel,
        data_particao
    from {{ ref('mart_historico_clinico__episodio') }}
    cross join params p
    where paciente_cpf is not null
    and safe_cast(data_particao as date) = p.data_ref
),

episodios_filtrados as (
    select e.*
    from episodios_ontem_base e
    cross join params p
    where e.condicoes is not null
        and exists (
            select 1
            from unnest(e.condicoes) cid
            where substr(regexp_replace(upper(cid.id), r'\.', ''), 1, 4)
                    in unnest({{ sinanrio_lista_cids_sintomaticos() }})
                and upper(trim(cid.situacao)) = 'ATIVO'
                and coalesce(
                    date(timestamp(safe_cast(nullif(trim(cid.data_diagnostico), '') as datetime), p.tz), p.tz),
                    date(safe_cast(nullif(trim(cid.data_diagnostico), '') as timestamp), p.tz),
                    safe.parse_date('%Y-%m-%d', nullif(trim(cid.data_diagnostico), '')),
                    safe.parse_date('%d/%m/%Y', nullif(trim(cid.data_diagnostico), '')),
                    safe.parse_date('%d-%m-%Y', nullif(trim(cid.data_diagnostico), ''))
                ) between date_sub(p.data_ref, interval 30 day) and p.data_ref
        )
),

-- =============================
-- SISREG (ontem)
-- =============================
sisreg_solicitacoes_ontem as (
    select
        regexp_replace(paciente_cpf, r'\D', '') as cpf,
        cast(data_solicitacao as timestamp) as entrada_datahora,
        cast(data_solicitacao as timestamp) as saida_datahora,
        'sisreg' as prontuario_fornecedor,

        struct(
            nullif(
                lpad(regexp_replace(cast(id_cnes_unidade_solicitante as string), r'\D', ''), 7, '0'),
                '0000000'
            ) as id_cnes
        ) as estabelecimento,

        struct(
            cast(profissional_solicitante_cpf as string) as cpf,
            struct(
                nullif(
                    lpad(regexp_replace(cast(id_cnes_unidade_solicitante as string), r'\D', ''), 7, '0'),
                    '0000000'
                ) as id_cnes
            ) as estabelecimento,
            nullif(
                lpad(regexp_replace(cast(id_cnes_unidade_solicitante as string), r'\D', ''), 7, '0'),
                '0000000'
            ) as id_cnes
        ) as profissional_saude_responsavel,

        2 as fonte_prioridade

    from {{ ref("mart_sisreg__solicitacoes") }}
    cross join params p

    where date(cast(data_solicitacao as timestamp), p.tz) = p.data_ref
    and data_cancelamento is null
    and regexp_contains(regexp_replace(paciente_cpf, r'\D', ''), r'^\d{11}$')
    and substr(regexp_replace(upper(cid_solicitacao), r'\.', ''), 1, 4)
            in unnest({{ sinanrio_lista_cids_sintomaticos() }})

    qualify row_number() over (
        partition by regexp_replace(paciente_cpf, r'\D', ''), date(cast(data_solicitacao as timestamp), p.tz)
        order by
            data_atualizacao_registro desc,
            data_solicitacao desc,
            safe_cast(id_solicitacao as int64) desc nulls last,
            id_solicitacao desc
    ) = 1
),

-- =============================
-- SISARE (ontem)
-- =============================
sisare_altas_ontem as (

    with base as (
        select
            safe_cast(i.id_internacao as int64) as id_internacao,
            regexp_replace(pac.cpf, r'\D', '') as cpf,

            nullif(
                lpad(regexp_replace(cast(i.unidade_atendimento as string), r'\D', ''), 7, '0'),
                '0000000'
            ) as cnes_atendimento,

            regexp_replace(a.cpf_cadastrante, r'\D', '') as cpf_cadastrante,

            nullif(
                lpad(regexp_replace(cast(a.unidade_aps as string), r'\D', ''), 7, '0'),
                '0000000'
            ) as cnes_aps,

            coalesce(
                timestamp(safe_cast(a.created_at as datetime), p.tz),
                safe_cast(a.created_at as timestamp),

                timestamp(safe_cast(i.dt_saida as datetime), p.tz),
                safe_cast(i.dt_saida as timestamp),

                timestamp(safe_cast(i.dt_entrada as datetime), p.tz),
                safe_cast(i.dt_entrada as timestamp)
            ) as evento_ts,

            coalesce(
                timestamp(safe_cast(a.updated_at as datetime), p.tz),
                safe_cast(a.updated_at as timestamp),

                timestamp(safe_cast(i.updated_at as datetime), p.tz),
                safe_cast(i.updated_at as timestamp),

                safe_cast(i.datalake_loaded_at as timestamp)
            ) as updated_ts,

            i.datalake_loaded_at

        from {{ ref('raw_plataforma_subpav_sisare__internacoes') }} i
        cross join params p
        left join {{ ref('raw_plataforma_subpav_sisare__pacientes') }} pac
            on safe_cast(pac.id_paciente as int64) = safe_cast(i.id_paciente as int64)
        left join {{ ref('raw_plataforma_subpav_sisare__vw_altas') }} a
            on safe_cast(a.id_internacao as int64) = safe_cast(i.id_internacao as int64)
            and (a.status is null or safe_cast(a.status as int64) = 1)
        where regexp_contains(regexp_replace(pac.cpf, r'\D', ''), r'^\d{11}$')
        and (i.status is null or safe_cast(i.status as int64) = 1)
    )

    select
        b.cpf,
        b.evento_ts as entrada_datahora,
        b.evento_ts as saida_datahora,
        'sisare' as prontuario_fornecedor,

        struct(b.cnes_atendimento as id_cnes) as estabelecimento,

        struct(
            nullif(b.cpf_cadastrante, '') as cpf,
            struct(coalesce(nullif(b.cnes_aps, ''), b.cnes_atendimento) as id_cnes) as estabelecimento,
            coalesce(nullif(b.cnes_aps, ''), b.cnes_atendimento) as id_cnes
        ) as profissional_saude_responsavel,

        3 as fonte_prioridade

    from base b
    cross join params p
    where date(b.evento_ts, p.tz) = p.data_ref
    and (
        exists (
            select 1
            from {{ ref('raw_plataforma_subpav_sisare__internacoes_comorbidades') }} c
            where c.id_internacao = b.id_internacao
            and (c.status is null or safe_cast(c.status as int64) = 1)
            and cast(c.id_comorbidade as string) in ('11841','11879','11907')
        )
        or exists (
            select 1
            from {{ ref('raw_plataforma_subpav_sisare__internacoes_diagnosticos') }} d
            where d.id_internacao = b.id_internacao
            and (d.status is null or safe_cast(d.status as int64) = 1)
            and cast(d.id_diagnostico as string) in ('11841','11879','11907')
        )
    )

    qualify row_number() over (
        partition by b.cpf, date(b.evento_ts, p.tz)
        order by b.updated_ts desc, b.id_internacao desc
    ) = 1
),
-- =============================
-- GAL - Positivos e não notificados/ não sintomatico (ontem)
-- =============================
gal_positivos_ontem as (
    select
        regexp_replace(r.paciente_cpf, r'\D', '') as cpf,
        'gal' as prontuario_fornecedor,
        timestamp(r.dt_resultado, p.tz) as entrada_datahora,
        timestamp(r.dt_resultado, p.tz) as saida_datahora,

        struct(
            nullif(
                lpad(regexp_replace(cast(r.cnes as string), r'\D', ''), 7, '0'),
                '0000000'
            ) as id_cnes
        ) as estabelecimento,

        struct(
            cast(null as string) as cpf,
            struct(
                nullif(
                    lpad(regexp_replace(cast(r.cnes as string), r'\D', ''), 7, '0'),
                    '0000000'
                ) as id_cnes
            ) as estabelecimento,
            nullif(
                lpad(regexp_replace(cast(r.cnes as string), r'\D', ''), 7, '0'),
                '0000000'
            ) as id_cnes
        ) as profissional_saude_responsavel,

        4 as fonte_prioridade

    from {{ ref('mart_subpav_sinanrio__resultado_exame') }} r
    cross join params p
    where r.dt_resultado = p.data_ref
        and r.diagnostico = 1
        and regexp_contains(regexp_replace(coalesce(r.paciente_cpf, ''), r'\D', ''), r'^\d{11}$')
        and (
            (r.id_tipo_exame = 1 and r.id_resultado in (1, 4, 5))  -- baciloscopia positiva
            or (r.id_tipo_exame = 2 and r.id_resultado in (1, 2))     -- TRM positivo
            or (r.id_tipo_exame = 3 and r.id_resultado = 1)           -- cultura positiva, se entrar na regra
        )

    qualify row_number() over (
        partition by regexp_replace(r.paciente_cpf, r'\D', ''), r.dt_resultado
        order by
            case when r.id_tipo_exame = 1 then 1 else 2 end,
            r.codigo_amostra desc nulls last
    ) = 1
),

-- =============================
-- Unifica fontes de suspeitos
-- =============================
eventos_suspeitos as (
    select
        cpf,
        prontuario_fornecedor,
        cast(entrada_datahora as timestamp) as entrada_datahora,
        cast(saida_datahora   as timestamp) as saida_datahora,

        struct(
            nullif(
                lpad(regexp_replace(cast(estabelecimento.id_cnes as string), r'\D', ''), 7, '0'),
                '0000000'
            ) as id_cnes
        ) as estabelecimento,

        struct(
            cast(coalesce(
                json_extract_scalar(to_json_string(profissional_saude_responsavel), '$.cpf'),
                json_extract_scalar(to_json_string(profissional_saude_responsavel), '$.id_cpf')
            ) as string) as cpf,

            struct(
                nullif(
                    lpad(
                        regexp_replace(
                            cast(coalesce(
                                json_extract_scalar(to_json_string(profissional_saude_responsavel), '$.estabelecimento.id_cnes'),
                                json_extract_scalar(to_json_string(profissional_saude_responsavel), '$.id_cnes'),
                                estabelecimento.id_cnes
                            ) as string),
                            r'\D', ''
                        ),
                        7, '0'
                    ),
                    '0000000'
                ) as id_cnes
            ) as estabelecimento,

            nullif(
                lpad(
                    regexp_replace(
                        cast(coalesce(
                            json_extract_scalar(to_json_string(profissional_saude_responsavel), '$.estabelecimento.id_cnes'),
                            json_extract_scalar(to_json_string(profissional_saude_responsavel), '$.id_cnes'),
                            estabelecimento.id_cnes
                        ) as string),
                        r'\D', ''
                    ),
                    7, '0'
                ),
                '0000000'
            ) as id_cnes
        ) as profissional_saude_responsavel,

        1 as fonte_prioridade
    from episodios_filtrados

    union all
    select
        cpf,
        prontuario_fornecedor,
        entrada_datahora,
        saida_datahora,
        estabelecimento,
        profissional_saude_responsavel,
        fonte_prioridade
    from sisreg_solicitacoes_ontem

    union all
    select
        cpf,
        prontuario_fornecedor,
        entrada_datahora,
        saida_datahora,
        estabelecimento,
        profissional_saude_responsavel,
        fonte_prioridade
    from sisare_altas_ontem

    union all
    select
        cpf,
        prontuario_fornecedor,
        entrada_datahora,
        saida_datahora,
        estabelecimento,
        profissional_saude_responsavel,
        fonte_prioridade
    from gal_positivos_ontem
),

cpfs_sintomaticos as (
    select distinct cpf
    from eventos_suspeitos
),

cpfs_sintomaticos_int as (
    select distinct safe_cast(cpf as int64) as cpf_particao
    from cpfs_sintomaticos
    where cpf is not null
    and regexp_contains(cpf, r'^\d+$')
),

-- =============================
-- Cadastro paciente
-- =============================
cadastros_de_paciente as (
    select
        regexp_replace(p.cpf, r'\D', '') as cpf,
        p.dados.nome                          as nome,
        p.dados.data_nascimento               as dt_nascimento,
        p.dados.genero                        as sexo,
        {{ sinanrio_padronize_sexo('p.dados.genero') }} as id_sexo,
        p.dados.raca                          as raca,
        {{ sinanrio_padronize_raca_cor('p.dados.raca') }} as id_raca_cor,
        (
            select c
            from unnest(p.cns) c with offset off
            order by
                case
                    when regexp_contains(c, r'^\d{15}$') and substr(c,1,1) = '7' then 1
                    when regexp_contains(c, r'^\d{15}$') and substr(c,1,1) in ('1','2') then 2
                    when regexp_contains(c, r'^\d{15}$') and substr(c,1,1) = '8' then 3
                    when regexp_contains(c, r'^\d{15}$') then 4
                    else 9
                end,
                off
            limit 1
        ) as cns,
        (
            select as struct t.valor as telefone
            from unnest(p.contato.telefone) t
            order by t.rank
            limit 1
        ).telefone as telefone,
        (
            select as struct
                e.cep             as endereco_cep,
                e.tipo_logradouro as endereco_tipo_logradouro,
                e.logradouro      as endereco_logradouro,
                e.numero          as endereco_numero,
                e.complemento     as endereco_complemento,
                e.bairro          as endereco_bairro,
                e.cidade          as endereco_cidade,
                e.datahora_ultima_atualizacao
            from unnest(p.endereco) e
            where coalesce(lower(e.sistema), '') = 'vitacare'
            and nullif(trim(e.logradouro), '') is not null
            and upper(trim(e.logradouro)) not in ('SEM INFORMACAO', 'SEM INFORMAÇÃO')
            order by e.datahora_ultima_atualizacao desc nulls last
            limit 1
        ) as endr_vitacare,
        (
            select as struct
                e.cep             as endereco_cep,
                e.tipo_logradouro as endereco_tipo_logradouro,
                e.logradouro      as endereco_logradouro,
                e.numero          as endereco_numero,
                e.complemento     as endereco_complemento,
                e.bairro          as endereco_bairro,
                e.cidade          as endereco_cidade,
                e.datahora_ultima_atualizacao
            from unnest(p.endereco) e
            where coalesce(lower(e.sistema), '') <> 'vitacare'
            and nullif(trim(e.logradouro), '') is not null
            and upper(trim(e.logradouro)) not in ('SEM INFORMACAO', 'SEM INFORMAÇÃO')
            order by e.datahora_ultima_atualizacao desc nulls last
            limit 1
        ) as endr_outros,
        (
            select as struct
                pr.id_cnes     as cnes,
                pr.id_paciente as n_prontuario,
                pr.rank        as rank
            from unnest(p.prontuario) pr
            where coalesce(lower(pr.sistema), '') = 'vitacare'
            order by pr.rank
            limit 1
        ) as prt_vitacare,
        (
            select as struct
                pr.id_cnes     as cnes,
                pr.id_paciente as n_prontuario,
                pr.rank        as rank
            from unnest(p.prontuario) pr
            where coalesce(lower(pr.sistema), '') <> 'vitacare'
            order by pr.rank
            limit 1
        ) as prt_outros,
        p.equipe_saude_familia as equipe_saude_familia
    from {{ ref('mart_historico_clinico__paciente') }} p
    join cpfs_sintomaticos cs on cs.cpf = regexp_replace(p.cpf, r'\D', '')
    join cpfs_sintomaticos_int csi on csi.cpf_particao = p.cpf_particao
),

cadastros_de_paciente_norm as (
    select
        c.cpf,
        c.cns,
        c.nome,
        c.dt_nascimento,
        c.sexo,
        c.id_sexo,
        c.raca,
        c.id_raca_cor,
        case
            when c.telefone is null then null
            else (
                select
                case
                    when tel = '' then null
                    when length(tel) in (8, 9) then concat('21', tel)
                    when length(tel) in (10, 11) then tel
                    when length(tel) in (12, 13) and substr(tel, 1, 2) = '55' then substr(tel, 3)
                    else tel
                end
                from (select regexp_replace(c.telefone, r'\D', '') as tel) t
            )
        end as telefone,

        coalesce(c.endr_vitacare.endereco_cep, c.endr_outros.endereco_cep) as endereco_cep,
        coalesce(c.endr_vitacare.endereco_tipo_logradouro, c.endr_outros.endereco_tipo_logradouro) as endereco_tipo_logradouro,
        coalesce(c.endr_vitacare.endereco_logradouro, c.endr_outros.endereco_logradouro) as endereco_logradouro,
        coalesce(c.endr_vitacare.endereco_numero, c.endr_outros.endereco_numero) as endereco_numero,
        coalesce(c.endr_vitacare.endereco_complemento, c.endr_outros.endereco_complemento) as endereco_complemento,
        coalesce(c.endr_vitacare.endereco_bairro, c.endr_outros.endereco_bairro) as endereco_bairro,
        coalesce(c.endr_vitacare.endereco_cidade, c.endr_outros.endereco_cidade) as endereco_cidade,

        nullif(
            lpad(regexp_replace(cast(c.prt_vitacare.cnes as string), r'\D', ''), 7, '0'),
            '0000000'
        ) as cnes,

        c.prt_vitacare.n_prontuario as n_prontuario,

        (
            select ef.id_ine
            from unnest(c.equipe_saude_familia) ef
            where
                nullif(
                    lpad(regexp_replace(cast(ef.clinica_familia.id_cnes as string), r'\D', ''), 7, '0'),
                    '0000000'
                )
                =
                nullif(
                    lpad(regexp_replace(cast(c.prt_vitacare.cnes as string), r'\D', ''), 7, '0'),
                    '0000000'
                )
            order by
                safe_cast(ef.rank as int64) asc nulls last,
                ef.datahora_ultima_atualizacao desc nulls last
            limit 1
        ) as ine
    from cadastros_de_paciente c
),

cadastros_com_bairro as (
    select
        n.*,
        b.id as id_bairro
    from cadastros_de_paciente_norm n
    left join {{ ref('raw_plataforma_subpav_principal__bairros') }} b
    on {{ clean_name_string("n.endereco_bairro") }} = {{ clean_name_string("b.descricao") }}
),

atendimentos_enriquecidos as (
    select
        ef.*,
        cad.* except (cpf),
        ef.cpf as cpf_pessoa
    from eventos_suspeitos ef
    left join cadastros_com_bairro cad using (cpf)
),

preferencias as (
    select
        au.cpf_pessoa, au.cns, au.nome, au.dt_nascimento, au.id_raca_cor, au.id_sexo,
        au.telefone, au.endereco_cep, au.endereco_tipo_logradouro, au.endereco_logradouro,
        au.endereco_numero, au.endereco_complemento, au.id_bairro, au.endereco_cidade,
        au.estabelecimento, au.profissional_saude_responsavel, au.cnes, au.ine,
        au.n_prontuario, au.entrada_datahora, au.saida_datahora, au.prontuario_fornecedor,
        au.cnes as cnes_final,
        au.ine as ine_final,
        au.n_prontuario as n_prontuario_final,

        nullif(
            lpad(
                regexp_replace(
                    cast(coalesce(
                        json_extract_scalar(to_json_string(au.profissional_saude_responsavel), '$.estabelecimento.id_cnes'),
                        json_extract_scalar(to_json_string(au.profissional_saude_responsavel), '$.id_cnes'),
                        au.estabelecimento.id_cnes
                    ) as string),
                    r'\D', ''
                ),
                7, '0'
            ),
            '0000000'
        ) as cnes_cadastrante_final,

        coalesce(
            json_extract_scalar(to_json_string(au.profissional_saude_responsavel), '$.cpf'),
            json_extract_scalar(to_json_string(au.profissional_saude_responsavel), '$.id_cpf')
        ) as cpf_cadastrante_final,

        coalesce(
            json_extract_scalar(to_json_string(au.profissional_saude_responsavel), '$.cns'),
            json_extract_scalar(to_json_string(au.profissional_saude_responsavel), '$.id_cns')
        ) as cns_cadastrante_final,

        case
            when au.id_bairro is not null then 0
            when au.endereco_cidade is null then 0
            when {{ clean_name_string("au.endereco_cidade") }} like '%RIO DE JANEIRO%' then 0
            else 1
        end as nao_municipe_final,

        row_number() over (
            partition by au.cpf_pessoa
            order by coalesce(au.saida_datahora, au.entrada_datahora) desc, au.fonte_prioridade asc
        ) as rn
    from atendimentos_enriquecidos au
),

-- =============================
-- Notificações
-- =============================
notificacoes_validas as (
    select distinct
        regexp_replace(nu_cartao_sus, r'\D', '') as cns,
        regexp_replace(cpf_paciente, r'\D', '')  as cpf,
        dt_notificacao
    from {{ ref("mart_subpav_sinanrio__notificacao") }}
    where dt_notificacao is not null
),

cpfs_bloqueados_por_notificacao as (
    select distinct
        p.cpf_pessoa
    from preferencias p
    cross join params prm
    inner join notificacoes_validas n
        on (
                (
                    p.cns is not null
                    and regexp_replace(p.cns, r'\D', '') != ''
                    and regexp_replace(p.cns, r'\D', '') = n.cns
                )
                or (
                    regexp_replace(p.cpf_pessoa, r'\D', '') != ''
                    and regexp_replace(p.cpf_pessoa, r'\D', '') = n.cpf
                )
            )
        and date_diff(
            date(coalesce(p.saida_datahora, p.entrada_datahora), prm.tz),
            n.dt_notificacao,
            day
        ) between 0 and {{ janela_notif_dias }}
    where p.rn = 1
),

preferencias_filtrado as (
    select
        p.*
    from preferencias p
    cross join params prm

    left join {{ ref("raw_plataforma_subpav_sinanrio__tb_sintomatico") }} s
    on (
            (
                regexp_replace(p.cpf_pessoa, r'\D', '') != ''
                and regexp_replace(p.cpf_pessoa, r'\D', '') = regexp_replace(s.cpf, r'\D', '')
            )
            or (
                p.cns is not null
                and regexp_replace(p.cns, r'\D', '') != ''
                and regexp_replace(p.cns, r'\D', '') = regexp_replace(s.cns, r'\D', '')
            )
        )
    and coalesce(
            safe_cast(s.created_at as timestamp),
            safe_cast(s.datalake_loaded_at as timestamp),
            timestamp(safe_cast(s.created_at as datetime), prm.tz),
            timestamp(safe_cast(s.datalake_loaded_at as datetime), prm.tz)
        ) >= prm.ts_sint_min
    and coalesce(
            safe_cast(s.created_at as timestamp),
            safe_cast(s.datalake_loaded_at as timestamp),
            timestamp(safe_cast(s.created_at as datetime), prm.tz),
            timestamp(safe_cast(s.datalake_loaded_at as datetime), prm.tz)
        ) < prm.ts_ref_fim

    left join cpfs_bloqueados_por_notificacao nb
        on p.cpf_pessoa = nb.cpf_pessoa

    where p.rn = 1
    and s.cpf is null
    and s.cns is null
    and nb.cpf_pessoa is null
)

select
    cpf_pessoa                                  as cpf,
    cns                                         as cns,
    nome                                        as nome,
    dt_nascimento                               as dt_nascimento,
    id_raca_cor                                 as id_raca_cor,
    id_sexo                                     as id_sexo,
    cast(null as int64)                         as id_escolaridade,
    telefone                                    as telefone,
    endereco_cep                                as cep,
    trim(regexp_replace(
        concat(ifnull(endereco_tipo_logradouro,''),' ',ifnull(endereco_logradouro,'')),
        r'\s+', ' '
    ))                                          as logradouro,
    cast(endereco_numero as string)             as numero,
    endereco_complemento                        as complemento,
    id_bairro                                   as id_bairro,
    endereco_cidade                             as cidade,
    nullif(
        lpad(regexp_replace(cast(cnes_final as string), r'\D', ''), 7, '0'),
        '0000000'
    )                                           as cnes,
    ine_final                                   as ine,
    cnes_cadastrante_final                      as cnes_cadastrante,
    nao_municipe_final                          as nao_municipe,
    n_prontuario_final                          as n_prontuario,
    cpf_cadastrante_final                       as cpf_cadastrante,
    cns_cadastrante_final                       as cns_cadastrante,
    1                                           as id_tb_situacao,
    case trim(lower(prontuario_fornecedor))
        when 'vitacare' then 2
        when 'vitahiscare' then 2
        when 'vitai' then 3
        when 'sisreg' then 4
        when 'sisare' then 5
        when 'smsrio' then 6
        when 'pcsm' then 7
        when 'sarah' then 8
        when 'prontuario' then 9
        when 'gal' then 10
        else 1
    end as id_origem,
    p.data_ref                                  as dt_referencia,
    date(coalesce(saida_datahora, entrada_datahora), p.tz) as dt_situacao
from preferencias_filtrado
cross join params p
