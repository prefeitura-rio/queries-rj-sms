{{ 
    config(
        materialized = 'table',
        alias        = "sintomaticos_respiratorios_dia"
    ) 
}}

{% set ontem = (modules.datetime.date.today() - modules.datetime.timedelta(days=1)).isoformat() %}

with

episodios_ontem_base as (
    select
        paciente_cpf                           as cpf,
        prontuario.fornecedor                  as prontuario_fornecedor,
        condicoes,
        entrada_datahora,
        saida_datahora,
        estabelecimento,
        profissional_saude_responsavel,
        data_particao
    from {{ ref('mart_historico_clinico__episodio') }}
    where paciente_cpf is not null
      and data_particao = '{{ ontem }}'
),

episodios_filtrados as (
    select e.*
    from episodios_ontem_base e
    where e.condicoes is not null
      and exists (
          select 1
          from unnest(e.condicoes) cid
          where substr(regexp_replace(upper(cid.id), r'\.', ''), 1, 4)
              in unnest({{ sinanrio_lista_cids_sintomaticos() }})
      )
),

cpfs_sintomaticos as (
    select distinct cpf from episodios_filtrados
),

cpfs_sintomaticos_int as (
    select distinct safe_cast(cpf as int64) as cpf_particao
    from cpfs_sintomaticos
    where cpf is not null
      and regexp_contains(cpf, r'^\d+$')
),

cadastros_de_paciente as (
    select
        p.cpf,
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
                e.cep               as endereco_cep,
                e.tipo_logradouro   as endereco_tipo_logradouro,
                e.logradouro        as endereco_logradouro,
                e.numero            as endereco_numero,
                e.complemento       as endereco_complemento,
                e.bairro            as endereco_bairro,
                e.cidade            as endereco_cidade,
                e.datahora_ultima_atualizacao
            from unnest(p.endereco) e
            order by e.datahora_ultima_atualizacao desc nulls last
            limit 1
        ) as endr,
        (
            select as struct
                pr.id_cnes     as cnes,
                pr.id_paciente as n_prontuario,
                pr.rank        as rank
            from unnest(p.prontuario) pr
            order by pr.rank
            limit 1
        ) as prt,
        (select ef.id_ine from unnest(p.equipe_saude_familia) ef limit 1) as ine
    from {{ ref('mart_historico_clinico__paciente') }} p
    join cpfs_sintomaticos     cs  on cs.cpf = p.cpf
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
        c.telefone,
        c.endr.endereco_cep              as endereco_cep,
        c.endr.endereco_tipo_logradouro  as endereco_tipo_logradouro,
        c.endr.endereco_logradouro       as endereco_logradouro,
        c.endr.endereco_numero           as endereco_numero,
        c.endr.endereco_complemento      as endereco_complemento,
        c.endr.endereco_bairro           as endereco_bairro,
        c.endr.endereco_cidade           as endereco_cidade,
        c.prt.cnes                       as cnes,
        c.ine                            as ine,
        c.prt.n_prontuario               as n_prontuario
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
    from episodios_filtrados ef
    left join cadastros_com_bairro cad using (cpf)
),

preferencias as (
    select
        au.cpf_pessoa, au.cns, au.nome, au.dt_nascimento, au.id_raca_cor, au.id_sexo,
        au.telefone, au.endereco_cep, au.endereco_tipo_logradouro, au.endereco_logradouro,
        au.endereco_numero, au.endereco_complemento, au.id_bairro, au.endereco_cidade,
        au.estabelecimento, au.profissional_saude_responsavel, au.cnes, au.ine,
        au.n_prontuario, au.saida_datahora, au.prontuario_fornecedor,

        coalesce(au.estabelecimento.id_cnes, au.cnes) as cnes_final,

        coalesce(
            json_extract_scalar(to_json_string(au.profissional_saude_responsavel), '$.equipe.id_ine'),
            au.ine
        ) as ine_final,

        au.n_prontuario as n_prontuario_final,

        coalesce(
            json_extract_scalar(to_json_string(au.profissional_saude_responsavel), '$.estabelecimento.id_cnes'),
            json_extract_scalar(to_json_string(au.profissional_saude_responsavel), '$.id_cnes'),
            au.estabelecimento.id_cnes
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

        row_number() over (partition by au.cpf_pessoa order by au.saida_datahora desc) as rn
    from atendimentos_enriquecidos au
),

preferencias_filtrado as (
    select
        p.*
    from preferencias p
    left join {{ ref("raw_plataforma_subpav_sinanrio__tb_sintomatico") }} s
        on (
            regexp_replace(p.cpf_pessoa, r'\D', '') != ''
            and regexp_replace(p.cpf_pessoa, r'\D', '') = regexp_replace(s.cpf, r'\D', '')
        )
        or (
            p.cns is not null
            and regexp_replace(p.cns, r'\D', '') != ''
            and regexp_replace(p.cns, r'\D', '') = regexp_replace(s.cns, r'\D', '')
        )
    where
        p.rn = 1
        and s.cpf is null
        and s.cns is null
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
    cnes_final                                  as cnes,
    ine_final                                   as ine,
    nao_municipe_final                          as nao_municipe,
    n_prontuario_final                          as n_prontuario,
    cnes_cadastrante_final                      as cnes_cadastrante,
    cpf_cadastrante_final                       as cpf_cadastrante,
    cns_cadastrante_final                       as cns_cadastrante,
    1                                           as id_tb_situacao,
    prontuario_fornecedor                       as origem
from preferencias_filtrado
