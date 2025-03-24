{{
    config(
        schema="intermediario_historico_clinico",
        alias="obito_vitai",
        materialized="table",
    )
}}
-- Cria tabela de episódios vitai com desfecho óbito para sinalização

-- Óbitos assinalados em episódios vitai
with alta_adm as (
    select * from 
    {{ref('raw_prontuario_vitai__alta')}}
),
alta_internacao as (
    select * from 
    {{ref('raw_prontuario_vitai__resumo_alta')}}
),
boletim_r as (
    select 
        b.gid, 
        b.alta_data, 
        paciente.cpf,
        paciente.cns,
        b.internacao_data
    from {{ ref("raw_prontuario_vitai__boletim") }} as b
    left join 
        {{ ref("raw_prontuario_vitai__paciente") }} as paciente
        on b.gid_paciente = paciente.gid  
),
boletins_consulta as (
    select
        boletim_r.gid,
        boletim_r.cpf,
        boletim_r.cns,
        boletim_r.alta_data
    from boletim_r
    left join
        {{ ref("raw_prontuario_vitai__atendimento") }} as atendimento
        on boletim_r.gid = atendimento.gid_boletim
    where atendimento.gid_boletim is not null and boletim_r.internacao_data is null
),
boletins_internacao as (
    select
        boletim_r.gid,
        boletim_r.cpf,
        boletim_r.cns,
        boletim_r.alta_data
    from boletim_r
    left join {{ ref("raw_prontuario_vitai__internacao") }}  internacao
        on boletim_r.gid = internacao.gid_boletim
    where
        internacao.gid_boletim is not null
        and boletim_r.internacao_data is not null
),
boletins_exames as (
    select
        boletim_r.gid,
        boletim_r.cpf,
        boletim_r.cns,
        boletim_r.alta_data
    from boletim_r
    left join {{ ref("raw_prontuario_vitai__exame") }} as exame_table
        on boletim_r.gid = exame_table.gid_boletim
    left join{{ ref("raw_prontuario_vitai__atendimento") }} as atendimento
        on boletim_r.gid = atendimento.gid_boletim
    where
        exame_table.gid_boletim is not null
        and atendimento.gid_boletim is null
        and boletim_r.internacao_data is null
),
boletim as (
    select distinct         
        gid,
        cpf,
        cns,
        alta_data
        from (
        select * from boletins_consulta
        union all 
        select * from boletins_internacao
        union all
        select * from boletins_exames
        )
),
obitos as (
    select 
        case 
            when {{clean_numeric_string('boletim.cpf')}}  = '' then null 
            else {{clean_numeric_string('boletim.cpf')}} 
        end as cpf, 
        case 
            when {{clean_numeric_string('boletim.cns')}}  = '' then null 
            else {{clean_numeric_string('boletim.cns')}} 
        end as cns, 
        extract(
            date from boletim.alta_data
        ) as obito_data, 
        boletim.gid as gid_boletim_obito
    from boletim
    left join alta_adm
    on boletim.gid = alta_adm.gid_boletim
    left join alta_internacao
    on boletim.gid = alta_internacao.gid_boletim
    where (
    regexp_contains(alta_adm.abe_obs, '(CAD[Á|A]VER)|([O|Ó]BITO)') 
    or (
        alta_adm.obito_data  is not null 
    )
    )
    or alta_internacao.desfecho_internacao = 'ÓBITO'
),
-- FILTROS --
-- Filtro de obitos com episodios posteriores na vitai ou vitacare
ultimo_boletim_vitai as (
  select 
    cpf, 
    max(
        extract(
            date from alta_data
            )
    ) as ultima_entrada 
  from boletim
  group by 1
),
boletins_vitacare as(
  select 
    cpf, 
    case
        when
            eh_coleta = 'True' then false
        when 
            eh_coleta != 'True' and 
            (
                json_extract_scalar(condicao_json, "$.cod_cid10") is not null
                or soap_subjetivo_motivo is not null
                or soap_plano_observacoes is not null
            )
        then false
        else true
    end as flag__episodio_sem_informacao,
    datahora_fim
  from  {{ref('raw_prontuario_vitacare__atendimento')}},unnest(json_extract_array(condicoes)) as condicao_json
),
ultimo_boletim as(
    select cpf, max(ultima_entrada) as ultima_entrada
    from (
    select cpf,max(extract(date from datahora_fim)) as ultima_entrada
    from boletins_vitacare
    where flag__episodio_sem_informacao=false
    group by 1
    union all 
    select * 
    from ultimo_boletim_vitai
    )
    group by 1
),
-- Final -- 
obitos_flags as (
    select obitos.*, 
        ultimo_boletim.ultima_entrada,
        IF(
            (ultimo_boletim.ultima_entrada > date_add(obito_data, interval 1 day)) OR (obito_data is null),
            1,
            0) as tem_boletim_pos_obito
    from obitos
    left join ultimo_boletim
    on obitos.cpf = ultimo_boletim.cpf
)
select 
    cpf, 
    max(obito_data) as obito_data,
    array_agg(distinct cns ignore nulls) as cns, 
    array_agg(distinct gid_boletim_obito ignore nulls) as gid_boletim_obito
from obitos_flags 
where tem_boletim_pos_obito = 0
and cpf is not null
group by 1