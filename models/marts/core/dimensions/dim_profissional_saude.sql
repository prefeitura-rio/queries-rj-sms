{{
    config(
        schema="saude_dados_mestres",
        alias="profissional_saude",
        materialized="table",
        tags=["weekly"],
    )
}}

with
    estabelecimentos as (select distinct id_cnes from {{ ref("dim_estabelecimento") }}),

    funcionarios_status as (
        select distinct 
        cpf, 
        dados.status_ativo
        from {{ ref("raw_ergon_funcionarios")}},
        unnest(dados) AS dados
        where dados.status_ativo = true
    ),

    alocacao as (
        select
            profissional_codigo_sus,
            id_cbo,
            cbo,
            id_cbo_familia,
            cbo_familia,
            id_tipo_conselho,
            id_registro_conselho
        from
            {{ ref("int_profissional_saude__vinculo_estabelecimento_serie_historica") }}
            as v
        inner join estabelecimentos on estabelecimentos.id_cnes = v.id_cnes
        where
            data_registro = (
                select max(data_registro)
                from
                    {{
                        ref(
                            "int_profissional_saude__vinculo_estabelecimento_serie_historica"
                        )
                    }}
            )
    ),

    profissionais_datasus as (
        select
            id_codigo_sus,
            upper(nome) as nome,
            cns,
            data_particao,
            data_atualizacao,
            row_number() over (
                partition by id_codigo_sus order by data_atualizacao desc
            ) as ord_cod_sus,
            row_number() over (
                partition by cns order by data_atualizacao desc
            ) as ord_cns
        from {{ ref("raw_cnes_web__dados_profissional_sus") }} as unique_p
        inner join
            alocacao as alocacao
            on unique_p.id_codigo_sus = alocacao.profissional_codigo_sus
    ),



    cbo_distinct as (
        select distinct
            profissional_codigo_sus,
            id_cbo,
            cbo,
            case 
                when regexp_contains(lower(cbo),'^medic')
                    then 'MÉDICOS'
                when regexp_contains(lower(cbo),'^cirurgiao[ |-|]dentista')
                    then 'DENTISTAS'
                when regexp_contains(lower(cbo),'psic')
                    then 'PSICÓLOGOS'  
                when regexp_contains(lower(cbo),'fisioterap')
                    then 'FISIOTERAPEUTAS'
                when regexp_contains(lower(cbo),'nutri[ç|c]')
                    then 'NUTRICIONISTAS'
                when regexp_contains(lower(cbo),'fono')
                    then 'FONOAUDIÓLOGOS'   
                when regexp_contains(lower(cbo),'farm')
                    then 'FARMACÊUTICOS'  
                when ((regexp_contains(lower(cbo),'enferm')) and (lower(cbo) !='socorrista (exceto medicos e enfermeiros)'))
                    then 'ENFERMEIROS'  
                else
                    'OUTROS PROFISSIONAIS'
            end as cbo_agrupador,
            id_cbo_familia,
            cbo_familia, 
        from alocacao
    ),

    cbo_agg as (
        select
            profissional_codigo_sus,
            array_agg(struct(id_cbo, cbo, cbo_agrupador, id_cbo_familia, cbo_familia)) as cbo
        from cbo_distinct
        group by 1
    ),

    conselho_distinct as (
        select distinct profissional_codigo_sus, id_tipo_conselho, id_registro_conselho
        from alocacao
    ),

    conselho_agg as (
        select
            profissional_codigo_sus,
            array_agg(struct(id_registro_conselho, id_tipo_conselho)) as conselho
        from conselho_distinct
        group by 1
    ),
    --=-=-=-=--=-=-=-=-=-=-=-==-=
    --  ENRIQUECIMENTO DE CPF  --
    --=-=-=-=-=-=-=-=-=-=-=-=-=-=
    -- GDB  
    profissionais_cnes_gdb as (
        select distinct 
            id_profissional_sus,
            cpf,
            cns,
            upper(nome) as nome,
        from {{ ref('raw_cnes_gdb__profissional') }}
    ),

    -- Cópia de mart_historico_clinico__paciente na parte de cns
    cpf_profissionais as (
        select distinct *
        from (
            select
                cpf,
                c.cns,
                upper(d.nome) as nome,
                c.rank AS rank,
                "VITAI" AS sistema,
                3 AS merge_order
            from {{ ref('int_historico_clinico__paciente__vitai') }}, unnest(cns) as c, unnest(dados) as d
            union all
            select
                cpf,
                c.cns,
                upper(d.nome) as nome,
                c.rank AS rank,
                "SMSRIO" AS sistema,
                2 AS merge_order
            from {{ ref('int_historico_clinico__paciente__smsrio') }}, unnest(cns) as c, unnest(dados) as d
        )
    ),
    cns_dedup AS (
        SELECT
            cpf,
            cns,
            nome,
            sistema
        FROM(
            SELECT 
                cpf,
                cns,
                nome,
                rank,
                merge_order,
                ROW_NUMBER() OVER (PARTITION BY cpf, cns ORDER BY merge_order, rank ASC) AS dedup_rank,
                sistema
            FROM cpf_profissionais
            ORDER BY  merge_order ASC, rank ASC 
        )
        WHERE dedup_rank = 1
        ORDER BY  merge_order ASC, rank ASC 
    ),
    --  Retira do de-para cns que possui mais de um cpf, por não ser confiavel
    cns_contagem AS (
        SELECT
            cpf,
            nome,
            CASE
                WHEN cc.cpf_count > 1 THEN NULL
                ELSE cd.cns
            END AS cns
        FROM cns_dedup cd
        LEFT JOIN (
            SELECT 
                cns, 
                COUNT(DISTINCT cpf) AS cpf_count
            FROM cns_dedup
            GROUP BY cns
        ) AS cc
            ON cd.cns = cc.cns
    ),
    cns_dados AS (
        SELECT 
            cpf,
            nome,
            cns,
        FROM cns_contagem
        WHERE cns IS NOT NULL
    ),
final as (
    select
        distinct 
        profissionais_datasus.id_codigo_sus as id_profissional_sus,
        coalesce(
            profissionais_cnes_gdb.cpf,
            case 
                when regexp_extract(profissionais_datasus.nome, '([^ ]*) ') = regexp_extract(cns_dados.nome, '([^ ]*) ') then cns_dados.cpf
                else null 
            end 
        )as cpf,
        coalesce(profissionais_cnes_gdb.cns, profissionais_datasus.cns) as cns,
        coalesce(profissionais_cnes_gdb.nome,profissionais_datasus.nome) as nome,
        cbo_agg.cbo,
        conselho_agg.conselho,
        funcionarios_status.status_ativo as funcionario_ativo_indicador
    from ( 
        select * 
        from profissionais_datasus 
        where ord_cns=1 
        and ord_cod_sus=1
    ) as profissionais_datasus
    left join
        cns_dados 
        on profissionais_datasus.cns = cns_dados.cns
    left join
        cbo_agg 
        on profissionais_datasus.id_codigo_sus = cbo_agg.profissional_codigo_sus
    left join
        conselho_agg 
        on profissionais_datasus.id_codigo_sus = conselho_agg.profissional_codigo_sus
    left join
        funcionarios_status 
        on cns_dados.cpf = funcionarios_status.cpf
    left join 
        profissionais_cnes_gdb
        on profissionais_datasus.id_codigo_sus = profissionais_cnes_gdb.id_profissional_sus
) 
select * from final