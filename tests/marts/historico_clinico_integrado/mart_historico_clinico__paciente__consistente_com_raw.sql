-- este teste verifica se a tabela
-- mart_historico_clinico__paciente__consistente_com_raw foi criada corretamente, não
-- descartando nenhuma informação não itencional das tabelas raw até a tabela final.
with
    -- import CTEs
    -- -- SMSRIO
    raw_smsrio as (select * from {{ ref("raw_plataforma_smsrio__paciente") }}),

    int_smsrio as (select * from {{ ref("int_historico_clinico__paciente__smsrio") }}),

    mrg_smsrio as (
        select *
        from
            {{ ref("mart_historico_clinico__paciente") }},
            unnest(prontuario) as prontuario
        where prontuario.sistema = "SMSRIO"
    ),

    -- logical CTEs
    -- -- SMS RIO
    raw_smsrio_metrics as (
        select
            "smsrio" as sistema,
            count(*) as raw_total_rows,
            count(distinct cpf) as raw_distinct_cpfs,
            count(
                distinct case when {{ validate_cpf("cpf") }} then cpf else null end
            ) as raw_valid_cpfs,
            count(*) - count(
                distinct case when {{ validate_cpf("cpf") }} then cpf else null end
            ) as raw_to_int_discarded_rows_expected  -- remover linhas com CPF inválido e duplicados
        from raw_smsrio
    ),

    int_smsrio_valid_record as (
        select
            "smsrio" as sistema,
            count(
                case
                    when
                        {{ validate_cpf("cpf") }}
                        and dados.nome is not null
                        and dados.data_nascimento is not null
                    then 1
                    else null
                end
            ) as int_valid_records  -- remover linhas com CPF inválido e dados faltantes
        from (select * except (dados), dados from int_smsrio, unnest(dados) as dados)
    ),

    int_smsrio_metrics as (
        select
            a.*,
            int_valid_records,
            int_total_rows - int_valid_records as int_to_mrg_discarded_rows_expected  
        from
            (
                select
                    "smsrio" as sistema,
                    count(*) as int_total_rows,
                    count(distinct cpf) as int_distinct_cpfs,
                    count(
                        distinct case
                            when {{ validate_cpf("cpf") }} then cpf else null
                        end
                    ) as int_valid_cpfs,
                from int_smsrio
            ) as a
        left join int_smsrio_valid_record using (sistema)
    ),

    mrg_smsrio_metrics as (
        select
            "smsrio" as sistema,
            count(*) as mrg_total_rows,
            count(distinct cpf) as mrg_distinct_cpfs,
        from mrg_smsrio
    ),

    final_smsrio as (
        select
            raw_total_rows,
            raw_distinct_cpfs,
            raw_valid_cpfs,
            int_total_rows,
            raw_to_int_discarded_rows_expected,
            raw_total_rows - int_total_rows as raw_to_int_discarded_rows_actual,
            int_valid_cpfs,
            int_valid_records,
            int_to_mrg_discarded_rows_expected,
            int_total_rows - mrg_total_rows as int_to_mrg_discarded_rows_actual,
            mrg_total_rows
        from raw_smsrio_metrics
        join int_smsrio_metrics using (sistema)
        join mrg_smsrio_metrics using (sistema)
    ),

    checks as (
        (
            select
                'raw_to_int_discarded_rows' as metric,
                'smsrio' as sistema,
                raw_to_int_discarded_rows_expected as expected,
                raw_to_int_discarded_rows_actual as actual,
                case
                    when
                        raw_to_int_discarded_rows_expected
                        = raw_to_int_discarded_rows_actual
                    then 'pass'
                    else 'fail'
                end as status
            from final_smsrio
        )
        union all
        (
            select
                'int_to_mrg_discarded_rows' as metric,
                'smsrio' as sistema,
                int_to_mrg_discarded_rows_expected as expected,
                int_to_mrg_discarded_rows_actual as actual,
                case
                    when
                        int_to_mrg_discarded_rows_expected
                        = int_to_mrg_discarded_rows_actual
                    then 'pass'
                    else 'fail'
                end as status
            from final_smsrio
        )
    )

-- final CTE
-- simple select statement
select *
from checks
-- where status = 'fail'
order by metric desc
