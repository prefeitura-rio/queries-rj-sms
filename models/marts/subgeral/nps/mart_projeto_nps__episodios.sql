{{
    config(
        enabled=true,
        schema="projeto_nps",
        alias="episodios",
        partition_by={"field": "data_consulta", "data_type": "DATE"},
    )
}}

with
    episodios_interesse as (
        select
            paciente_cpf,
            paciente.data_nascimento,
            entrada_data,
            entrada_datahora,
            estabelecimento.id_cnes,
            prontuario.fornecedor,
            prontuario.id_prontuario_global,
            id_hci,
        from {{ ref("mart_historico_clinico__episodio") }}
        where extract(year from entrada_datahora) >= 2025
    ),

    enriq_pacientes as (
        select
            eps.*,
            p.dados.genero as sexo,
            concat(telefone.ddd, telefone.valor) as telefone,
            equipe_saude_familia.id_ine,
            endereco.cidade

        from episodios_interesse as eps
        left join
            {{ ref("mart_historico_clinico__paciente") }} as p
            on eps.paciente_cpf = p.cpf
        left join unnest(p.contato.telefone) as telefone
        left join unnest(p.equipe_saude_familia) as equipe_saude_familia
        left join unnest(p.endereco) as endereco
    ),

    enriq_riscos as (
        select eps.*, string_agg(distinct risco.risco, ', ') as risco

        from enriq_pacientes as eps
        left join
            {{ ref("raw_prontuario_vitai__classificacao_risco") }} as risco
            on eps.id_prontuario_global = risco.gid_boletim
        group by all
    ),

    final as (
        select
            extract(year from entrada_data) as competencia_ano,
            extract(month from entrada_data) as competencia_mes,
            id_hci,
            id_prontuario_global,
            risco,
            fornecedor,
            date(entrada_data) as data_consulta,
            entrada_datahora as data_hora_consulta,
            id_cnes as cnes_unidade,
            id_ine as codigo_inea_equipe,
            sexo,
            data_nascimento,
            telefone,
            cidade as municipio
        from enriq_riscos
    )

select *
from final
