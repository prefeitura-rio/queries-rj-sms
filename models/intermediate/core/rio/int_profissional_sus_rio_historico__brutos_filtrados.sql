
with
versao_atual as (
    select MAX(data_particao) as versao 
    from {{ ref("raw_cnes_web__tipo_unidade") }}
), 

estabelecimentos_mrj_sus as (
    select distinct safe_cast(id_cnes as int64) as id_cnes from {{ ref("dim_estabelecimento_sus_rio_historico") }} where safe_cast(data_particao as string) = (select versao from versao_atual)
),

profissionais_mrj_non_unique as (
select
    ano as ano_competencia,
    mes as mes_competencia,
    concat(ano, '-', lpad(cast(mes as string), 2, '0')) data_registro,
    lpad(id_estabelecimento_cnes, 7, '0') AS id_cnes,
    sigla_uf,
    case 
        when cartao_nacional_saude = "nan" then NULL 
        else cartao_nacional_saude 
    end as profissional_cns,
    nome as profissional_nome,
    lpad(cbo_2002, 6, '0') as id_cbo,
    substring(tipo_vinculo, 1, 4) as id_tipo_vinculo,
    substring(tipo_vinculo, 1, 2) as id_vinculacao,
    left(lpad(cbo_2002, 6, '0'), 4) as id_cbo_familia,
    lpad(id_registro_conselho, 8, '0') as id_registro_conselho,
    tipo_conselho as id_tipo_conselho,
    carga_horaria_outros,
    carga_horaria_hospitalar,
    carga_horaria_ambulatorial,
    carga_horaria_outros + carga_horaria_hospitalar + carga_horaria_ambulatorial as carga_horaria_total

from {{ ref("raw_cnes_ftp__profissional") }}

where
    ano >= 2010
    and sigla_uf = "RJ"
    and (
        indicador_atende_sus = 1
        or indicador_vinculo_contratado_sus = 1
        or indicador_vinculo_autonomo_sus = 1
    )
    and safe_cast(id_estabelecimento_cnes as int64) in (select id_cnes from estabelecimentos_mrj_sus)
)

select distinct * from profissionais_mrj_non_unique