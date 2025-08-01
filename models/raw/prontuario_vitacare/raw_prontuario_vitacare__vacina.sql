{{
    config(
        alias="vacina",
        materialized="table",
        tags=['daily']
    )
}}

with
    source as (
        select * from {{ source("brutos_prontuario_vitacare_staging", "vacina") }}
    ),
    renamed as (
        select
            id,
            ncnesunidade as id_cnes,
            ap as area_programatica,
            nomeunidadesaude as estabelecimento_nome,
            microareacodigo as id_microarea,
            codigoequipesaude as id_equipe,
            codigoineequipesaude as id_equipe_ine,
            nomeequipesaude as equipe_nome,
            nprotuario as id_vitacare_paciente,
            ncpf as paciente_cpf,
            ncns as paciente_cns,
            nomepessoacadastrada as paciente_nome,
            sexonascimento as paciente_sexo,
            datanascimento as paciente_nascimento_data,
            nomemaepessoacadastrada as nome_mae,
            datanascimentomae as mae_nascimento_data,
            situacaousuario as paciente_situacao,
            datacadastro as paciente_cadastro_data,
            obito as paciente_obito,
            vacina as descricao,
            dataaplicacao as aplicacao_data,
            datahoraregistro as registro_data,
            dosevtc as dose,
            lote as lote,
            tiporegistro as registro_tipo,
            estrategia,
            diff,
            cbo as profissional_cbo,
            cnsprofissional as profissional_cns,
            cpfprofissional as profissional_cpf,
            profissional as profissional_nome,
            ano_particao as ano_particao,
            mes_particao as mes_particao,
            data_particao as data_particao,
            _data_carga as imported_at,
        from source
    ),

    final as (

        select
            -- Primary Key
            concat(id_cnes, "-", id) as id,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id_cnes",
                        "id",
                        "paciente_cns"
                    ]
                )
            }} as id_surrogate,

            -- Foreign Key
            {{clean_numeric("id_cnes")}} as id_cnes,
            id_microarea,
            id_equipe,
            id_equipe_ine,
            area_programatica,
            id_vitacare_paciente,

            -- - Common Fields
            {{ proper_estabelecimento("estabelecimento_nome") }}
            as estabelecimento_nome,
            paciente_cpf,
            paciente_cns,
            {{proper_br("paciente_nome")}} as paciente_nome,
            lower(paciente_sexo) as paciente_sexo,
            safe_cast(paciente_nascimento_data as date) as paciente_nascimento_data,
            {{ proper_br("nome_mae") }} as nome_mae,
            safe_cast(mae_nascimento_data as date) as mae_nascimento_data,
            lower(paciente_situacao) as paciente_situacao,
            safe_cast(paciente_cadastro_data as date) as paciente_cadastro_data,
            paciente_obito,
            lower(descricao) as descricao,
            safe_cast(aplicacao_data as date) as aplicacao_data,
            timestamp_add(datetime(safe_cast({{process_null('registro_data')}} as timestamp), 'America/Sao_Paulo'),interval 3 hour) as registro_data,
            lower(dose) as dose,
            lote,
            lower(registro_tipo) as registro_tipo,
            lower(estrategia) as estrategia,
            diff,
            {{ proper_br("equipe_nome") }} as equipe_nome,
            lower(profissional_cbo) as profissional_cbo,
            profissional_cns,
            profissional_cpf,
            {{ proper_br("profissional_nome") }} as profissional_nome,

            -- Metadata
            safe_cast(data_particao as date) as data_particao,
            timestamp_add(datetime(timestamp({{process_null('imported_at')}}), 'America/Sao_Paulo'),interval 3 hour) as imported_at,

        from renamed
        qualify row_number() over(partition by id_surrogate order by registro_data desc) = 1
    )

select distinct * 
from final
