{{
    config(
        schema="brutos_prontuario_carioca_saude_mental",
        alias="profissionais",
        materialized="table",
        tags=["raw", "pcsm"],
        description=""
    )
}} 

with 
    renomeado as (
        select
            seqprof as id_profissional,
            sequs as id_unidade_saude,
            seqlogin as id_login,
            codcns as cns,
            cpfprof as cpf,
            dtnasc as data_nascimento,
            upper(nomeprof) as nome,
            nomsocprof as nome_social,
            indusonomsoc as nome_social_indicador,
            inscprof as inscricao,
            matrprof as matricula,
            emailprof as email,
            sigconselho as sigla_conselho,
            sigufconselho as uf_conselho,
            indsexo as sexo,
            indgenero as genero,
            safe_cast(dtcadast as date) as data_cadastro,
            if (crmbloq = 'S', true, false) as crm_bloqueado,
            if (indativo = 'S', true, false) as ativo,
            dsctel as telefone
            safe_cast(datultmodif as date) as data_ultima_modificacao,

            --- Colunas desconhecidas 
            nomgue,
        from {{ source('brutos_prontuario_carioca_saude_mental_staging', 'gh_prof') }}
)

select * from renomeado