{{
    config(
        schema="brutos_centralderegulacao_mysql", alias="minha_saude__lista_usuario"
    )
}}

with
    source as (
        select
            safe_cast(idusuario as int64) as idusuario,
            cadastroconfirmado,
            cadastroativo,
            ultimaatualizacaocadsus,
            celularvalido,
            datahoravalidacaocelular,
            datahoracadastro,
            cadastrogovbr,
            datanascimento,
            safe_cast(idadepaciente as int64) as idadepaciente,
            sexo,
            racacor,
            bairroresidencia,
            safe_cast(safe_cast(cap as float64) as int64) as cap,    
            municipioresidencia,
            ufresidencia,
            safe_cast(safe_cast(fontecadastro as float64) as int64) as fontecadastro,    
            date(data_extracao) as data_extracao,
            ano_particao,
            mes_particao,
            data_particao
        from
            {{
                source(
                    "brutos_centralderegulacao_mysql_staging",
                    "vw_minhaSaude_listaUsuario",
                )
            }}
    ),
    deduplicated as (
        select *
        from source
        qualify row_number() over (
            partition by idusuario 
            order by data_extracao desc, datahoracadastro desc
        ) = 1
    )
select *
from deduplicated
