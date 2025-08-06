with

sisreg_solicitacoes as (
    select
        -- profissionais
        to_hex(sha256(cast(profissional_solicitante_cpf as string))) as profissional_solicitante_id,
        to_hex(sha256(operador_solicitante_nome)) as operador_solicitante_id,
        to_hex(sha256(operador_cancelamento_nome)) as operador_cancelamento_id,

        -- unidades
        central_solicitante,
        central_reguladora,
        unidade_solicitante_id as unidade_solicitante_id_cnes,
        unidade_desejada_id as unidade_desejada_id_cnes,

        -- paciente
        to_hex(sha256(cast(paciente_cpf as string))) as paciente_id,
        paciente_sexo,

        case
            when paciente_dt_nasc is null then 'Desconhecida'
            else (
                case
                    when date_diff(current_date(), cast(paciente_dt_nasc as date), year) < 0 then 'Desconhecida'
                    when date_diff(current_date(), cast(paciente_dt_nasc as date), year) < 15 then '0-14'
                    when date_diff(current_date(), cast(paciente_dt_nasc as date), year) < 30 then '15-29'
                    when date_diff(current_date(), cast(paciente_dt_nasc as date), year) < 45 then '30-44'
                    when date_diff(current_date(), cast(paciente_dt_nasc as date), year) < 60 then '45-59'
                    when date_diff(current_date(), cast(paciente_dt_nasc as date), year) < 75 then '60-74'
                    else '75+'
                end
            )
        end as paciente_faixa_etaria,

        -- solicitacao & andamento
        solicitacao_id,
        data_solicitacao,
        data_desejada,
        data_cancelamento,
        data_atualizacao,
        solicitacao_status,
        solicitacao_situacao,
        solicitacao_visualizada_regulador,
        solicitacao_risco,

        -- procedimento & cid
        procedimento_id as procedimento_sisreg_id,
        vaga_solicitada_tp,
        cid_id,

        -- laudo
        laudo_descricao_tp,
        laudo_situacao,
        laudo_observacao,
        laudo_data_observacao

    from {{ ref("raw_sisreg_api__solicitacoes") }}
    where 1 = 1
        and uf_solicitante_id = "33"
        and uf_regulador_id = "33"
        and paciente_uf_res = "RJ"
        and paciente_mun_res = "RIO DE JANEIRO"
        and data_solicitacao >= '2022-01-01' 
        and data_solicitacao < '2025-01-01'
)

select *
from sisreg_solicitacoes
where profissional_solicitante_id is not null
