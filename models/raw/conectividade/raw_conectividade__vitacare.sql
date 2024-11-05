{{
    config(
        alias="vitacare",
        materialized="table",
    )
}}

with
    -- PREPARATION
    events_from_window as (
        select 
            *
        from {{ source("brutos_conectividade_staging", "vitacare") }}
    ),
    events_ranked_by_freshness as (
        select *, row_number() over (partition by file_name order by gcs_updated_at desc) as rank
        from events_from_window
    ),
    latest_events as (select * from events_ranked_by_freshness where rank = 1),

    -- TRANSFORM
    data_as_json as (
        SELECT
            * except(content),
            SAFE.PARSE_JSON(content) AS content
        FROM latest_events
    ),
    treated as (
        SELECT
            data_as_json.host_cnes as unidade_cnes,
            cast(IF(JSON_VALUE(content, "$.type") is null, false, true) as boolean) as is_test_successfull,

            JSON_VALUE(content, "$.error") as error_message,

            SAFE_CAST(JSON_VALUE(content, "$.download.bandwidth") AS INT64) AS download_bandwidth,
            SAFE_CAST(JSON_VALUE(content, "$.download.bytes") AS INT64) AS download_bytes,
            SAFE_CAST(JSON_VALUE(content, "$.download.elapsed") AS INT64) AS download_elapsed,
            SAFE_CAST(JSON_VALUE(content, "$.upload.bandwidth") AS INT64) AS upload_bandwidth,
            SAFE_CAST(JSON_VALUE(content, "$.upload.bytes") AS INT64) AS upload_bytes,
            SAFE_CAST(JSON_VALUE(content, "$.upload.elapsed") AS INT64) AS upload_elapsed,
            SAFE_CAST(JSON_VALUE(content, "$.ping.jitter") AS FLOAT64) AS ping_jitter,
            SAFE_CAST(JSON_VALUE(content, "$.ping.latency") AS FLOAT64) AS ping_latency,
            SAFE_CAST(JSON_VALUE(content, "$.packetLoss") AS FLOAT64) AS packet_loss,
            JSON_VALUE(content, "$.isp") AS isp,
            JSON_VALUE(content, "$.interface.internalIp") AS internal_ip,
            JSON_VALUE(content, "$.interface.externalIp") AS external_ip,
            JSON_VALUE(content, "$.interface.macAddr") AS mac_address,
            SAFE_CAST(JSON_VALUE(content, "$.interface.isVpn") AS BOOLEAN) AS is_vpn,
            SAFE_CAST(JSON_VALUE(content, "$.server.id") AS INT64) AS server_id,
            JSON_VALUE(content, "$.server.name") AS server_name,
            JSON_VALUE(content, "$.server.location") AS server_location,
            JSON_VALUE(content, "$.server.country") AS server_country,
            JSON_VALUE(content, "$.server.host") AS server_host,
            SAFE_CAST(JSON_VALUE(content, "$.server.port") AS INT64) AS server_port,
            JSON_VALUE(content, "$.server.ip") AS server_ip,
            JSON_VALUE(content, "$.result.id") AS result_id,
            JSON_VALUE(content, "$.result.url") AS result_url,

            struct(
                safe_cast(JSON_VALUE(content, "$.timestamp") as timestamp) AS host_executed_at,
                safe_cast(data_as_json.host_created_at as timestamp) as host_created_at,
                safe_cast(data_as_json.gcs_created_at as timestamp) as gcs_created_at,
                safe_cast(data_as_json.gcs_updated_at as timestamp) as gcs_updated_at,
                safe_cast(data_as_json.datalake_loaded_at as timestamp) as datalake_loaded_at
            ) as metadata
        FROM data_as_json
    )
select *
from treated
