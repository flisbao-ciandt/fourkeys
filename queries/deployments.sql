# Deployments Table

WITH deploys_github AS (
      SELECT 
      source,
      id as deploy_id,
      time_created,
      JSON_EXTRACT_SCALAR(metadata, '$.deployment.sha') as main_commit,
      ARRAY(
                SELECT JSON_EXTRACT_SCALAR(string_element, '$')
                FROM UNNEST(JSON_EXTRACT_ARRAY(metadata, '$.deployment.additional_sha')) AS string_element) as additional_commits,
      FROM four_keys.events_raw 
      WHERE event_type = "deployment_status" and JSON_EXTRACT_SCALAR(metadata, '$.deployment_status.state') = "success"
    ),
    changes_raw AS (
      SELECT
      id,
      metadata as change_metadata
      FROM four_keys.events_raw
    ),
    deployment_changes as (
      SELECT
      source,
      deploy_id,
      deploys_github.time_created time_created,
      change_metadata,
      four_keys.json2array(JSON_EXTRACT(change_metadata, '$.commits')) as array_commits,
      main_commit
      FROM deploys_github
      JOIN
        changes_raw on (
          changes_raw.id = deploys_github.main_commit
          or changes_raw.id in unnest(deploys_github.additional_commits)
        )
    )

    SELECT 
    source,
    deploy_id,
    time_created,
    main_commit,   
    ARRAY_AGG(DISTINCT JSON_EXTRACT_SCALAR(array_commits, '$.id')) changes,    
    FROM deployment_changes
    CROSS JOIN deployment_changes.array_commits
    GROUP BY 1,2,3,4;
