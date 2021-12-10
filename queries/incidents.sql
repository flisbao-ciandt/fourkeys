# Incidents Table
SELECT
  source,
  incident_id,
  MIN(IF(root.time_created < issue.time_created, root.time_created, issue.time_created)) as time_created,
  MAX(time_resolved) as time_resolved,
  ARRAY_AGG(root_cause IGNORE NULLS) changes,
FROM
  (
    SELECT 
      source,
      JSON_EXTRACT_SCALAR(metadata, '$.kanbanizePayload.card.taskid') as incident_id,
      TIMESTAMP(JSON_EXTRACT_SCALAR(metadata, '$.kanbanizePayload.timestamp')) as time_created,
      TIMESTAMP(JSON_EXTRACT_SCALAR(metadata, '$.kanbanizePayload.card.closedAt')) as time_resolved,
      JSON_EXTRACT_SCALAR(metadata, '$.kanbanizePayload.card.customFields.RootCause')  as root_cause,
    FROM 
      four_keys.events_raw 
    WHERE 
      event_type = "Bug" 
  ) issue
LEFT JOIN 
  (
    SELECT 
      time_created, changes 
    FROM 
      four_keys.deployments d, 
      d.changes
  ) root on root.changes = root_cause
GROUP BY 
  1,2;