# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import os
import json


import shared

from flask import Flask, request
from google.cloud import bigquery

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    """
    Receives messages from a push subscription from Pub/Sub.
    Parses the message, and inserts it into BigQuery.
    """
    event = None
    envelope = request.get_json()

    # Check that data has been posted
    if not envelope:
        raise Exception("Expecting JSON payload")
    # Check that message is a valid pub/sub message
    if "message" not in envelope:
        raise Exception("Not a valid Pub/Sub Message")
    msg = envelope["message"]

    if "attributes" not in msg:
        raise Exception("Missing pubsub attributes")

    try:
        # [TODO: Replace mock function below]
        event = process_new_source_event(msg)

        # [Do not edit below]
        shared.insert_row_into_bigquery(event)

    except Exception as e:
        entry = {
                "severity": "WARNING",
                "msg": "Data not saved to BigQuery",
                "errors": str(e),
                "json_payload": envelope
            }
        print(json.dumps(entry))

    return "", 204


# [TODO: Replace mock function below]
def process_new_source_event(msg):
    metadata = json.loads(base64.b64decode(msg["data"]).decode("utf-8").strip())

    
    event_type = metadata["kanbanizePayload"]["card"]["typeName"]
    e_id = metadata["kanbanizePayload"]["card"]["taskid"]
    created_at = metadata["kanbanizePayload"]["timestamp"]
    signature = shared.create_unique_id(msg)
    source = metadata["kanbanizePayload"]["card"]["customFields"]["project"]


    if "Sprint Done" in metadata["kanbanizePayload"]["card"]["columnname"]:
        metadata["kanbanizePayload"]["card"].update({ "closedAt":  metadata["kanbanizePayload"]["timestamp"] })
    else:
        deploy = find_last_deploy(source)
        metadata["kanbanizePayload"]["card"].update({ "closedAt":  None, "commit": deploy.main_commit })


    # [TODO: Parse the msg data to map to the event object below]
    kanbanize_event = {
        "event_type": event_type,  # Event type, eg "push", "pull_reqest", etc
        "id": e_id,  # Object ID, eg pull request ID
        "metadata": json.dumps(metadata),  # The body of the msg
        "time_created": created_at,  # The timestamp of with the event
        "signature": signature,  # The unique event signature
        "msg_id": msg["message_id"],  # The pubsub message id
        "source": source,  # The name of the source, eg "github"
    }

    print(kanbanize_event)

    return kanbanize_event

def find_last_deploy(source):
    client = bigquery.Client()

    query = """
        SELECT 
            source, 
            id as deploy_id, 
            time_created, 
            JSON_EXTRACT_SCALAR(metadata, '$.deployment.sha') as main_commit 
        FROM 
            four_keys.events_raw 
        WHERE 
            event_type = 'deployment_status' and 
            JSON_EXTRACT_SCALAR(metadata, '$.deployment_status.state') = 'success' and 
            source = @source 
        ORDER BY 
            time_created DESC 
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source", "STRING", source)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    rows = query_job.result() 

    for row in rows:
        deploy = row

    return deploy

if __name__ == "__main__":
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host="127.0.0.1", port=PORT, debug=True)
