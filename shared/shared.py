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
import hashlib
import json

from google.cloud import bigquery


def insert_row_into_bigquery(event):
    if not event:
        raise Exception("No data to insert")

    # Set up bigquery instance
    client = bigquery.Client()
    dataset_id = "four_keys"
    table_id = "events_raw"

    if is_unique(client, event["signature"]):
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)

        # Insert row
        row_to_insert = [
            (
                event["event_type"],
                event["id"],
                event["metadata"],
                event["time_created"],
                event["signature"],
                event["msg_id"],
                event["source"],
            )
        ]
        bq_errors = client.insert_rows(table, row_to_insert)

        # If errors, log to Stackdriver
        if bq_errors:
            entry = {
                "severity": "WARNING",
                "msg": "Row not inserted.",
                "errors": bq_errors,
                "row": row_to_insert,
            }
            print(json.dumps(entry))


def is_unique(client, signature):
    sql = "SELECT signature FROM four_keys.events_raw WHERE signature = '%s'"
    query_job = client.query(sql % signature)
    results = query_job.result()
    return not results.total_rows


def create_unique_id(msg):
    hashed = hashlib.sha1(bytes(json.dumps(msg), "utf-8"))
    return hashed.hexdigest()

def find_last_deploy(source):
    client = bigquery.Client()

    query = (
        "SELECT"
            "source,"
            "id as deploy_id,"
            "time_created,"
            "JSON_EXTRACT_SCALAR(metadata, '$.deployment.sha') as main_commit"
        "FROM "
            "four_keys.events_raw"
        "WHERE "
            "event_type = 'deployment_status' and "
            "JSON_EXTRACT_SCALAR(metadata, '$.deployment_status.state') = 'success' and "
            "source = '%s'"
        "ORDER BY "
            "time_created DESC "
        "LIMIT 1"
    )

    query_job = client.query(query % source)
    result = query_job.result()
    
    return result