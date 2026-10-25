#!/usr/bin/env bash
# Cree le template Elasticsearch pour que les positions soient des geo_point (carte Kibana).
# A lancer UNE fois, avant le simulateur.
set -e
ES=${ES:-http://localhost:9200}

curl -s -X PUT "$ES/_index_template/uber-geo" -H 'Content-Type: application/json' -d '{
  "index_patterns": ["taxi-locations-*", "client-requests-*", "zone-stats"],
  "template": {
    "mappings": {
      "properties": {
        "location":         { "type": "geo_point" },
        "pickup_location":  { "type": "geo_point" },
        "dropoff_location": { "type": "geo_point" },
        "status":     { "type": "keyword" },
        "zone":       { "type": "keyword" },
        "taxi_id":    { "type": "keyword" },
        "request_id": { "type": "keyword" },
        "client_id":  { "type": "keyword" },
        "driver":     { "type": "keyword" },
        "@timestamp": { "type": "date" }
      }
    }
  }
}'
echo
echo "Template uber-geo cree."
