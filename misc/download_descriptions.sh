#!/bin/bash

PROJECT_ID="cymbal-telco-da"
DATASET_ID="cymbal_telco_dataset"

TABLE_NAMES=(
    "browsing_mobile_behaviour"
    "fibre_behaviour"
    "fibre_connected_devices_info"
    "file_access_mobile_behaviour"
    "gaming_mobile_behaviour"
    "im_mobile_behaviour"
    "other_mobile_behaviour"
    "streaming_mobile_behaviour"
    "subscribers_info"
    "voip_mobile_behaviour"
)

OUTPUT_DIR="descriptions"

for TABLE_NAME in "${TABLE_NAMES[@]}"; do
    echo "Processing table: ${PROJECT_ID}:${DATASET_ID}.${TABLE_NAME}"

    # Get table details (including table description)
    bq show --format=prettyjson "${PROJECT_ID}:${DATASET_ID}.${TABLE_NAME}" > "${OUTPUT_DIR}/${TABLE_NAME}_table_details.json"

    # Get schema details (including column descriptions)
    bq show --schema --format=prettyjson "${PROJECT_ID}:${DATASET_ID}.${TABLE_NAME}" > "${OUTPUT_DIR}/${TABLE_NAME}_table_schema.json"

    echo "  - Table details saved to ${OUTPUT_DIR}/${TABLE_NAME}_table_details.json"
    echo "  - Schema details saved to ${OUTPUT_DIR}/${TABLE_NAME}_table_schema.json"
done

echo "All descriptions downloaded to the '$OUTPUT_DIR' directory."