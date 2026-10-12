"""ClickHouse table definitions for the Slack datasets (all loaded from landed Parquet in MinIO)."""

SLACK_TABLES = {
    "bronze_slack_messages": {
        "schema": [
            ("message_id", "string"), ("channel_id", "string"), ("user_id", "string"), ("text", "string"),
            ("message_ts", "string"), ("thread_ts", "string"), ("source", "string"),
            ("processing_date", "date"), ("inserted_at", "datetime"),
        ],
        "engine": "ReplacingMergeTree(inserted_at)",
        "order_by": "(channel_id, message_id)",
        "partition_by": "processing_date",
    },
    "slack_users": {
        "schema": [("user_id", "string"), ("name", "string"), ("real_name", "string")],
        "engine": "ReplacingMergeTree", "order_by": "user_id", "partition_by": None,
    },
    "slack_channels": {
        "schema": [("channel_id", "string"), ("name", "string")],
        "engine": "ReplacingMergeTree", "order_by": "channel_id", "partition_by": None,
    },
    "slack_files": {
        "schema": [
            ("file_id", "string"), ("message_id", "string"), ("channel_id", "string"),
            ("file_name", "string"), ("file_type", "string"), ("size", "int"),
        ],
        "engine": "ReplacingMergeTree", "order_by": "(channel_id, file_id)", "partition_by": None,
    },
    "slack_reactions": {
        "schema": [("message_id", "string"), ("channel_id", "string"), ("emoji", "string"), ("user_id", "string")],
        "engine": "ReplacingMergeTree", "order_by": "(channel_id, message_id, emoji, user_id)", "partition_by": None,
    },
}

# Source column in the landed file -> target column, where they differ.
USERS_RENAMES = {"user_id": "id"}
CHANNELS_RENAMES = {"channel_id": "id"}
