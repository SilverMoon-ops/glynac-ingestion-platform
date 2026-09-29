from __future__ import annotations

from typing import Dict, List, Optional


def mock_channels() -> List[Dict]:
    return [
        {"id": "C001", "name": "general"},
        {"id": "C002", "name": "compliance"},
        {"id": "C003", "name": "sales"},
    ]


def mock_users() -> List[Dict]:
    return [
        {"id": "U001", "name": "alice", "real_name": "Alice Smith"},
        {"id": "U002", "name": "bob", "real_name": "Bob Jones"},
        {"id": "U003", "name": "carol", "real_name": "Carol Wilson"},
    ]


def fetch_history(
    channel_id: str,
    cursor: Optional[int] = None,
    page_size: int = 10,
    total_messages: int = 50,
) -> Dict:
    """
    Mock Slack conversations.history endpoint.

    The cursor is represented as an integer offset. Real Slack uses an opaque
    cursor string, but the checkpoint behavior is equivalent.
    """
    start = int(cursor or 0)
    end = min(start + page_size, total_messages)

    messages = []

    for index in range(start, end):
        message_ts = f"170000{index:05d}.000000"

        messages.append(
            {
                "id": f"{channel_id}:{message_ts}",
                "channel_id": channel_id,
                "user_id": f"U00{(index % 3) + 1}",
                "text": f"Historical message {index} from {channel_id}",
                "message_ts": message_ts,
                "thread_ts": None,
                "source": "historical",
            }
        )

        # Every fifth message receives a mock thread reply.
        if index % 5 == 0:
            messages.append(
                {
                    "id": f"{channel_id}:{message_ts}:reply",
                    "channel_id": channel_id,
                    "user_id": "U002",
                    "text": f"Thread reply to message {index}",
                    "message_ts": f"170000{index:05d}.100000",
                    "thread_ts": message_ts,
                    "source": "historical",
                }
            )

    next_cursor = end if end < total_messages else None

    return {
        "messages": messages,
        "next_cursor": next_cursor,
        "has_more": next_cursor is not None,
    }