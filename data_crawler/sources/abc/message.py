"""
Specifies some general message types that hold the payload as well as some metadata
"""
import dataclasses
from typing import Any, TypeAlias


@dataclasses.dataclass
class Message:
    """Generalized message bundle including the content and some optional metadata"""
    payload: dict[str, Any]  # The actual message content to be delivered to the sink
    metadata: dict[str, Any] = dataclasses.field(default_factory=dict)  # Optional metadata to control the sink


MessageData: TypeAlias = dict[str, Any] | Message
