import datetime
from typing import Optional


class RecoverCriteria:
    def __init__(
        self,
        transaction_id: Optional[int] = None,
        timestamp: Optional[datetime.datetime] = None,
    ):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
