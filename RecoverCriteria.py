import datetime
from typing import Optional,List


class RecoverCriteria:
    def __init__(
        self,
        transaction_id: Optional[List[int]] = None,
        timestamp: Optional[datetime.datetime] = None,
    ):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
