from datetime import datetime
import FailureRecoveryManager
from RecoverCriteria import RecoverCriteria

if __name__ == "__main__":
    frm = FailureRecoveryManager.FailureRecoveryManager()

    frm.undo_list = [1, 2]


    exec_result_1 = FailureRecoveryManager.ExecutionResult(
        transaction_id=1,
        timestamp=datetime(2024, 11, 22, 10, 0, 0),
        type="UPDATE",
        status="",
        query="UPDATE table_name SET name='new_value' WHERE id=1",
        previous_data=FailureRecoveryManager.Rows([{'id': 1, 'name': 'old_value'}], 1),
        new_data=FailureRecoveryManager.Rows([{'id': 1, 'name': 'new_value'}], 1)
    )
    exec_result_2 = FailureRecoveryManager.ExecutionResult(
        transaction_id=2,
        timestamp=datetime(2024, 11, 22, 10, 5, 0),
        type="INSERT",
        status="",
        query="INSERT INTO table_name (id, name) VALUES (2, 'new_entry')",
        previous_data=FailureRecoveryManager.Rows([], 0),
        new_data=FailureRecoveryManager.Rows([{'id': 2, 'name': 'new_entry'}], 1)
    )
    exec_result_start_1 = FailureRecoveryManager.ExecutionResult(
        transaction_id=1,
        timestamp=datetime(2024, 11, 22, 9, 59, 0),
        type="START",
        status="",
        query=None,
        previous_data=None,
        new_data=None
    )
    exec_result_start_2 = FailureRecoveryManager.ExecutionResult(
        transaction_id=2,
        timestamp=datetime(2024, 11, 22, 9, 58, 0),
        type="START",
        status="",
        query=None,
        previous_data=None,
        new_data=None
    )
    frm.memory_wal.extend([exec_result_start_1, exec_result_start_2, exec_result_2,exec_result_1])


    criteria = RecoverCriteria(transaction_id=[1,2])
    undo_queries = frm.recover(criteria)

    print(undo_queries)