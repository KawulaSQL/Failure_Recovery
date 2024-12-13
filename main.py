from datetime import datetime
import time
import FailureRecoveryManager
from RecoverCriteria import RecoverCriteria

if __name__ == "__main__":
    frm = FailureRecoveryManager.FailureRecoveryManager()
    exec_result_1 = FailureRecoveryManager.ExecutionResult(
        transaction_id=1,
        timestamp=datetime(2024, 11, 22, 10, 0, 0),
        type="UPDATE",
        status="",
        query="UPDATE table_name SET name='new_value' WHERE id=1;",
        previous_data=FailureRecoveryManager.Rows([{'id': 1, 'name': 'old_value'}], 1),
        new_data=FailureRecoveryManager.Rows([{'id': 1, 'name': 'new_value'}], 1)
    )
    exec_result_2 = FailureRecoveryManager.ExecutionResult(
        transaction_id=2,
        timestamp=datetime(2024, 11, 22, 10, 5, 0),
        type="INSERT",
        status="",
        query="INSERT INTO table_name (id, name) VALUES (2, 'new_entry');",
        previous_data=FailureRecoveryManager.Rows([], 0),
        new_data=FailureRecoveryManager.Rows([{'id': 2, 'name': 'new_entry'}], 1)
    )
    exec_result_start_1 = FailureRecoveryManager.ExecutionResult(
        transaction_id=1,
        timestamp=datetime(2024, 11, 22, 9, 59, 0),
        type="START",
        status="",
        query=None,
        previous_data=FailureRecoveryManager.Rows([], 0),
        new_data=FailureRecoveryManager.Rows([], 0)
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

    frm.write_log(exec_result_start_1)
    frm.write_log(exec_result_start_2)
    frm.write_log(exec_result_1)
    frm.write_log(exec_result_2)


    

    exec_result_21 = FailureRecoveryManager.ExecutionResult(
        transaction_id=2,
        timestamp=datetime(2024, 11, 22, 10, 58, 0),
        type="COMMIT",
        status="",
        query=None,
        previous_data=None,
        new_data=None,
    )
    frm.write_log(exec_result_21)
    exec_result_212 = FailureRecoveryManager.ExecutionResult(
        transaction_id=1,
        timestamp=datetime(2024, 11, 22, 10, 58, 0),
        type="INSERT",
        status="",
        query="INSERT INTO table_name (id, name) VALUES (3, 'new_entry');",
        previous_data=None,
        new_data=FailureRecoveryManager.Rows([{'id': 3, 'name': 'new_entry'}], 1),
    )
    frm.write_log(exec_result_212)
    criteria = RecoverCriteria(transaction_id=[1])
    undo_queries = frm.recover(criteria)

    print(undo_queries)


    while True:
        time.sleep(1)  # Sleep for 1 second to avoid busy-waiting