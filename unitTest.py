import sys
import time
import unittest
from unittest.mock import patch, mock_open, MagicMock
from datetime import datetime, timedelta
from FailureRecoveryManager import FailureRecoveryManager, ExecutionResult, Rows
import threading

from RecoverCriteria import RecoverCriteria

class ColoredTextTestResult(unittest.TextTestResult):
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"

    def addSuccess(self, test):
        super().addSuccess(test)
        print(f" {self.GREEN}SUCCESS: {test}{self.RESET}")
        sys.stdout.flush()

    def addFailure(self, test, err):
        super().addFailure(test, err)
        print(f" {self.RED}FAIL: {test}{self.RESET}")
        sys.stdout.flush()

    def addError(self, test, err):
        super().addError(test, err)
        print(f" {self.RED}ERROR: {test}{self.RESET}")
        sys.stdout.flush()


class ColoredTextTestRunner(unittest.TextTestRunner):
    resultclass = ColoredTextTestResult

class TestFailureRecoveryManager(unittest.TestCase):

    def setUp(self):
        # Reset class-level variables before each test
        FailureRecoveryManager._checkpoint_thread_started = False
        self.mock_file = "test.log"
        self.manager = FailureRecoveryManager(log_file=self.mock_file)


    @patch("builtins.open", new_callable=mock_open)
    def test_write_log_commit(self, mocked_file):
        """Test writing a COMMIT log entry."""
        # Arrange
        execution_result = ExecutionResult(
            transaction_id=1,
            timestamp=datetime.now(),
            type="COMMIT",
            previous_data=Rows([{'id': 1, 'name': 'old_value'}], 1),
            new_data=Rows([{'id': 1, 'name': 'new_value'}], 1),
            query="UPDATE table_name SET name='new_value' WHERE id=1",
            status=""
        )


        # Act
        self.manager.write_log(execution_result)

        # Assert
        mocked_file.assert_called_once_with(self.manager.log_file, "a")
        self.assertEqual(len(self.manager.memory_wal), 0)  # Memory WAL should be cleared
        handle = mocked_file()
        handle.write.assert_called_once_with(
            f"COMMIT,1,{execution_result.timestamp.isoformat()},UPDATE table_name SET name='new_value' WHERE id=1,Before: {repr(execution_result.previous_data.data)},After: {repr(execution_result.new_data.data)}\n"
        )

    @patch("builtins.open", new_callable=mock_open)
    def test_write_log_active_with_full_memory_wal(self, mocked_file):
        """Test writing logs when memory WAL is full."""
        # Arrange

        self.manager.wal_size = 1  # Set WAL size to 1 to simulate it being full
        execution_result = ExecutionResult(
            transaction_id=2,
            timestamp=datetime.now(),
            type="ACTIVE",
            previous_data=Rows([{'id': 2, 'name': 'temp_value'}], 1),
            new_data=Rows([{'id': 2, 'name': 'final_value'}], 1),
            query="UPDATE table_name SET name='final_value' WHERE id=2",
            status=""
        )

        # Act
        self.manager.write_log(execution_result)

        # Assert
        mocked_file.assert_called_once_with(self.manager.log_file, "a")
        self.assertEqual(len(self.manager.memory_wal), 0)  # Memory WAL should be cleared
        handle = mocked_file()
        handle.write.assert_called_once_with(
            f"ACTIVE,2,{execution_result.timestamp.isoformat()},UPDATE table_name SET name='final_value' WHERE id=2,Before: {repr(execution_result.previous_data.data)},After: {repr(execution_result.new_data.data)}\n"
        )

    @patch("builtins.open", new_callable=mock_open)
    def test_write_log_add_to_undo_list(self, mocked_file):
        """Test adding a transaction to the undo list when not committed."""
        # Arrange
        execution_result = ExecutionResult(
            transaction_id=3,
            timestamp=datetime.now(),
            type="START",
            previous_data=None,
            new_data=None,
            query=None,
            status=""
        )

        # Act
        self.manager.write_log(execution_result)

        # Assert
        self.assertIn(3, self.manager.undo_list)  # Transaction ID should be added to undo list
        mocked_file.assert_not_called()  # No file operations should be performed for non-COMMIT actions

    @patch("builtins.open", new_callable=mock_open)
    def test_write_log_clear_undo_list_on_commit(self, mocked_file):
        """Test clearing the undo list when a transaction commits."""
        # Arrange
        self.manager.undo_list = [4]
        execution_result = ExecutionResult(
            transaction_id=4,
            timestamp=datetime.now(),
            type="COMMIT",
            previous_data=Rows([{'id': 4, 'value': 'old'}], 1),
            new_data=Rows([{'id': 4, 'value': 'new'}], 1),
            query="UPDATE table_name SET value='new' WHERE id=4",
            status=""
        )


        # Act
        self.manager.write_log(execution_result)

        # Assert
        self.assertNotIn(4, self.manager.undo_list)  # Transaction ID should be removed from undo list
        mocked_file.assert_called_once_with(self.manager.log_file, "a")

    @patch("builtins.open", new_callable=mock_open)
    def test_write_log_multiple_entries(self, mocked_file):
        """Test writing multiple entries from the memory WAL."""
        # Arrange
        execution_result_1 = ExecutionResult(
            transaction_id=5,
            timestamp=datetime.now(),
            type="ACTIVE",
            previous_data=Rows([{'id': 5, 'value': 'temp'}], 1),
            new_data=Rows([{'id': 5, 'value': 'final'}], 1),
            query="UPDATE table_name SET value='final' WHERE id=5",
            status=""
        )
        execution_result_2 = ExecutionResult(
            transaction_id=6,
            timestamp=datetime.now(),
            type="COMMIT",
            previous_data=Rows([{'id': 6, 'value': 'old'}], 1),
            new_data=Rows([{'id': 6, 'value': 'new'}], 1),
            query="UPDATE table_name SET value='new' WHERE id=6",
            status=""
        )
        self.manager.memory_wal.extend([execution_result_1, execution_result_2])

        # Act
        self.manager.write_log(execution_result_2)

        # Assert
        mocked_file.assert_called_once_with(self.manager.log_file, "a")
        self.assertEqual(len(self.manager.memory_wal), 0)  # Memory WAL should be cleared
        handle = mocked_file()
        handle.write.assert_any_call(
            f"ACTIVE,5,{execution_result_1.timestamp.isoformat()},UPDATE table_name SET value='final' WHERE id=5,Before: {repr(execution_result_1.previous_data.data)},After: {repr(execution_result_1.new_data.data)}\n"
        )
        handle.write.assert_any_call(
            f"COMMIT,6,{execution_result_2.timestamp.isoformat()},UPDATE table_name SET value='new' WHERE id=6,Before: {repr(execution_result_2.previous_data.data)},After: {repr(execution_result_2.new_data.data)}\n"
        )

    @patch("builtins.open", new_callable=mock_open)
    def test_write_log_with_full_memory_wal(self, mocked_file):
        """Test writing logs when memory WAL is full with 3 pre-appended entries."""
        # Arrange
        self.manager.wal_size = 4  # Set memory WAL size to 4
        # Prepopulate memory WAL with 3 entries
        execution_result_1 = ExecutionResult(
            transaction_id=1,
            timestamp=datetime.now(),
            type="ACTIVE",
            previous_data=Rows([{'id': 1, 'value': 'old_value_1'}], 1),
            new_data=Rows([{'id': 1, 'value': 'new_value_1'}], 1),
            query="UPDATE table_name SET value='new_value_1' WHERE id=1",
            status=""
        )
        execution_result_2 = ExecutionResult(
            transaction_id=2,
            timestamp=datetime.now(),
            type="ACTIVE",
            previous_data=Rows([{'id': 2, 'value': 'old_value_2'}], 1),
            new_data=Rows([{'id': 2, 'value': 'new_value_2'}], 1),
            query="UPDATE table_name SET value='new_value_2' WHERE id=2",
            status=""
        )
        execution_result_3 = ExecutionResult(
            transaction_id=3,
            timestamp=datetime.now(),
            type="ACTIVE",
            previous_data=Rows([{'id': 3, 'value': 'old_value_3'}], 1),
            new_data=Rows([{'id': 3, 'value': 'new_value_3'}], 1),
            query="UPDATE table_name SET value='new_value_3' WHERE id=3",
            status=""
        )
        self.manager.memory_wal.extend([execution_result_1, execution_result_2, execution_result_3])

        # New entry to write, making the memory full
        execution_result_4 = ExecutionResult(
            transaction_id=4,
            timestamp=datetime.now(),
            type="ACTIVE",
            previous_data=Rows([{'id': 4, 'value': 'old_value_4'}], 1),
            new_data=Rows([{'id': 4, 'value': 'new_value_4'}], 1),
            query="UPDATE table_name SET value='new_value_4' WHERE id=4",
            status=""
        )

        # Act
        self.manager.write_log(execution_result_4)

        # Assert
        mocked_file.assert_called_once_with(self.manager.log_file, "a")  # Log file opened for writing
        self.assertEqual(len(self.manager.memory_wal), 0)  # Memory WAL should be cleared
        handle = mocked_file()
        # Assert the log file writes 4 entries (3 pre-existing + 1 new)
        handle.write.assert_any_call(
            f"ACTIVE,1,{execution_result_1.timestamp.isoformat()},UPDATE table_name SET value='new_value_1' WHERE id=1,Before: {repr(execution_result_1.previous_data.data)},After: {repr(execution_result_1.new_data.data)}\n"
        )
        handle.write.assert_any_call(
            f"ACTIVE,2,{execution_result_2.timestamp.isoformat()},UPDATE table_name SET value='new_value_2' WHERE id=2,Before: {repr(execution_result_2.previous_data.data)},After: {repr(execution_result_2.new_data.data)}\n"
        )
        handle.write.assert_any_call(
            f"ACTIVE,3,{execution_result_3.timestamp.isoformat()},UPDATE table_name SET value='new_value_3' WHERE id=3,Before: {repr(execution_result_3.previous_data.data)},After: {repr(execution_result_3.new_data.data)}\n"
        )
        handle.write.assert_any_call(
            f"ACTIVE,4,{execution_result_4.timestamp.isoformat()},UPDATE table_name SET value='new_value_4' WHERE id=4,Before: {repr(execution_result_4.previous_data.data)},After: {repr(execution_result_4.new_data.data)}\n"
        )
        self.assertEqual(handle.write.call_count, 4)  # Ensure write was called 4 times

    def test_save_checkpoint_with_memory_wal(self):
        # Arrange
        fixed_time = datetime(2024, 12, 10, 10, 0, 0)
        self.manager.memory_wal = [
            ExecutionResult(
                transaction_id=1,
                timestamp=fixed_time,
                type="ACTIVE",
                previous_data=Rows([{'id': 1, 'name': 'old_value'}], 1),
                new_data=Rows([{'id': 1, 'name': 'new_value'}], 1),
                query="UPDATE table_name SET name='new_value' WHERE id=1",
                status=""
            )
        ]

        with patch("builtins.open", mock_open()) as mocked_file:
            with patch("datetime.datetime") as mock_datetime:
                # Mock datetime.now() to return fixed_time
                mock_datetime.now.return_value = fixed_time
                mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

                # Act
                self.manager.save_checkpoint()

                # Assert
                # Ensure open was called twice: once for memory_wal entries, once for the checkpoint
                self.assertEqual(mocked_file.call_count, 2)

                # Verify the contents of the writes
                handle = mocked_file()
                handle.write.assert_any_call(
                    "ACTIVE,1,2024-12-10T10:00:00,UPDATE table_name SET name='new_value' WHERE id=1,"
                    "Before: [{'id': 1, 'name': 'old_value'}],After: [{'id': 1, 'name': 'new_value'}]\n"
                )
                handle.write.assert_any_call('CHECKPOINT,2024-12-10T10:00:00,[]\n')
    def test_parse_log_file_with_valid_logs(self):
        # Arrange
        log_content = (
            "ACTIVE,1,2024-12-10T10:00:00,UPDATE table_name SET name='new_value' WHERE id=1,"
            "Before: [{'id': 1, 'name': 'old_value'}],After: [{'id': 1, 'name': 'new_value'}]\n"
            "CHECKPOINT,2024-12-10T11:00:00,[1, 2, 3]\n"
        )
        with patch("builtins.open", mock_open(read_data=log_content)):
            # Act
            results, undo_list = self.manager.parse_log_file(self.mock_file)

            # Assert
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0].transaction_id, 1)
            self.assertEqual(results[1].type, "CHECKPOINT")



    @patch("builtins.open", new_callable=mock_open)
    @patch("datetime.datetime")
    def test_save_checkpoint(self, mock_datetime, mocked_file):
        """Test saving a checkpoint manually."""
        # Arrange
        fixed_time = datetime(2024, 12, 11, 12, 0, 0)
        mock_datetime.now.return_value = fixed_time
        log_entry = ExecutionResult(
            transaction_id=1,
            timestamp=fixed_time,
            type="ACTIVE",
            previous_data=Rows([{'id': 1, 'value': 'old_value'}], 1),
            new_data=Rows([{'id': 1, 'value': 'new_value'}], 1),
            query="UPDATE table_name SET value='new_value' WHERE id=1",
            status=""
        )
        self.manager.memory_wal = [log_entry]

        # Act
        self.manager.save_checkpoint()

        # Assert
        self.assertEqual(mocked_file.call_count, 2)  # Ensure file opened twice
        handle = mocked_file()
        handle.write.assert_any_call(
            f"ACTIVE,1,{fixed_time.isoformat()},UPDATE table_name SET value='new_value' WHERE id=1,Before: {repr(log_entry.previous_data.data)},After: {repr(log_entry.new_data.data)}\n"
        )
        handle.write.assert_any_call(f"CHECKPOINT,{fixed_time.isoformat()},[]\n")
        self.assertEqual(len(self.manager.memory_wal), 0)  # Ensure memory WAL is cleared


    @patch("builtins.open", new_callable=mock_open)
    @patch("datetime.datetime")
    def test_save_checkpoint_with_empty_memory_wal(self, mock_datetime, mocked_file):
        """Test saving a checkpoint with an empty memory WAL."""
        # Arrange
        fixed_time = datetime(2024, 12, 11, 12, 0, 0)
        mock_datetime.now.return_value = fixed_time
        self.manager.memory_wal = []

        # Act
        self.manager.save_checkpoint()

        # Assert
        self.assertEqual(mocked_file.call_count, 1)  # Only checkpoint log is written
        handle = mocked_file()
        handle.write.assert_called_once_with(f"CHECKPOINT,{fixed_time.isoformat()},[]\n")

    @patch("builtins.open", new_callable=mock_open)
    @patch("datetime.datetime")
    def test_save_checkpoint_with_undo_list(self, mock_datetime, mocked_file):
        """Test saving a checkpoint with an undo list."""
        # Arrange
        fixed_time = datetime(2024, 12, 11, 12, 0, 0)
        mock_datetime.now.return_value = fixed_time
        self.manager.undo_list = [1, 2, 3]

        # Act
        self.manager.save_checkpoint()

        # Assert
        self.assertEqual(mocked_file.call_count, 1)  # Only checkpoint log is written
        handle = mocked_file()
        handle.write.assert_called_once_with(f"CHECKPOINT,{fixed_time.isoformat()},[1, 2, 3]\n")


    def test_build_update_query(self):
        # Arrange
        table_name = "users"
        before = Rows([{"id": 1, "name": "old_value"}], 1)
        after = Rows([{"id": 1, "name": "new_value"}], 1)

        # Act
        queries = self.manager.build_update_query(table_name, before, after)

        # Assert
        expected_query = "UPDATE users SET id=1, name='old_value' WHERE id=1 AND name='new_value';"
        self.assertEqual(queries[0], expected_query)

    def test_build_delete_query(self):
        # Arrange
        table_name = "users"
        after = Rows([{"id": 1, "name": "new_value"}], 1)

        # Act
        queries = self.manager.build_delete_query(table_name, after)

        # Assert
        expected_query = "DELETE FROM users WHERE id=1 AND name='new_value';"
        self.assertEqual(queries[0], expected_query)

    def test_build_insert_query(self):
        # Arrange
        table_name = "users"
        before = Rows([{"id": 1, "name": "old_value"}], 1)
        after = Rows([{"id": 1, "name": "new_value"}], 1)

        # Act
        queries = self.manager.build_insert_query(table_name, before, after)

        # Assert
        expected_query = "INSERT INTO users (id, name) VALUES (1, 'old_value');"
        self.assertEqual(queries[0], expected_query)

    @patch("builtins.print")
    def test_recover_no_criteria(self, mock_print):
        """Test recover with no transaction IDs provided."""
        # Arrange
        criteria = RecoverCriteria(transaction_id=[])

        # Act
        undo_queries = self.manager.recover(criteria)

        self.assertEqual(undo_queries, [])

    @patch("builtins.print")
    def test_recover_empty_undo_list(self, mock_print):
        """Test recover with transaction IDs not present in undo list."""
        # Arrange
        criteria = RecoverCriteria(transaction_id=[1, 2])
        self.manager.undo_list = [3, 4]  # Different transaction IDs
        # Act
        undo_queries = self.manager.recover(criteria)
        # Assert
        self.assertEqual(undo_queries, [])

    @patch("FailureRecoveryManager.FailureRecoveryManager.parse_log_file")
    def test_recover_stops_at_start(self, mock_parse_log_file):
        """Test that recovery stops when encountering a START log entry."""
        # Arrange
        criteria = RecoverCriteria(transaction_id=[1])
        self.manager.undo_list = [1]

        # Memory WAL has transaction 1 active and a START entry
        exec_result_1 = ExecutionResult(
            transaction_id=1,
            timestamp=datetime(2024, 11, 22, 10, 0, 0),
            type="UPDATE",
            status="",
            query="UPDATE table_name SET name='new_value' WHERE id=1",
            previous_data=Rows([{'id': 1, 'name': 'old_value'}], 1),
            new_data=Rows([{'id': 1, 'name': 'new_value'}], 1)
        )
        exec_result_start = ExecutionResult(
            transaction_id=1,
            timestamp=datetime(2024, 11, 22, 9, 59, 0),
            type="START",
            status="",
            query=None,
            previous_data=None,
            new_data=None
        )
        self.manager.memory_wal.extend([exec_result_start, exec_result_1])

        # Mock log file parsing to include no relevant entries
        mock_parse_log_file.return_value = ([], [])

        # Act
        undo_queries = self.manager.recover(criteria)

        # Assert
        expected_queries = [
           [1, "UPDATE table_name SET id=1, name='old_value' WHERE id=1 AND name='new_value';"]
        ]
        self.assertEqual(undo_queries, expected_queries)

    @patch("FailureRecoveryManager.FailureRecoveryManager.parse_log_file")
    def test_recover_uses_log_file_if_needed(self, mock_parse_log_file):
        """Test that recovery uses the log file if memory WAL does not contain START."""
        # Arrange
        criteria = RecoverCriteria(transaction_id=[1])
        self.manager.undo_list = [1]

        # Memory WAL has transaction 1 active but no START entry
        exec_result_1 = ExecutionResult(
            transaction_id=1,
            timestamp=datetime(2024, 11, 22, 10, 0, 0),
            type="UPDATE",
            status="",
            query="UPDATE table_name SET name='new_value' WHERE id=1",
            previous_data=Rows([{'id': 1, 'name': 'old_value'}], 1),
            new_data=Rows([{'id': 1, 'name': 'new_value'}], 1)
        )
        self.manager.memory_wal.append(exec_result_1)

        # Mock log file parsing to include the START entry
        exec_result_start = ExecutionResult(
            transaction_id=1,
            timestamp=datetime(2024, 11, 22, 9, 59, 0),
            type="START",
            status="",
            query=None,
            previous_data=None,
            new_data=None
        )
        mock_parse_log_file.return_value = ([exec_result_start], [1])

        # Act
        undo_queries = self.manager.recover(criteria)

        # Assert
        expected_queries = [
            [1,"UPDATE table_name SET id=1, name='old_value' WHERE id=1 AND name='new_value';"]
        ]
        self.assertEqual(undo_queries, expected_queries)
        mock_parse_log_file.assert_called_once_with(self.mock_file)

    @patch("FailureRecoveryManager.FailureRecoveryManager.parse_log_file")
    def test_recover_handles_multiple_transactions(self, mock_parse_log_file):
        """Test recovery with multiple transactions in the undo list."""
        # Arrange
        criteria = RecoverCriteria(transaction_id=[1, 2])
        self.manager.undo_list = [1, 2]

        # Memory WAL has transaction 1 active and 2 without START
        exec_result_1 = ExecutionResult(
            transaction_id=1,
            timestamp=datetime(2024, 11, 22, 10, 0, 0),
            type="UPDATE",
            status="",
            query="UPDATE table_name SET name='new_value' WHERE id=1;",
            previous_data=Rows([{'id': 1, 'name': 'old_value'}], 1),
            new_data=Rows([{'id': 1, 'name': 'new_value'}], 1)
        )
        exec_result_2 = ExecutionResult(
            transaction_id=2,
            timestamp=datetime(2024, 11, 22, 10, 5, 0),
            type="INSERT",
            status="",
            query="INSERT INTO table_name (id, name) VALUES (2, 'new_entry');",
            previous_data=Rows([], 0),
            new_data=Rows([{'id': 2, 'name': 'new_entry'}], 1)
        )
        self.manager.memory_wal.extend([exec_result_1, exec_result_2])

        # Mock log file parsing to include START for both transactions
        exec_result_start_1 = ExecutionResult(
            transaction_id=1,
            timestamp=datetime(2024, 11, 22, 9, 59, 0),
            type="START",
            status="",
            query=None,
            previous_data=None,
            new_data=None
        )
        exec_result_start_2 = ExecutionResult(
            transaction_id=2,
            timestamp=datetime(2024, 11, 22, 9, 58, 0),
            type="START",
            status="",
            query=None,
            previous_data=None,
            new_data=None
        )
        mock_parse_log_file.return_value = ([exec_result_start_1, exec_result_start_2],[])

        # Act
        undo_queries = self.manager.recover(criteria)

        # Assert
        expected_queries = [
            [2,"DELETE FROM table_name WHERE id=2 AND name='new_entry';"],
            [1,"UPDATE table_name SET id=1, name='old_value' WHERE id=1 AND name='new_value';"],
            
        ]
        self.assertEqual(undo_queries, expected_queries)
        mock_parse_log_file.assert_called_once_with(self.mock_file)

    @patch("FailureRecoveryManager.FailureRecoveryManager.parse_log_file")
    def test_recover_handles_delete(self, mock_parse_log_file):
        """Test recovery handles DELETE operation."""
        # Arrange
        criteria = RecoverCriteria(transaction_id=[3])
        self.manager.undo_list = [3]

        # Memory WAL has transaction 3 with a DELETE operation
        exec_result_delete = ExecutionResult(
            transaction_id=3,
            timestamp=datetime(2024, 11, 22, 10, 10, 0),
            type="DELETE",
            status="",
            query="DELETE FROM table_name WHERE id=3;",
            previous_data=Rows([{'id': 3, 'name': 'deleted_entry'}], 1),
            new_data=Rows([], 0)
        )
        self.manager.memory_wal.append(exec_result_delete)

        # Mock log file parsing to include START entry for transaction 3
        exec_result_start = ExecutionResult(
            transaction_id=3,
            timestamp=datetime(2024, 11, 22, 9, 55, 0),
            type="START",
            status="",
            query=None,
            previous_data=None,
            new_data=None
        )
        mock_parse_log_file.return_value = ([exec_result_start],[])

        # Act
        undo_queries = self.manager.recover(criteria)

        # Assert
        expected_queries = [
            [3,"INSERT INTO table_name (id, name) VALUES (3, 'deleted_entry');"]
        ]
        self.assertEqual(undo_queries, expected_queries)
        mock_parse_log_file.assert_called_once_with(self.mock_file)

    @patch("FailureRecoveryManager.FailureRecoveryManager.parse_log_file")
    @patch("builtins.print")
    def test_recover_file_transactions(self, mock_print, mock_parse_log_file):
        """Test recover with aborted transactions present in the log file."""
        # Arrange
        criteria = RecoverCriteria(transaction_id=[1])
        self.manager.undo_list = [1, 2]

        # Memory WAL has transaction 1 active
        exec_result_1 = ExecutionResult(
            transaction_id=1,
            timestamp=datetime(2024, 11, 22, 10, 0, 0),
            type="UPDATE",
            status="",
            query="UPDATE table_name SET name='new_value' WHERE id=1;",
            previous_data=Rows([{'id': 1, 'name': 'old_value'}], 1),
            new_data=Rows([{'id': 1, 'name': 'new_value'}], 1)
        )
        self.manager.memory_wal.append(exec_result_1)

        # Mock log file parsing to include transaction 2 as aborted
        exec_result_2 = ExecutionResult(
            transaction_id=1,
            timestamp=datetime(2024, 11, 22, 10, 15, 0),
            type="START",
            status="",
            query=None,
            previous_data=Rows([], 0),
            new_data=Rows([], 0)
        )
        mock_parse_log_file.return_value = ([exec_result_2],[])

        # Act
        undo_queries = self.manager.recover(criteria)
        # Assert
        expected_queries = [
            [1, "UPDATE table_name SET id=1, name='old_value' WHERE id=1 AND name='new_value';"]
        ]
        self.assertEqual(undo_queries, expected_queries)
        mock_parse_log_file.assert_called_once_with(self.mock_file)


    @patch("builtins.print")
    def test_recover_partial_transactions_in_memory_wal(self, mock_print):
        """Test recover where some transactions are in memory_wal and some are not."""
        # Arrange
        criteria = RecoverCriteria(transaction_id=[1, 2, 3])
        self.manager.undo_list = [1, 2, 3]

        # Memory WAL has transaction 1 active
        exec_result_1 = ExecutionResult(
            transaction_id=1,
            timestamp=datetime(2024, 11, 22, 10, 0, 0),
            type="UPDATE",
            status="",
            query="UPDATE table_name SET name='new_value1' WHERE id=1;",
            previous_data=Rows([{'id': 1, 'name': 'old_value1'}], 1),
            new_data=Rows([{'id': 1, 'name': 'new_value1'}], 1)
        )
        self.manager.memory_wal.append(exec_result_1)

        # Mock parse_log_file to have transaction 2 in log file
        exec_result_2 = ExecutionResult(
            transaction_id=2,
            timestamp=datetime(2024, 11, 22, 10, 5, 0),
            type="INSERT",
            status="",
            query="INSERT INTO table_name (id, name) VALUES (2, 'value2');",
            previous_data=Rows([], 0),
            new_data=Rows([{'id': 2, 'name': 'value2'}], 1)
        )
        exec_result_3 = ExecutionResult(
            transaction_id=3,
            timestamp=datetime(2024, 11, 22, 10, 10, 0),
            type="START",
            status="",
            query=None,
            previous_data=Rows([], 0),
            new_data=Rows([], 0)
        )
        with patch.object(self.manager, 'parse_log_file', return_value=([exec_result_2, exec_result_3],[])):
            # Act
            undo_queries = self.manager.recover(criteria)

            # Assert
            expected_queries = [
                [1,"UPDATE table_name SET id=1, name='old_value1' WHERE id=1 AND name='new_value1';"],
                [2,"DELETE FROM table_name WHERE id=2 AND name='value2';"]
            ]
            self.assertEqual(undo_queries, expected_queries)


    @patch("FailureRecoveryManager.FailureRecoveryManager.parse_log_file")
    @patch("builtins.print")
    def test_recover_no_log_file(self, mock_print, mock_parse_log_file):
        """Test recover when log file does not exist."""
        # Arrange
        criteria = RecoverCriteria(transaction_id=[1])
        self.manager.undo_list = [1]

        # Memory WAL has transaction 1 active
        exec_result_1 = ExecutionResult(
            transaction_id=1,
            timestamp=datetime(2024, 11, 22, 10, 0, 0),
            type="ACTIVE",
            status="DELETE",
            query="DELETE FROM table_name WHERE id=1;",
            previous_data=Rows([{'id': 1, 'name': 'value1'}], 1),
            new_data=Rows([], 0)
        )
        self.manager.memory_wal.append(exec_result_1)

        # Mock parse_log_file to raise an exception indicating missing log file
        mock_parse_log_file.side_effect = Exception("No log file. Abort recovery.")

        # Act
        undo_queries = self.manager.recover(criteria)

        # Assert
        mock_print.assert_any_call("Error during recovery: No log file. Abort recovery.")
        self.assertEqual(undo_queries, [])

    @patch("FailureRecoveryManager.FailureRecoveryManager.parse_log_file")
    @patch("builtins.print")
    def test_recover_start_transaction_only(self, mock_print, mock_parse_log_file):
        """Test recover where a transaction has only a START log entry."""
        # Arrange
        criteria = RecoverCriteria(transaction_id=[1])
        self.manager.undo_list = [1]

        # Memory WAL has transaction 1 as START
        exec_result_1 = ExecutionResult(
            transaction_id=1,
            timestamp=datetime(2024, 11, 22, 10, 0, 0),
            type="START",
            status="",
            query=None,
            previous_data=Rows([], 0),
            new_data=Rows([], 0)
        )
        self.manager.memory_wal.append(exec_result_1)

        # Act
        undo_queries = self.manager.recover(criteria)

        # Assert
        expected_queries = []
        self.assertEqual(undo_queries, expected_queries)
        mock_parse_log_file.assert_not_called()

    def test_recovery_scenario(self):

        before_insert_data = Rows(data=[], rows_count=0)
        after_insert_data = Rows(data=[{"id": 1, "name": "Alice"}], rows_count=1)

        before_update_data = Rows(data=[{"id": 1, "name": "Alice"}], rows_count=1)
        after_update_data = Rows(data=[{"id": 1, "name": "Alicia"}], rows_count=1)

        before_delete_data = Rows(data=[{"id": 2, "name": "Bob"}], rows_count=1)
        after_delete_data = Rows(data=[], rows_count=0)

        # Transaction 100: Will be fully committed
        # START
        start_100 = ExecutionResult(
            transaction_id=100,
            timestamp=datetime.now(),
            type="START",
            status="",
            query=None,
            previous_data=before_insert_data,
            new_data=after_insert_data
        )
        self.manager.write_log(start_100)

        # INSERT
        insert_100 = ExecutionResult(
            transaction_id=100,
            timestamp=datetime.now(),
            type="INSERT",
            status="",
            query="INSERT INTO test (id, name) VALUES (1,'Alice');",
            previous_data=before_insert_data,
            new_data=after_insert_data
        )
        self.manager.write_log(insert_100)

        # UPDATE
        update_100 = ExecutionResult(
            transaction_id=100,
            timestamp=datetime.now(),
            type="UPDATE",
            status="",
            query="UPDATE test SET name='Alicia' WHERE id=1;",
            previous_data=before_update_data,
            new_data=after_update_data
        )
        self.manager.write_log(update_100)

        # COMMIT transaction 100
        commit_100 = ExecutionResult(
            transaction_id=100,
            timestamp=datetime.now(),
            type="COMMIT",
            status="",
            query="COMMIT;",
            previous_data=before_insert_data,
            new_data=after_insert_data
        )
        self.manager.write_log(commit_100)

        # Transaction 101: Will not commit
        start_101 = ExecutionResult(
            transaction_id=101,
            timestamp=datetime.now(),
            type="START",
            status="",
            query="",
            previous_data=before_insert_data,
            new_data=after_insert_data
        )
        self.manager.write_log(start_101)

        insert_101 = ExecutionResult(
            transaction_id=101,
            timestamp=datetime.now(),
            type="INSERT",
            status="",
            query="INSERT INTO test (id, name) VALUES (3,'Charlie');",
            previous_data=before_insert_data,
            new_data=Rows(data=[{"id": 3, "name": "Charlie"}], rows_count=1)
        )
        self.manager.write_log(insert_101)

        update_101 = ExecutionResult(
            transaction_id=101,
            timestamp=datetime.now(),
            type="UPDATE",
            status="",
            query="UPDATE test SET name='Charles' WHERE id=3;",
            previous_data=Rows(data=[{"id": 3, "name": "Charlie"}], rows_count=1),
            new_data=Rows(data=[{"id": 3, "name": "Charles"}], rows_count=1)
        )
        self.manager.write_log(update_101)
        # No COMMIT for transaction 101

        # Transaction 102: Will not commit (DELETE case)
        start_102 = ExecutionResult(
            transaction_id=102,
            timestamp=datetime.now(),
            type="START",
            status="",
            query=None,
            previous_data=before_delete_data,
            new_data=after_delete_data
        )
        self.manager.write_log(start_102)
        
        start_104 = ExecutionResult(
            transaction_id=104,
            timestamp=datetime.now(),
            type="START",
            status="",
            query=None,
            previous_data=before_delete_data,
            new_data=after_delete_data
        )
        self.manager.write_log(start_104)
        delete_102 = ExecutionResult(
            transaction_id=102,
            timestamp=datetime.now(),
            type="DELETE",
            status="",
            query="DELETE FROM test WHERE id=2;",
            previous_data=before_delete_data,
            new_data=after_delete_data
        )
        self.manager.write_log(delete_102)

        self.manager.save_checkpoint()

        criteria = RecoverCriteria(transaction_id=[101, 102], timestamp=None)
        undo_queries = self.manager.recover(criteria)


        expected_101_update_undo = "UPDATE test SET id=3, name='Charlie' WHERE id=3 AND name='Charles';"
        expected_101_insert_undo = "DELETE FROM test WHERE id=3 AND name='Charlie';"   
        expected_102_delete_undo = "INSERT INTO test (id, name) VALUES (2, 'Bob');"
        received_101 = [q for (tid, q) in undo_queries if tid == 101]
        received_102 = [q for (tid, q) in undo_queries if tid == 102]

        self.assertEqual(len(received_101), 2)
        self.assertEqual(len(received_102), 1)

        self.assertIn(expected_101_update_undo, received_101)
        self.assertIn(expected_101_insert_undo, received_101)
        self.assertIn(expected_102_delete_undo, received_102)

    @patch('FailureRecoveryManager.FailureRecoveryManager.parse_log_file')
    def test_recoverSystem_checkpoint_redo_undo(self, mock_parse_log_file):
        """Test recovery system with checkpoint, redo, and undo operations."""
        logs = [
            ExecutionResult(
                transaction_id=2,
                timestamp=datetime.now() - timedelta(minutes=20),
                type="START",
                status="",
                query=None,
                previous_data=None,
                new_data=None
            ),
            ExecutionResult(
                transaction_id=2,
                timestamp=datetime.now() - timedelta(minutes=20),
                type="DELETE",
                status="",
                query="DELETE FROM table1 where id=1;",
                previous_data=Rows(data=[{"id": 1, "name": "Bob"}], rows_count=1),
                new_data=None
            ),
            ExecutionResult(
                transaction_id=None,
                timestamp=datetime.now() - timedelta(minutes=15),
                type="CHECKPOINT",
                status="",
                query=None,
                previous_data=None,
                new_data=None
            ),
            ExecutionResult(
                transaction_id=1,
                timestamp=datetime.now() - timedelta(minutes=10),
                type="START",
                status="",
                query=None,
                previous_data=None,
                new_data=None
            ),
            ExecutionResult(
                transaction_id=1,
                timestamp=datetime.now() - timedelta(minutes=9),
                type="INSERT",
                status="",
                query="INSERT INTO table1 (id, name) VALUES (1, 'Alice');",
                previous_data=None,
                new_data=Rows(data=[{"id": 1, "name": "Alice"}], rows_count=1)
            ),
            ExecutionResult(
                transaction_id=2,
                timestamp=datetime.now() - timedelta(minutes=8),
                type="UPDATE",
                status="",
                query="UPDATE table2 SET age=30 WHERE id=2;",
                previous_data=Rows(data=[{"id": 2, "age": 25}], rows_count=1),
                new_data=Rows(data=[{"id": 2, "age": 30}], rows_count=1)
            ),
            ExecutionResult(
                transaction_id=1,
                timestamp=datetime.now() - timedelta(minutes=7),
                type="COMMIT",
                status="",
                query=None,
                previous_data=None,
                new_data=None
            )
        ]
        mock_parse_log_file.return_value = (logs, [2])

        redo_query, undo_query = self.manager.recoverSystem()

        self.assertEqual(len(redo_query), 2)
        self.assertIn("INSERT INTO table1 (id, name) VALUES (1, 'Alice');", redo_query[0][1])
        self.assertIn("UPDATE table2 SET age=30 WHERE id=2;", redo_query[1][1])
        self.assertEqual(len(undo_query), 2)
        self.assertIn("UPDATE table2 SET id=2, age=25 WHERE id=2 AND age=30;", undo_query[0][1])
        self.assertIn("INSERT INTO table1 (id, name) VALUES (1, 'Bob');", undo_query[1][1])

if __name__ == "__main__":
    unittest.main(testRunner=ColoredTextTestRunner())
