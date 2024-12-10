import unittest
from unittest.mock import patch, mock_open, MagicMock
from datetime import datetime, timedelta
from FailureRecoveryManager import FailureRecoveryManager, ExecutionResult, Rows

class TestFailureRecoveryManager(unittest.TestCase):

    def setUp(self):
        # Set up a fresh FailureRecoveryManager instance for each test
        self.manager = FailureRecoveryManager(base_path="./Storage_Manager/storage")
        self.mock_file = "mock_wal.log"
        
    def test_save_checkpoint_with_memory_wal(self):
        # Arrange
        fixed_time = datetime(2024, 12, 10, 10, 0, 0)
        self.manager.memory_wal = [
            ExecutionResult(
                transaction_id=1,
                timestamp=fixed_time,
                status="ACTIVE",
                before=Rows([{'id': 1, 'name': 'old_value'}], 1),
                after=Rows([{'id': 1, 'name': 'new_value'}], 1),
                query="UPDATE table_name SET name='new_value' WHERE id=1"
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
            results = self.manager.parse_log_file(self.mock_file)

            # Assert
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0].transaction_id, 1)
            self.assertEqual(results[1].status, "CHECKPOINT")

    def test_write_log_commit(self):
        # Arrange
        execution_result = ExecutionResult(
            transaction_id=1,
            timestamp=datetime.now(),
            status="COMMIT",
            before=Rows([{'id': 1, 'name': 'old_value'}], 1),
            after=Rows([{'id': 1, 'name': 'new_value'}], 1),
            query="UPDATE table_name SET name='new_value' WHERE id=1"
        )
        self.manager.memory_wal.append(execution_result)

        with patch("builtins.open", mock_open()) as mocked_file:
            # Act
            self.manager.write_log(execution_result)

            # Assert
            mocked_file.assert_called_once_with(self.manager.log_file, "a")
            self.assertEqual(len(self.manager.memory_wal), 0)  # Memory WAL should be cleared

    def test_start_checkpointing(self):
        # Arrange
        with patch.object(self.manager, "save_checkpoint") as mock_save_checkpoint:
            # Act
            with patch("threading.Thread.start") as mock_thread_start:
                self.manager.start_checkpointing()
                mock_thread_start.assert_called_once()
            # Simulate time passing and the checkpoint being called
            mock_save_checkpoint.assert_not_called()

    def test_build_update_query(self):
        # Arrange
        table_name = "users"
        before = Rows([{"id": 1, "name": "old_value"}], 1)
        after = Rows([{"id": 1, "name": "new_value"}], 1)

        # Act
        queries = self.manager.build_update_query(table_name, before, after)

        # Assert
        expected_query = "UPDATE users SET id='1', name='old_value' WHERE id='1' AND name='new_value';"
        self.assertEqual(queries[0], expected_query)

    def test_build_delete_query(self):
        # Arrange
        table_name = "users"
        after = Rows([{"id": 1, "name": "new_value"}], 1)

        # Act
        queries = self.manager.build_delete_query(table_name, after)

        # Assert
        expected_query = "DELETE FROM users WHERE id='1' AND name='new_value';"
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

    # def test_recover_abort(self):
    #     # Arrange
    #     log_content = (
    #         "ACTIVE,1,2024-12-10T10:00:00,UPDATE table_name SET name='new_value' WHERE id=1,"
    #         "Before: [{'id': 1, 'name': 'old_value'}],After: [{'id': 1, 'name': 'new_value'}]\n"
    #         "ABORT,1,2024-12-10T11:00:00,None,Before: [{'id': 1, 'name': 'old_value'}],After: [{'id': 1, 'name': 'new_value'}]\n"
    #     )
    #     with patch("builtins.open", mock_open(read_data=log_content)):
    #         with patch("builtins.print") as mocked_print:
    #             # Act
    #             self.manager.recover(None)

    #             # Assert
    #             mocked_print.assert_any_call("Executing undo query: UPDATE table_name SET id='1', name='old_value' WHERE id='1' AND name='new_value';\n")

if __name__ == "__main__":
    unittest.main()
