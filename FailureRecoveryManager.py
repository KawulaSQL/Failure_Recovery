import datetime
import os
import re
from typing import Generic, TypeVar, List

T = TypeVar('T')

class Rows(Generic[T]):
    def __init__(self, data: List[T], rows_count: int):
        self.data = data
        self.rows_count = rows_count

class ExecutionResult:
    def __init__(self, transaction_id: int, timestamp: datetime, message: str, before: Rows, data: Rows, query: str):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.message = message
        self.before = before
        self.data = data
        self.query = query
from RecoverCriteria import RecoverCriteria

class FailureRecoveryManager:
    def __init__(self, log_file='wal.log', buffer_size=10):
        self.memory_wal = []  # In-memory WAL
        self.log_file = log_file
        self.buffer = []  # Buffer to hold modified data
        self.buffer_size = buffer_size  # Maximum size of the buffer
        self.last_checkpoint_time = datetime.datetime.now()
        self.checkpoint_interval = datetime.timedelta(minutes=5)
        
        # Initialize log file if they don't exist
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                pass
            
    def parse_log_file(self, file_path: str) -> List[ExecutionResult]:
        execution_results = []
        with open(file_path, 'r') as file:
            for line in file:
                match = re.match(r"(\w+),(\d+),([\d\-T:]+),Query: (.+?),Before: (.+?),After: (.+)", line)
                if match:
                    message = match.group(1)
                    transaction_id = int(match.group(2))
                    timestamp = datetime.datetime.fromisoformat(match.group(3))
                    query = match.group(4)

                    # Parse Before and After data
                    before_data = eval(match.group(5))  # [{'id': 1, 'name': 'old_value'}]
                    after_data = eval(match.group(6))   # [{'id': 1, 'name': 'new_value'}]

                    before_rows = Rows(data=before_data, rows_count=len(before_data))
                    after_rows = Rows(data=after_data, rows_count=len(after_data))

                    execution_result = ExecutionResult(
                        transaction_id=transaction_id,
                        timestamp=timestamp,
                        message=message,
                        before=before_rows,
                        data=after_rows,
                        query=query
                    )
                    execution_results.append(execution_result)
        return execution_results

    def write_log(self, info: ExecutionResult) -> None:
        log_entry = f"{info.message},{info.transaction_id},{info.timestamp.isoformat()},Query: {info.query},Before: {info.before.data},After: {info.data.data}"
        
        self.memory_wal.append(log_entry)
        
        if len(self.memory_wal) >= self.buffer_size:
            with open(self.log_file, 'a') as log_file:
                for entry in self.memory_wal:
                    log_file.write(entry + '\n')
            self.memory_wal.clear()

    # Get the table name from the query
    def get_table_name(self, query: str) -> str:
        match = re.search(r"FROM\s+(\w+)|INTO\s+(\w+)|UPDATE\s+(\w+)", query, re.IGNORECASE)
        if match:
            return match.group(1) or match.group(2) or match.group(3)
        return ""
    
    # Build an UPDATE query to undo to the 'before' state
    def build_update_query(self, table_name: str, before: Rows, after: Rows) -> List[str]:
        queries = []
        for before_row, after_row in zip(before.data, after.data):
            set_clause = ", ".join(f"{k}='{v}'" for k, v in before_row.items())
            condition = " AND ".join(f"{k}='{v}'" for k, v in after_row.items())
            query = f"UPDATE {table_name} SET {set_clause} WHERE {condition};"
            queries.append(query)
        
        return queries

    # Build a DELETE query to undo an INSERT operation
    def build_delete_query(self, table_name: str, after: Rows) -> List[str]:
        condition = " AND ".join(f"{k}='{v}'" for row in after.data for k, v in row.items())
        return [f"DELETE FROM {table_name} WHERE {condition};"]
    
    # TODO masih ngebug
    # Build an INSERT query to undo a DELETE operation
    # def build_insert_query(self, table_name: str, before: Rows) -> str:
    #     columns = ", ".join(before.data[0].keys()) if before.data else ""
    #     values_list = [
    #         f"({', '.join(f'\"{v}\"' for v in row.values())})" for row in before.data
    #     ]
    #     return f"INSERT INTO {table_name} ({columns}) VALUES {', '.join(values_list)};"
    
    def recover(self, criteria:RecoverCriteria):
        """
        Recovers the database state to meet the criteria (timestamp or transaction id).
        From what i've seen now tho
        For abort:
        - Scan backwards from last query WHILE it meets the criteria
        - Abort (undo) and write on the log
        - No need to redo
        """
        if not os.path.exists(self.log_file):
            print("No log file. Abort")
            return

        logs = self.parse_log_file("./wal.log")
        
        # If aborting
        # Scan backwards from the last log entry
        for log_entry in reversed(logs):
            checkcurr_timestamp = log_entry.timestamp
            checkcurr_transaction_id = log_entry.transaction_id
            query_words = log_entry.query.split()
            query_type = query_words[0].upper()
            table_name = self.get_table_name(log_entry.query)
            # check RecoveryCriteria
            # if (checkcurr_timestamp and criteria.timestamp and checkcurr_timestamp < criteria.timestamp):
            #     print("Finish recovering based on timestamp..")
            #     break
            
            # if (checkcurr_transaction_id and criteria.transaction_id and checkcurr_transaction_id < criteria.transaction_id):
            #     print("Finish recovering based on transaction_id..")
            #     break
            undo_message = ""
            print("------------------> " + log_entry.query)
            if query_type == "UPDATE":
                print(log_entry.before)
                undo_query = self.build_update_query(table_name, log_entry.before, log_entry.data)
            elif query_type == "INSERT":
                undo_query = self.build_delete_query(table_name, log_entry.data)
            elif query_type == "DELETE":
                undo_query = self.build_insert_query(table_name, log_entry.before)
            else:
                print("Pass")
                continue
            for i in undo_query:
                print(f"Executing undo query: {i}\n")
            # query processor do the relog here -- TODO
                undo_log = ExecutionResult(
                    transaction_id=checkcurr_transaction_id,
                    timestamp=datetime.datetime.now(),
                    message=undo_message,
                    before=log_entry.data,
                    data=log_entry.before,
                    query=i,
                )
                self.write_log(undo_log)

        # TODO abort log ini masih konfyus
        undo_query = self.build_update_query(table_name,log_entry.query)
        # relogquery = "UPDATE " + table_name + " WHERE " + condition
        relog = ExecutionResult("ABORT", checkcurr_transaction_id, datetime.datetime.now(), Rows([log_entry.data.data],log_entry.data.rows_count),Rows([log_entry.before.data],log_entry.before.rows_count), relogquery)
        self.write_log(relog)
            
        
        # If system fails
        """
        Redo dulu:
        - Scan dari atas sampe ketemu rec checkpoint L trkhr
        - Buat undo list
        - Masukin nomor transaksi ke undo list kalau ada start
        - Remove dari undo list kalau ketemu commit/abort

        Undo:
        - Scan dari bawah sampai undo list habis
        - Undo kalau ada di undo list
        - Kalau ketemu start hapus transaksi di undo list
        """
        # undo_list = []
        # for log_entry in logs:
        #     checkcurr_timestamp = log_entry.timestamp
        #     checkcurr_transaction_id = log_entry.transaction_id
        #     if(log_entry == "Start" and log_entry not in undo_list):
        #         undo_list.append(log_entry)
        #     if (log_entry == "commit" or log_entry == "abort"):
        #         undo_list.remove(checkcurr_transaction_id)

        # for log_entry in reversed(logs):
        #     if (len(undo_list)==0):
        #         break
                # TODO

            
                
        print("\nRecovery from aborting complete.\n")
        return
    
    


if __name__ == "__main__":

    failurerec = FailureRecoveryManager()

    print("\nExecution Result:")
    res = failurerec.parse_log_file("./wal.log")
    for i in res:
        print("Message: "+str(i.message))
        print("Transaction id: "+str(i.transaction_id))
        print("Timestamp: "+str(i.timestamp))
        print("Query: "+str(i.query))
        print("Before data: "+str(i.before.data))
        print("Before rows count: "+str(i.before.rows_count))
        print("After data: "+str(i.data.data))
        print("After rows count: "+str(i.data.rows_count))
        print("")
    failurerec.recover(None)

""" Cek recover pakai ini dah
ACTIVE,1,2024-11-22T10:00:00,Query: UPDATE table_name SET name='new_value' WHERE id=1,Before: [{'id': 1, 'name': 'old_value'}],After: [{'id': 1, 'name': 'new_value'}]
START,3,2024-11-22T10:10:00,Query: DELETE FROM table_name WHERE id=1,Before: [{'id': 1, 'name': 'new_value'}],After: []
COMMIT,2,2024-11-22T10:05:00,Query: INSERT INTO table_name (id, name) VALUES (2, 'another_value'),Before: [],After: [{'id': 2, 'name': 'another_value'}]
ABORT,4,2024-11-22T10:15:00,Query: UPDATE table_name SET name='updated_value' WHERE name='another_value',Before: [{'id': 2, 'name': 'another_value'},{'id': 3, 'name': 'another_value'}],After: [{'id': 2, 'name': 'updated_value'},{'id': 3, 'name': 'updated_value'}]
END,5,2024-11-22T10:20:00,Query: SELECT * FROM table_name,Before: [],After: []
"""
