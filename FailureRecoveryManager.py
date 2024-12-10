import sys
sys.path.append('./Storage_Manager')

import datetime
import os
import re
import threading
from typing import Generic, Optional, TypeVar, List, Dict, Union
from dataclasses import dataclass
from Storage_Manager.StorageManager import StorageManager

T = TypeVar('T')

@dataclass
class Rows(Generic[T]):
    data: List[T]
    rows_count: int
    schema: Optional[List[str]] = None
    columns: Optional[Dict[str, str]] = None

@dataclass
class ExecutionResult:
    transaction_id: int
    timestamp: datetime
    type: str 
    status: str
    query: str
    previous_data: Union[Rows,int, None]
    new_data: Union[Rows,int, None]
        
from RecoverCriteria import RecoverCriteria

class FailureRecoveryManager:
    def __init__(self, base_path: str, log_file='wal.log', log_size=50):
        self.memory_wal = []
        self.undo_list = []
        self.log_file = log_file
        self.buffer = []
        self.wal_size = log_size
        self.last_checkpoint_time = datetime.datetime.now()
        self.checkpoint_interval = datetime.timedelta(minutes=5)
        self.lock = threading.Lock() 
        # self.storage_manager = StorageManager(base_path)

        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                pass
            
    def parse_log_file(self, file_path: str) -> List[ExecutionResult]:
        execution_results = []
        with open(file_path, 'r') as file:
            for line in file:
                if line.startswith('CHECKPOINT'):
                    match = re.match(r"CHECKPOINT,([\d\-T:]+),(\[.*\])", line)
                    if match:
                        timestamp = datetime.datetime.fromisoformat(match.group(1))
                        undo_list = eval(match.group(2))  
                        self.undo_list.extend(undo_list)  
                        execution_result = ExecutionResult(
                            transaction_id=None, 
                            timestamp=timestamp,
                            type='CHECKPOINT',
                            query=None, 
                            previous_data=None,
                            new_data=None,
                            status=None
                        )
                        execution_results.append(execution_result)
                else:
                    match = re.match(r"(\w+),(\d+),([\d\-T:]+),(.+?),Before: (.*?),After: (.*)", line)
                    if match:
                        type = match.group(1)
                        transaction_id = int(match.group(2))
                        timestamp = datetime.datetime.fromisoformat(match.group(3))
                        query = None if match.group(4) == "None"  else match.group(4)

                        before_data = eval(match.group(5))  
                        after_data = eval(match.group(6))   

                        before_rows = Rows(data=before_data, rows_count=len(before_data))
                        after_rows = Rows(data=after_data, rows_count=len(after_data))

                        execution_result = ExecutionResult(
                            transaction_id=transaction_id,
                            timestamp=timestamp,
                            status="",
                            query=query,  
                            previous_data=before_rows,
                            new_data=after_rows,
                            type=type
                        )
                        execution_results.append(execution_result)
        return execution_results

    def write_log(self, info: ExecutionResult) -> None:
        with self.lock:
            self.memory_wal.append(info)

            if info.type == "COMMIT":
                with open(self.log_file, 'a') as log_file:
                    for entry in self.memory_wal:
                        query_value = entry.query if entry.query else "None"  
                        log_entry = f"{entry.type},{entry.transaction_id},{entry.timestamp.isoformat()},{query_value},Before: {entry.previous_data.data},After: {entry.new_data.data}"
                        log_file.write(log_entry + '\n')

                self.memory_wal.clear()

                if info.transaction_id in self.undo_list:
                    self.undo_list.remove(info.transaction_id)

            if len(self.memory_wal) >= self.wal_size and info.type != "COMMIT":
                with open(self.log_file, 'a') as log_file:
                    for entry in self.memory_wal:
                        query_value = entry.query if entry.query else "None"  
                        log_entry = f"{entry.type},{entry.transaction_id},{entry.timestamp.isoformat()},{query_value},Before: {entry.previous_data.data},After: {entry.new_data.data}"
                        log_file.write(log_entry + '\n')

                self.memory_wal.clear()

            if info.status != "COMMIT": 
                if info.transaction_id not in self.undo_list:
                    self.undo_list.append(info.transaction_id)


    def save_checkpoint(self) -> None:
        with self.lock:
            if self.memory_wal:
                with open(self.log_file, "a") as log_file:
                    for entry in self.memory_wal:
                        query_value = entry.query if entry.query else "None"  
                        log_entry = f"{entry.type},{entry.transaction_id},{entry.timestamp.isoformat()},{query_value},Before: {entry.previous_data.data},After: {entry.new_data.data}"
                        log_file.write(log_entry + '\n')

            with open(self.log_file, "a") as log_file:
                checkpoint_entry = f"CHECKPOINT,{datetime.datetime.now().isoformat()},{self.undo_list}"
                log_file.write(checkpoint_entry + '\n')
            if hasattr(self, "storage_manager"):
                self.storage_manager.write_buffer(self.buffer)
            self.memory_wal.clear()

    def start_checkpointing(self):
        def checkpoint_loop():
            while True:
                datetime.time.sleep(300)  # Wait for 5 minutes (300 seconds)
                print("Running checkpoint...")
                self.save_checkpoint()
                print("Checkpoint saved.")

        threading.Thread(target=checkpoint_loop, daemon=True).start()

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
    
    # Build an INSERT query to undo a DELETE operation
    def build_insert_query(self, table_name: str, before: Rows, after: Rows) -> List[str]:
        if not before.data:
            return ""
        after_set = {tuple(row.items()) for row in after.data}
        filtered_before = [row for row in before.data if tuple(row.items()) not in after_set]

        if not filtered_before:
            return "" 

        columns = ", ".join(filtered_before[0].keys())
        values_list = [
            "(" + ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in row.values()) + ")"
            for row in filtered_before
        ]
        return [f"INSERT INTO {table_name} ({columns}) VALUES {', '.join(values_list)};"]
    
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
            undo_message = "ROLLBACK"
            print("------------------> " + log_entry.query)
            if query_type == "UPDATE":
                print(log_entry.before)
                undo_query = self.build_update_query(table_name, log_entry.before, log_entry.data)
            elif query_type == "INSERT":
                undo_query = self.build_delete_query(table_name, log_entry.data)
            elif query_type == "DELETE":
                undo_query = self.build_insert_query(table_name, log_entry.before, log_entry.data)
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
        undo_list = []
        for log_entry in logs:
            checkcurr_timestamp = log_entry.timestamp
            checkcurr_transaction_id = log_entry.transaction_id
            if(log_entry.message == "START" and log_entry not in undo_list):
                undo_list.append(log_entry)
            if (log_entry.message == "COMMIT" or log_entry.message == "ABORT"):
                undo_list.remove(checkcurr_transaction_id)

        for log_entry in reversed(logs):
            if (len(undo_list)==0):
                break
            checkcurr_timestamp = log_entry.timestamp
            checkcurr_transaction_id = log_entry.transaction_id
            query_words = log_entry.query.split()
            query_type = query_words[0].upper()
            table_name = self.get_table_name(log_entry.query)

            undo_message = "ROLLBACK"
            print("------------------> " + log_entry.query)
            if query_type == "UPDATE":
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
        relog = ExecutionResult("ABORT", checkcurr_transaction_id, datetime.datetime.now(), Rows([log_entry.data.data],log_entry.data.rows_count),Rows([log_entry.before.data],log_entry.before.rows_count), relogquery)
        self.write_log(relog)
                

            
                
        print("\nRecovery from aborting complete.\n")
        return
    
    
    def recoverSystem(self):
        # Parse the log file to retrieve all logs
        logs = self.parse_log_file("./wal.log")
        if not logs:
            print("No logs found for recovery.")
            return

        # REDO Phase
        print("Starting REDO phase...")
        last_checkpoint = None
        for log in reversed(logs):
            if log.type == "CHECKPOINT":
                last_checkpoint = log

        redo_start_index = logs.index(last_checkpoint) + 1 if last_checkpoint else 0

        # Perform REDO
        for log in logs[redo_start_index:]:
            if log.type in ("COMMIT", "ABORT"):
                if log.transaction_id in self.undo_list:
                    self.undo_list.remove(log.transaction_id)
            elif log.type == "START":
                if log.transaction_id not in self.undo_list:
                    self.undo_list.append(log.transaction_id)
            elif log.previous_data and log.new_data:  
                # Apply redo by simulating the write of V2 to Xj
                redo_query = log.query
                # pass to processor

        # UNDO Phase
        print("Starting UNDO phase...")
        for log in reversed(logs):
            if not self.undo_list:
                break

            if log.transaction_id in self.undo_list:
                if log.previous_data and log.new_data:
                    undo_log = ExecutionResult(
                        transaction_id=log.transaction_id,
                        timestamp=datetime.datetime.now(),
                        status="",
                        query=None,
                        before=log.previous_data,
                        after=None,
                        type="ROLLBACK"
                    )
                    self.write_log(undo_log)

                    query_words = log.query.split()
                    query_type = query_words[0].upper()
                    table_name = self.get_table_name(log.query)

                    print("------------------> " + log.query)
                    if query_type == "UPDATE":
                        undo_query = self.build_update_query(table_name, log.previous_data, log.new_data)
                    elif query_type == "INSERT":
                        undo_query = self.build_delete_query(table_name, log.new_data)
                    elif query_type == "DELETE":
                        undo_query = self.build_insert_query(table_name, log.previous_data)
                    else:
                        print("Pass")
                        continue

                    ## pass to processor

                elif log.status == "START":
                    # Write an ABORT log and remove transaction from undo list
                    abort_log = ExecutionResult(
                        transaction_id=log.transaction_id,
                        timestamp=datetime.datetime.now(),
                        status="",
                        query=None,
                        before=None,
                        after=None,
                        type="ABORT"
                    )
                    self.write_log(abort_log)
                    self.undo_list.remove(log.transaction_id)

if __name__ == "__main__":

    failurerec = FailureRecoveryManager("../Storage_Manager/storage")

    print("\nExecution Result:")
    res = failurerec.parse_log_file("./wal.log")
    for i in res:
        print("Type: " + str(i.type))
        print("Transaction id: " + str(i.transaction_id))
        print("Timestamp: " + str(i.timestamp))
        print("Query: " + str(i.query))

        # Check if previous data exists before accessing it
        if i.previous_data is not None:
            print("Before data: " + str(i.previous_data.data))
            print("Before rows count: " + str(i.previous_data.rows_count))
        else:
            print("Before data: None")
            print("Before rows count: 0")

        # Check if updated data exists before accessing it
        if i.new_data is not None:
            print("After data: " + str(i.new_data.data))
            print("After rows count: " + str(i.new_data.rows_count))
        else:
            print("After data: None")
            print("After rows count: 0")

        print("")

    failurerec.write_log(ExecutionResult(
                transaction_id=1,
                timestamp=datetime.datetime.now(),
                type="ACTIVE",
                previous_data=Rows([{'id': 1, 'name': 'old_value'}], 1),
                new_data=Rows([{'id': 1, 'name': 'new_value'}], 1),
                query="UPDATE table_name SET name='new_value' WHERE id=1",
                status=""
            ))
    
    failurerec.save_checkpoint()

    # failurerec.recover(None)

""" Cek recover pakai ini dah So far udh aman
ACTIVE,1,2024-11-22T10:00:00,Query: UPDATE table_name SET name='new_value' WHERE id=1,Before: [{'id': 1, 'name': 'old_value'}],After: [{'id': 1, 'name': 'new_value'}]
ACTIVE,1,2024-11-22T10:00:00,Query: UPDATE table_name SET name='new_value' WHERE name='old_value',Before: [{'id': 1, 'name': 'old_value'}],After: [{'id': 1, 'name': 'new_value'}]
START,3,2024-11-22T10:10:00,Query: DELETE FROM table_name WHERE id=1,Before: [{'id': 1, 'name': 'new_value'},{'id': 1, 'name': 'new_value2'}],After: []
START,3,2024-11-22T10:10:00,Query: DELETE FROM table_name WHERE id=1,Before: [{'id': 1, 'name': 'new_value'},{'id': 1, 'name': 'new_value2'},{'id': 2, 'name': 'OLD_value'}],After: [{'id': 2, 'name': 'OLD_value'}]
COMMIT,2,2024-11-22T10:05:00,Query: INSERT INTO table_name (id, name) VALUES (2, 'another_value'),Before: [],After: [{'id': 2, 'name': 'another_value'}]
ABORT,4,2024-11-22T10:15:00,Query: UPDATE table_name SET name='updated_value' WHERE name='another_value',Before: [{'id': 2, 'name': 'another_value'},{'id': 3, 'name': 'another_value'}],After: [{'id': 2, 'name': 'updated_value'},{'id': 3, 'name': 'updated_value'}]
ABORT,5,2024-11-22T10:15:00,Query: UPDATE table_name SET name='updated_value' WHERE name='another_value',Before: [{'id': 2, 'name': 'another_value'},{'id': 3, 'name': 'another_value'}],After: [{'id': 2, 'name': 'updated_value'},{'id': 3, 'name': 'updated_value'}]
END,6,2024-11-22T10:20:00,Query: SELECT * FROM table_name,Before: [],After: []
"""
