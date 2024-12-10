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
        # Class-level attributes
    _checkpoint_thread_started = False
    _checkpoint_lock = threading.Lock()

    def __init__(self, base_path: str, log_file='wal.log', log_size=50):
        self.memory_wal: List[ExecutionResult] = []
        self.undo_list = []
        self.log_file = log_file
        self.buffer = []
        self.wal_size = log_size
        self.last_checkpoint_time = datetime.datetime.now()
        self.checkpoint_interval = datetime.timedelta(minutes=5)
        self.lock = threading.Lock()

        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                pass

        # Start the checkpointing thread if not already started
        self._start_checkpointing_thread()

    @classmethod
    def _start_checkpointing_thread(cls):
        """Start the checkpointing thread if it hasn't been started already."""
        with cls._checkpoint_lock:  # Ensure thread-safe initialization
            if not cls._checkpoint_thread_started:
                print("Starting checkpointing thread...")
                threading.Thread(target=cls._checkpoint_loop, daemon=True, args=(cls,)).start()
                cls._checkpoint_thread_started = True
            else:
                print("Checkpointing thread already running.")

    @classmethod
    def _checkpoint_loop(cls):
        """Thread loop for checkpointing."""
        while True:
            datetime.time.sleep(300) 
            print("Running checkpoint...")
            # Iterate over instances and save checkpoint
            for instance in cls._instances:
                instance.save_checkpoint()
            print("Checkpoint saved.")
         
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

    def get_buffer(self):
        return self.buffer()

    def set_buffer(self, buffer):
        self.buffer = buffer
        
    def clear_buffer(self):
        self.buffer.clear()


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

            if info.type != "COMMIT": 
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
        For abort normal case:
        - Get undolist from RecoverCriteria transaction id, timestamp is not used
        - Scan memory_wal
        - Abort (undo) and write on the log
        - IF UNDO LIST NOT EMPTY-Scan backwards from wal.log from last query
        - Abort (undo) and write on the log
        - Redo dilakukan di concurrency!
        """
        if not criteria.transaction_id:
            print("No transaction IDs provided in criteria.")
            return []
        with self.lock:
            valid_transaction_ids = [tid for tid in criteria.transaction_id if tid in self.undo_list]

            if not valid_transaction_ids:
                print("No valid transactions to undo. Exiting recovery process.")
                return []

            print(f"Transactions to undo: {valid_transaction_ids}")
            undo_list = valid_transaction_ids
            undo_queries = []  # List of undo queries to return
            # Scan memory_wal
            # If the transaction id that we want to undo is now empty then stop
            done_undo = False
            for exec_result in self.memory_wal:
                if (len(undo_list)==0):
                    done_undo = True
                    print("Undo list is now empty, stop recovery")
                    break   
                if (checkcurr_transaction_id in undo_list):
                    print("------------------> " + exec_result.query)
                    if exec_result.status == "START":
                        print("Found the start for the transaction_id: "+ checkcurr_transaction_id)
                        undo_list.remove(checkcurr_transaction_id)
                    elif exec_result.status == "UPDATE":
                        print(exec_result.previous_data)
                        undo_query = self.build_update_query(table_name, exec_result.previous_data, exec_result.updated_data)
                    elif exec_result.status == "INSERT":
                        undo_query = self.build_delete_query(table_name, exec_result.updated_data)
                    elif exec_result.status == "DELETE":
                        undo_query = self.build_insert_query(table_name, exec_result.previous_data, exec_result.updated_data)
                    else:
                        print("Not a valid query")
                        continue
                    for i in undo_query:
                        print(f"Executing undo query: {i}\n") 
                        # undo_log = ExecutionResult(
                        #     transaction_id=checkcurr_transaction_id,
                        #     timestamp=datetime.datetime.now(),
                        #     message="ROLLBACK",
                        #     before=exec_result.updated_data,
                        #     data=exec_result.previous_data,
                        #     query=i,
                        # )
                        # self.write_log(undo_log)   
                        undo_queries.append(i)  

            # If we still cannot find START for every transaction_id in memory_wal
            # READ .LOG FILE
            print("Akan melakukan pembacaan dari wal.log")
            if (done_undo==False):
                if not os.path.exists(self.log_file):
                    raise Exception("No log file. Abort")
                logs = self.parse_log_file("./wal.log")
                for log_entry in reversed(logs):
                    checkcurr_transaction_id = log_entry.transaction_id
                    print("\nNow Running: "+ log_entry.status + " " + str(log_entry.transaction_id))
                    if (len(undo_list)==0):
                        done_undo = True
                        print("Undo list is now empty, stop recovery")
                        break
                    if(log_entry.query!=None):
                        if (checkcurr_transaction_id in undo_list):
                            table_name = self.get_table_name(log_entry.query)
                            print("------------------> " + log_entry.query)
                            query_type = log_entry.query.split()[0]
                            if query_type == "UPDATE":
                                undo_query = self.build_update_query(table_name, log_entry.previous_data, log_entry.updated_data)
                            elif query_type == "INSERT":
                                undo_query = self.build_delete_query(table_name, log_entry.updated_data)
                            elif query_type == "DELETE":
                                undo_query = self.build_insert_query(table_name, log_entry.previous_data, log_entry.updated_data)
                            else:
                                print("Not a valid query got:",log_entry.query)
                                continue
                            for i in undo_query:
                                print(f"Executing undo query: {i}\n") 
                                # undo_log = ExecutionResult(
                                #     transaction_id=checkcurr_transaction_id,
                                #     timestamp=datetime.datetime.now(),
                                #     status="ROLLBACK",
                                #     before=log_entry.updated_data,
                                #     after=log_entry.previous_data,
                                #     query=i,
                                # )
                                # self.write_log(undo_log)
                                undo_queries.append(i) 
                    else:
                        if log_entry.status == "START" and checkcurr_transaction_id in undo_list:
                            print("Found the start for the transaction_id: "+ str(checkcurr_transaction_id))
                            undo_list.remove(checkcurr_transaction_id)

            return undo_queries
        # Write ABORT log
        # abort_log = ExecutionResult(
        #     transaction_id=checkcurr_transaction_id,
        #     timestamp=datetime.datetime.now(),
        #     status="ABORT",
        #     before=Rows(data=[], rows_count=0),
        #     after=Rows(data=[], rows_count=0),
        #     query="None",
        # )
        # self.write_log(abort_log)
        
    def recoverSystem(self):
        # Parse the log file to retrieve all logs
        with self.lock:
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
            print("Previous data: " + str(i.previous_data.data))
            print("Previous rows count: " + str(i.previous_data.rows_count))
        else:
            print("Previous data: None")
            print("Previous rows count: 0")

        # Check if updated data exists before accessing it
        if i.new_data is not None:
            print("After data: " + str(i.new_data.data))
            print("After rows count: " + str(i.new_data.rows_count))
        else:
            print("After data: None")
            print("After rows count: 0")

    #     print("")

    failurerec.write_log(ExecutionResult(
                transaction_id=1,
                timestamp=datetime.datetime.now(),
                type="ACTIVE",
                previous_data=Rows([{'id': 1, 'name': 'old_value'}], 1),
                new_data=Rows([{'id': 1, 'name': 'new_value'}], 1),
                query="UPDATE table_name SET name='new_value' WHERE id=1",
                status=""
            ))
    
    # failurerec.save_checkpoint()
    criteria = RecoverCriteria([1,2],None)
    failurerec.recover(criteria)

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

"""

COMMIT,2,2024-11-22T10:05:00,None,Before: [],After: []
ROLLBACK,2,2024-11-22T10:05:00,None,Before: [],After: []
START,3,2024-11-22T10:10:00,DELETE FROM table_name WHERE id=1,Before: [{'id': 1, 'name': 'new_value'}],After: []
ABORT,4,2024-11-22T10:15:00,None,Before: [],After: []
CHECKPOINT,2024-11-22T10:20:00,[1,2]
"""
