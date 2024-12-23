import sys
sys.path.append('./Storage_Manager')

import datetime
import os
import re
import threading
from typing import Generic, Optional, TypeVar, List, Dict, Union
from dataclasses import dataclass
from Buffer import Buffer
import time 
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
    _checkpoint_thread = None
    _leader_instance = None 

    def __init__(self, log_file='wal.log', log_size=50):
        self.memory_wal: List[ExecutionResult] = []
        self.undo_list = []
        self.log_file = log_file
        self.buffer = Buffer(100)
        self.wal_size = log_size
        self.last_checkpoint_time = datetime.datetime.now()
        self.checkpoint_interval = datetime.timedelta(minutes=5)
        self.lock = threading.RLock()

        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                pass
        with FailureRecoveryManager._checkpoint_lock:
            if FailureRecoveryManager._leader_instance is None:
                FailureRecoveryManager._leader_instance = self

        # Start the checkpointing thread if not already started
        self._start_checkpointing_thread()

    @classmethod
    def _start_checkpointing_thread(cls):
        """Start the checkpointing thread if it hasn't been started already."""
        with cls._checkpoint_lock:  # Ensure thread-safe initialization
            if not cls._checkpoint_thread_started:
                threading.Thread(target=cls._checkpoint_loop, daemon=True).start()
                cls._checkpoint_thread_started = True
                

    @classmethod
    def _checkpoint_loop(cls):
        """Thread loop for checkpointing."""
        while True:
            try:
                time.sleep(300)  # Corrected to use time.sleep
                # Iterate over instances and save checkpoint
                with cls._checkpoint_lock:
                    if cls._leader_instance:
                        try:
                            cls._leader_instance.save_checkpoint()
                        except Exception as e:
                            print(f"Error during checkpointing for leader {cls._leader_instance}: {e}")
            except Exception as e:
                print(f"Error in checkpoint loop: {e}")
    
         
    def parse_log_file(self, file_path: str) -> List[ExecutionResult]:
        execution_results = []
        last_undo_list = []
        try:
            with self.lock:  # Protecting any modifications
                with open(file_path, 'r') as file:
                    for line in file:
                        if line.startswith('CHECKPOINT'):
                            match = re.match(r"CHECKPOINT,([\d\-T:\.]+),(\[.*\])", line)
                            if match:
                                timestamp = datetime.datetime.fromisoformat(match.group(1))
                                try:
                                    undo_list = eval(match.group(2)) 
                                    last_undo_list = undo_list  
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
                                except Exception as e:
                                    print(f"Error parsing CHECKPOINT line: {e}")
                        else:
                            match = re.match(r"(\w+),(\d+),([\d\-T:\.]+),(.+?),Before: (.*?),After: (.*)", line)
                            if match:
                                type = match.group(1)
                                transaction_id = int(match.group(2))
                                timestamp = datetime.datetime.fromisoformat(match.group(3))
                                query = None if match.group(4) == "None"  else match.group(4)
                                try:
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
                                except Exception as e:
                                    print(f"Error parsing log entry: {e}")
                return execution_results, last_undo_list
        except Exception as e:
            print(f"Error reading log file {file_path}: {e}")
            return [], []

    def get_buffer(self):
        return self.buffer

    def set_buffer(self, buffer):
        self.buffer = buffer


    def write_log(self, info: ExecutionResult) -> None:
        try:
            with self.lock:
                self.memory_wal.append(info)

                if info.type == "COMMIT":
                    try:
                        with open(self.log_file, 'a') as log_file:
                            for entry in self.memory_wal:
                                query_value = entry.query if entry.query else "None"  
                                previous_data = entry.previous_data.data if entry.previous_data else []
                                new_data = entry.new_data.data if entry.new_data else []
                                log_entry = f"{entry.type},{entry.transaction_id},{entry.timestamp.isoformat()},{query_value},Before: {previous_data},After: {new_data}"
                                log_file.write(log_entry + '\n')

                        self.memory_wal.clear()

                        if info.transaction_id in self.undo_list:
                            self.undo_list.remove(info.transaction_id)
                    except Exception as e:
                        print(f"Error writing COMMIT log: {e}")

                if len(self.memory_wal) >= self.wal_size and info.type != "COMMIT":
                    try:
                        with open(self.log_file, 'a') as log_file:
                            for entry in self.memory_wal:
                                query_value = entry.query if entry.query else "None"  
                                previous_data = entry.previous_data.data if entry.previous_data else []
                                new_data = entry.new_data.data if entry.new_data else []
                                log_entry = f"{entry.type},{entry.transaction_id},{entry.timestamp.isoformat()},{query_value},Before: {previous_data},After: {new_data}"
                                log_file.write(log_entry + '\n')

                        self.memory_wal.clear()
                    except Exception as e:
                        print(f"Error writing WAL log: {e}")

                if info.type != "COMMIT": 
                    if info.transaction_id not in self.undo_list:
                        self.undo_list.append(info.transaction_id)
        except Exception as e:
            print(f"Error in write_log: {e}")


    def save_checkpoint(self) -> None:
        try:
            with self.lock:
                if self.memory_wal:
                    try:
                        with open(self.log_file, "a") as log_file:
                            for entry in self.memory_wal:
                                query_value = entry.query if entry.query else "None"
                                # Handle None for previous_data and new_data
                                previous_data = entry.previous_data.data if entry.previous_data else []
                                new_data = entry.new_data.data if entry.new_data else []
                                log_entry = f"{entry.type},{entry.transaction_id},{entry.timestamp.isoformat()},{query_value},Before: {previous_data},After: {new_data}"
                                log_file.write(log_entry + '\n')
                    except Exception as e:
                        print(f"Error writing WAL during checkpoint: {e}")
                try:        
                    with open(self.log_file, "a") as log_file:
                        checkpoint_entry = f"CHECKPOINT,{datetime.datetime.now().isoformat()},{self.undo_list}"
                        log_file.write(checkpoint_entry + '\n')
                except Exception as e:
                    print(f"Error writing CHECKPOINT log: {e}")
                try:
                    self.buffer.flush()
                except Exception as e:
                    print(f"Error writing buffer to storage manager: {e}")

                self.memory_wal.clear()
        except Exception as e:
            print(f"Error in save_checkpoint: {e}")

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
            # Format SET clause
            set_clause = ", ".join(
                f"{k}={repr(v) if isinstance(v, str) else v}" for k, v in before_row.items()
            )
            # Format WHERE clause
            condition = " AND ".join(
                f"{k}={repr(v) if isinstance(v, str) else v}" for k, v in after_row.items()
            )
            query = f"UPDATE {table_name} SET {set_clause} WHERE {condition};"
            queries.append(query)
        return queries

    # Build a DELETE query to undo an INSERT operation
    def build_delete_query(self, table_name: str, after: Rows) -> List[str]:
        condition = " AND ".join(
            f"{k}={repr(v) if isinstance(v, str) else v}" for row in after.data for k, v in row.items()
        )
        return [f"DELETE FROM {table_name} WHERE {condition};"]

    
    # Build an INSERT query to undo a DELETE operation
    def build_insert_query(self, table_name: str, before: Rows, after: Rows) -> List[str]:
        if not before.data:
            return []
        after_set = {tuple(row.items()) for row in after.data} if after and after.data else set()
        filtered_before = [row for row in before.data if tuple(row.items()) not in after_set]

        if not filtered_before:
            return []

        # Format columns and values
        columns = ", ".join(filtered_before[0].keys())
        values_list = [
            "(" + ", ".join(repr(v) if isinstance(v, str) else str(v) for v in row.values()) + ")"
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
        For abort normal case:
        - Get undolist from RecoverCriteria transaction id, timestamp is not used
        - Scan memory_wal
        - Abort (undo) and write on the log
        - IF UNDO LIST NOT EMPTY-Scan backwards from wal.log from last query
        - Abort (undo) and write on the log
        - Redo dilakukan di concurrency!
        """
        try:
            if not criteria.transaction_id:
                return []
            with self.lock:
                valid_transaction_ids = [tid for tid in criteria.transaction_id if tid in self.undo_list]

                if not valid_transaction_ids:
                    return []

                undo_list = valid_transaction_ids
                undo_queries = []  # List of undo queries to return
                # Scan memory_wal
                # If the transaction id that we want to undo is now empty then stop
                done_undo = False
                for exec_result in reversed(self.memory_wal):
                    undo_query= []
                    if (len(undo_list)==0):
                        done_undo = True
                        break   
                    checkcurr_transaction_id = exec_result.transaction_id
                    if (checkcurr_transaction_id in undo_list):
                        if exec_result.type == "START":
                            undo_list.remove(checkcurr_transaction_id)
                            self.undo_list.remove(checkcurr_transaction_id)
                            undo_query= []
                        elif exec_result.type == "UPDATE":
                            table_name = self.get_table_name(exec_result.query)
                            undo_query = self.build_update_query(table_name, exec_result.previous_data, exec_result.new_data)
                        elif exec_result.type == "INSERT":
                            table_name = self.get_table_name(exec_result.query)
                            undo_query = self.build_delete_query(table_name, exec_result.new_data)
                        elif exec_result.type == "DELETE":
                            table_name = self.get_table_name(exec_result.query)
                            undo_query = self.build_insert_query(table_name, exec_result.previous_data, exec_result.new_data)
                        else:
                            continue
                        for query in undo_query: 
                            undo_queries.append([checkcurr_transaction_id, query]) 

                # If we still cannot find START for every transaction_id in memory_wal
                # READ .LOG FILE
                if len(undo_list) == 0:
                    done_undo = True

                if (done_undo==False):
                    if not os.path.exists(self.log_file):
                        raise Exception("No log file. Abort")
                    logs, last_undo_list = self.parse_log_file(self.log_file)
                    for log_entry in reversed(logs):
                        undo_query = []
                        checkcurr_transaction_id = log_entry.transaction_id
                        if (len(undo_list)==0):
                            done_undo = True
                            break
                        if log_entry.type == "START" and checkcurr_transaction_id in undo_list:
                            undo_list.remove(checkcurr_transaction_id)
                            self.undo_list.remove(checkcurr_transaction_id)
                        else:
                            if (checkcurr_transaction_id in undo_list):
                                if log_entry.type == "UPDATE":
                                    table_name = self.get_table_name(log_entry.query)
                                    undo_query = self.build_update_query(table_name, log_entry.previous_data, log_entry.new_data)
                                elif log_entry.type == "INSERT":
                                    table_name = self.get_table_name(log_entry.query)
                                    undo_query = self.build_delete_query(table_name, log_entry.new_data)
                                elif log_entry.type == "DELETE":
                                    table_name = self.get_table_name(log_entry.query)
                                    undo_query = self.build_insert_query(table_name, log_entry.previous_data, log_entry.new_data)
                                else:
                                    continue
                                for query in undo_query:
                                    undo_queries.append([checkcurr_transaction_id, query])
                return undo_queries
        except Exception as e:
            print(f"Error during recovery: {e}")
            return []
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
        try:
            with self.lock:
                logs, undo_list = self.parse_log_file(self.log_file)
                if not logs:
                    return

                self.undo_list = undo_list
                # REDO Phase
                last_checkpoint = None
                for log in reversed(logs):
                    if log.type == "CHECKPOINT":
                        last_checkpoint = log 
                redo_start_index = logs.index(last_checkpoint) + 1 if last_checkpoint else 0

                redo_query = []
                # Perform REDO
                for log in logs[redo_start_index:]:
                    if log.type in ("COMMIT", "ABORT"):
                        if log.transaction_id in self.undo_list:
                            self.undo_list.remove(log.transaction_id)
                    elif log.type == "START":
                        if log.transaction_id not in self.undo_list:
                            self.undo_list.append(log.transaction_id)
                    elif log.type == "INSERT" or log.type == "UPDATE" or log.type == "DELETE":  
                        redo_query.append([log.transaction_id, log.query])

                # UNDO Phase

                undo_queries = []
                for log in reversed(logs):
                    undo_query = []
                    if not self.undo_list:
                        break

                    if log.transaction_id in self.undo_list:
                        if log.status == "START":
                            self.undo_list.remove(log.transaction_id)
                        else:
                            if log.type == "UPDATE":
                                table_name = self.get_table_name(log.query)
                                undo_query = self.build_update_query(table_name, log.previous_data, log.new_data)
                            elif log.type == "INSERT":
                                table_name = self.get_table_name(log.query)
                                undo_query = self.build_delete_query(table_name, log.new_data)
                            elif log.type == "DELETE":
                                table_name = self.get_table_name(log.query)
                                undo_query = self.build_insert_query(table_name, log.previous_data, log.new_data)
                            else:
                                continue
                            for query in undo_query:
                                    undo_queries.append([log.transaction_id, query])

            return redo_query, undo_queries
        except Exception as e:
            print(f"Error during system recovery: {e}")
            return redo_query, undo_query

