import sys
sys.path.append('./Storage_Manager')

import datetime
import os
import re
import threading
from typing import Generic, TypeVar, List

# from Storage_Manager.StorageManager import StorageManager

T = TypeVar('T')

class Rows(Generic[T]):
    def __init__(self, data: List[T], rows_count: int):
        self.data = data
        self.rows_count = rows_count

class ExecutionResult:
    def __init__(self, transaction_id: int, timestamp: datetime, status: str, before: Rows, after: Rows, query: str):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.status = status
        self.previous_data = before
        self.updated_data = after
        self.query = query
from RecoverCriteria import RecoverCriteria

class FailureRecoveryManager:
    def __init__(self, base_path: str, log_file='wal.log', log_size=50):
        self.memory_wal: list[ExecutionResult] = []
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
                            status='CHECKPOINT',
                            query=None, 
                            before=None,
                            after=None,
                        )
                        execution_results.append(execution_result)
                else:
                    match = re.match(r"(\w+),(\d+),([\d\-T:]+),(.+?),Before: (.*?),After: (.*)", line)
                    if match:
                        status = match.group(1)
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
                            status=status,
                            query=query,  
                            before=before_rows,
                            after=after_rows
                        )
                        execution_results.append(execution_result)
        return execution_results

    def write_log(self, info: ExecutionResult) -> None:
        with self.lock:
            self.memory_wal.append(info)

            if info.status == "COMMIT":
                with open(self.log_file, 'a') as log_file:
                    for entry in self.memory_wal:
                        query_value = entry.query if entry.query else "None"  
                        log_entry = f"{entry.status},{entry.transaction_id},{entry.timestamp.isoformat()},{query_value},Before: {entry.previous_data.data},After: {entry.updated_data.data}"
                        log_file.write(log_entry + '\n')

                self.memory_wal.clear()

                if info.transaction_id in self.undo_list:
                    self.undo_list.remove(info.transaction_id)

            if len(self.memory_wal) >= self.wal_size and info.status != "COMMIT":
                with open(self.log_file, 'a') as log_file:
                    for entry in self.memory_wal:
                        query_value = entry.query if entry.query else "None"  
                        log_entry = f"{entry.status},{entry.transaction_id},{entry.timestamp.isoformat()},{query_value},Before: {entry.previous_data.data},After: {entry.updated_data.data}"
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
                        log_entry = f"{entry.status},{entry.transaction_id},{entry.timestamp.isoformat()},{query_value},Before: {entry.previous_data.data},After: {entry.updated_data.data}"
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
        For abort normal case:
        - Get undolist from RecoverCriteria transaction id, timestamp is not used
        - Scan memory_wal
        - Abort (undo) and write on the log
        - IF UNDO LIST NOT EMPTY-Scan backwards from wal.log from last query
        - Abort (undo) and write on the log
        - Redo dilakukan di concurrency!
        """
        undo_list = criteria.transaction_id
        undo_queries = [] #ini list undo query yang akan direturn ke QP
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

    def recoversystem(self):
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

        if not os.path.exists(self.log_file):
            print("No log file. Abort")
            return

        logs = self.parse_log_file("./wal.log")
        
        # Looping from the top getting all transaction id that needed to be undo
        for log_entry in logs:
            checkcurr_transaction_id = log_entry.transaction_id
            if(log_entry.status == "START" and checkcurr_transaction_id not in self.undo_list):
                self.undo_list.append(checkcurr_transaction_id)
            if ((log_entry.status == "COMMIT" or log_entry.status == "ABORT")and log_entry in self.undo_list):
                self.undo_list.remove(checkcurr_transaction_id)
            print(f"Proses redo {log_entry.status} berhasil dan undo list skrg menjadi: {self.undo_list} \n")
            
        # for log_entry in reversed(logs):
        #     if (len(self.undo_list)==0):
        #         break
        #     if (log_entry.status!="CHECKPOINT"):
        #         checkcurr_transaction_id = log_entry.transaction_id
        #         query_words = log_entry.query.split()
        #         query_type = query_words[0].upper()
        #         table_name = self.get_table_name(log_entry.query)

        #         undo_message = "ROLLBACK"
        #         print("------------------> " + log_entry.query)
        #         if query_type == "UPDATE":
        #             undo_query = self.build_update_query(table_name, log_entry.before, log_entry.data)
        #         elif query_type == "INSERT":
        #             undo_query = self.build_delete_query(table_name, log_entry.data)
        #         elif query_type == "DELETE":
        #             undo_query = self.build_insert_query(table_name, log_entry.before)
        #         else:
        #             print("Pass")
        #             continue

        #     for i in undo_query:
        #         print(f"Executing undo query: {i}\n")
        #         # query processor do the relog here -- TODO
        #         undo_log = ExecutionResult(
        #             transaction_id=checkcurr_transaction_id,
        #             timestamp=datetime.datetime.now(),
        #             message=undo_message,
        #             before=log_entry.data,
        #             data=log_entry.before,
        #             query=i,
        #         )
        #         self.write_log(undo_log)

        # TODO abort log ini masih konfyus
        # undo_query = self.build_update_query(table_name,log_entry.query)
        # relog = ExecutionResult("ABORT", checkcurr_transaction_id, datetime.datetime.now(), Rows([log_entry.data.data],log_entry.data.rows_count),Rows([log_entry.before.data],log_entry.before.rows_count), relogquery)
        # self.write_log(relog)
                

            
                
        print("\nRecovery from aborting complete.\n")
        return
    
if __name__ == "__main__":

    failurerec = FailureRecoveryManager("../Storage_Manager/storage")

    print("\nExecution Result:")
    # res = failurerec.parse_log_file("./wal.log")
    # for i in res:
    #     print("Status: " + str(i.status))
    #     print("Transaction id: " + str(i.transaction_id))
    #     print("Timestamp: " + str(i.timestamp))
    #     print("Query: " + str(i.query))

    #     # Check if previous data exists before accessing it
    #     if i.previous_data is not None:
    #         print("Previous data: " + str(i.previous_data.data))
    #         print("Previous rows count: " + str(i.previous_data.rows_count))
    #     else:
    #         print("Previous data: None")
    #         print("Previous rows count: 0")

    #     # Check if updated data exists before accessing it
    #     if i.updated_data is not None:
    #         print("Updated data: " + str(i.updated_data.data))
    #         print("Updated data rows count: " + str(i.updated_data.rows_count))
    #     else:
    #         print("After data: None")
    #         print("After rows count: 0")

    #     print("")

    # failurerec.write_log(ExecutionResult(
    #             transaction_id=1,
    #             timestamp=datetime.datetime.now(),
    #             status="ACTIVE",
    #             before=Rows([{'id': 1, 'name': 'old_value'}], 1),
    #             after=Rows([{'id': 1, 'name': 'new_value'}], 1),
    #             query="UPDATE table_name SET name='new_value' WHERE id=1"
    #         ))
    
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
