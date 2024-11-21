import datetime
import os
from RecoverCriteria import RecoverCriteria

class FailureRecoveryManager:
    def __init__(self, log_file='wal.log', buffer_size=10):
        self.memory_wal = []  # In-memory WAL
        self.log_file = log_file
        self.buffer = []  # Buffer to hold modified data
        self.buffer_size = buffer_size  # Maximum size of the buffer
        self.last_checkpoint_time = datetime.datetime.now()
        self.checkpoint_interval = datetime.timedelta(minutes=5)
        
        # Initialize log and checkpoint files if they don't exist
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                pass

    def write_log():
        pass


    # still a rough structure :")
    def recover(self, criteria:RecoverCriteria):
        """
        Recovers the database state to meet the criteria (timestamp or transaction id).
        From what i've seen now tho
        For abort:
        - Scan backwards from last query UNTIL criteria is met
        - Find the transaction that needed to get aborted
        - Abort (undo) and write on the log
        - No need to redo
        """
        if not os.path.exists(self.log_file):
            print("No log file. Abort")
            return

        undo_lists = []
        with open(self.log_file, 'r') as log_file:
            logs = log_file.readlines()
        
        # If aborting
        # Scan backwards from the last log entry
        for log_entry in reversed(logs):
            # gini dulu deh konfyus
            curr_timestamp = log_entry.get_timestamp()
            curr_transaction_id = log_entry.get_transaction_id()
            if log_entry.transaction_id == criteria.transaction_id:
                undo_lists.append(log_entry)

            # check RecoveryCriteria
            if curr_timestamp and criteria.timestamp and curr_timestamp < criteria.timestamp:
                print("Finish recovering based on timestamp..")
                break
            
            if curr_transaction_id and criteria.transaction_id and curr_transaction_id < criteria.transaction_id:
                print("Finish recovering based on transaction_id..")
                break
        
        # recovery query still example
        for log_entry in undo_lists:
            # recovery_query = construct_undo_query(log_entry)
            # query_processor.execute_undo(recovery_query)
            # write_log() recovery di sini?
            print(f"Recovered log: {log_entry}")

        # trus nulis status abort di log aku nunggu write log aja deh
                
                
        print("\nRecovery from aborting complete.\n")
        return
    
    


# if __name__ == "__main__":
#     try:
#         # Create a recovery criteria with transaction_id and timestamp
#         failurerec = FailureRecoveryManager()

#         print("\nExecution Result:")
#         # Create criteria to compare against
#         crit = RecoverCriteria(transaction_id=2, timestamp=datetime.datetime(2024, 11, 19, 15, 0))
#         # Check recovery
#         print(f"Recovery successful: {failurerec.recover(crit)}")
#     except Exception as e:
#         print(f"Error: {e}")
