import FailureRecoveryManager

if __name__ == "__main__":
    frm = FailureRecoveryManager.FailureRecoveryManager()
    results = frm.parse_log_file("wal.log")

    for result in results:
        print(result.transaction_id,result.timestamp,result.before,result.data, result.query)
