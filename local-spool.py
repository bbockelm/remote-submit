import os

import classad
import htcondor

TEN_DAYS = 60 * 60 * 24 * 10
COMPLETION_DATE = "CompletionDate"

sub = htcondor.Submit(
    {
        "executable": "/bin/cat",
        "arguments": "README.md",
        "transfer_input_files": "README.md",
        "output": "test.out",
        "error": "test.err",
        "hold": "true",
        "My.HoldReason": classad.quote("Spooling input files"),
        "My.HoldReasonCode": "16",
        # "My.LeaveJobInQueue": f"JobStatus == 5 && ( {COMPLETION_DATE} =?= UNDEFINED || {COMPLETION_DATE} == 0 || ((time() - {COMPLETION_DATE}) < {TEN_DAYS}) )",
        "My.LeaveJobInQueue": "true",
        "transfer_output_remaps": classad.quote(
            "_condor_stdout=test.out ; _condor_stderr=test.err"
        ),
    }
)

schedd = htcondor.Schedd()

ads = []
with schedd.transaction() as txn:
    result = sub.queue(txn, ad_results=ads)

schedd.spool(ads)

for ad in ads:
    print(ad)

print("submit result is", result)

for ad in schedd.query():
    print(ad)

schedd.retrieve(f"ClusterID == {result}")
