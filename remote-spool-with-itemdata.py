import os
import time

import classad
import htcondor

# htcondor.param["ALL_DEBUG"] = "D_SECURITY D_FULLDEBUG"
# htcondor.enable_debug()

TEN_DAYS = 60 * 60 * 24 * 10
COMPLETION_DATE = "CompletionDate"

sub = htcondor.Submit(
    {
        "executable": "/bin/cat",
        "arguments": "README.md",
        "transfer_input_files": "README.md",
        "output": "test-$(ProcID).out",
        "error": "test-$(ProcID).err",
        "request_cpus": "1",
        "request_memory": "1GB",
        "request_disk": "1GB",
        "hold": "true",
        "My.HoldReason": classad.quote("Spooling input files"),
        "My.HoldReasonCode": "16",
        # "My.LeaveJobInQueue": f"JobStatus == 5 && ( {COMPLETION_DATE} =?= UNDEFINED || {COMPLETION_DATE} == 0 || ((time() - {COMPLETION_DATE}) < {TEN_DAYS}) )",
        "My.LeaveJobInQueue": "true",
        "transfer_output_remaps": classad.quote(
            "_condor_stdout=test-$(ProcID).out ; _condor_stderr=test-$(ProcID).err"
        ),
    }
)

collector = htcondor.Collector("cm.chtc.wisc.edu")

schedd_ad = collector.locate(
    htcondor.DaemonTypes.Schedd, "submittest0000.chtc.wisc.edu"
)

print(schedd_ad)

schedd = htcondor.Schedd(schedd_ad)

ads = []
with schedd.transaction() as txn:
    result = sub.queue(txn, count=5, ad_results=ads)

schedd.spool(ads)

for ad in ads:
    print(ad)

constraint = f"ClusterID == {result}"

print("submit result is", result)

while True:
    time.sleep(1)

    statuses = [ad["JobStatus"] for ad in schedd.query(constraint=constraint)]
    print(statuses)

    if all(status == 4 for status in statuses):
        break

schedd.retrieve(constraint)
