import datetime
import os
import time

import classad
import htcondor

# htcondor.param["ALL_DEBUG"] = "D_SECURITY D_FULLDEBUG"
# htcondor.enable_debug()

REMOVAL_DELAY = datetime.timedelta(hours=1).total_seconds()
COMPLETION_DATE = "CompletionDate"
COMPLETED = 4

sub = htcondor.Submit(
    {
        "executable": "/bin/cat",
        "arguments": "$(item)",
        "transfer_input_files": "$(item)",
        "output": "test-$(ProcID).out",
        "error": "test-$(ProcID).err",
        "request_cpus": "1",
        "request_memory": "1GB",
        "request_disk": "1GB",
        "hold": "true",
        "My.HoldReason": classad.quote("Spooling input files"),
        "My.HoldReasonCode": "16",
        "My.LeaveJobInQueue": f"JobStatus == {COMPLETED} && ( {COMPLETION_DATE} =?= UNDEFINED || {COMPLETION_DATE} == 0 || ((time() - {COMPLETION_DATE}) < {REMOVAL_DELAY}) )",
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

count = 1
itemdata = [{"item": "README.md"}, {"item": "LICENSE"}, {"item": ".gitignore"}]
with schedd.transaction() as txn:
    result = sub.queue_with_itemdata(txn, count=count, itemdata=iter(itemdata))

clusterid = result.cluster()
ads = list(sub.jobs(count=count, itemdata=iter(itemdata), clusterid=clusterid))
print(len(ads))
schedd.spool(ads)

constraint = f"ClusterID == {clusterid}"

print("submit result is", clusterid)

while True:
    time.sleep(1)

    statuses = [ad["JobStatus"] for ad in schedd.query(constraint=constraint)]
    print(statuses)

    if all(status == COMPLETED for status in statuses):
        break

schedd.retrieve(constraint)
print(schedd.act(htcondor.JobAction.Remove, constraint))
