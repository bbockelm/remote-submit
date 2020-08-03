import datetime
import time

import classad
import htcondor

REMOVAL_DELAY = datetime.timedelta(days=3).total_seconds()
COMPLETION_DATE = "CompletionDate"
COMPLETED = 4
RETRIEVED = "RETRIEVED"


def get_schedd(pool, schedd):
    collector = htcondor.Collector(pool)

    schedd_ad = collector.locate(htcondor.DaemonTypes.Schedd, schedd)

    return htcondor.Schedd(schedd_ad)


def submit(submit, *, count=1, itemdata=None, pool, schedd):
    submit[f"My.{RETRIEVED}"] = "false"
    submit["hold"] = "true"
    submit["My.HoldReason"] = classad.quote("Spooling input files")
    submit["My.HoldReasonCode"] = "16"
    submit[
        "My.LeaveJobInQueue"
    ] = f"JobStatus == {COMPLETED} && ( {COMPLETION_DATE} =?= UNDEFINED || {COMPLETION_DATE} == 0 || ((time() - {COMPLETION_DATE}) < {REMOVAL_DELAY}) || {RETRIEVED} )"

    tor = submit.get("transfer_output_remaps", "").replace('"', "").split(";")
    print(tor)
    if submit.get("output", ""):
        tor.append(f"_condor_stdout={submit['output']}")
    if submit.get("error", ""):
        tor.append(f"_condor_stdout={submit['error']}")
    submit["transfer_output_remaps"] = classad.quote(" ; ".join(filter(None, tor)))

    print(submit)

    itemdata = list(itemdata or [])

    schedd = get_schedd(pool, schedd)

    with schedd.transaction() as txn:
        result = submit.queue_with_itemdata(txn, count=count, itemdata=iter(itemdata))

    clusterid = result.cluster()
    ads = list(submit.jobs(count=count, itemdata=iter(itemdata), clusterid=clusterid))

    schedd.spool(ads)

    return result


def retrieve(submit_result, pool, schedd):
    constraint = f"ClusterID == {submit_result.cluster()}"

    schedd = get_schedd(pool, schedd)

    while True:
        time.sleep(1)

        statuses = [ad["JobStatus"] for ad in schedd.query(constraint=constraint)]
        print(statuses)

        if all(status == COMPLETED for status in statuses):
            break

    schedd.retrieve(constraint)
    schedd.edit(constraint, RETRIEVED, "true")


if __name__ == "__main__":
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
        }
    )

    result = submit(
        sub,
        count=1,
        itemdata=[{"item": "README.md"}, {"item": "LICENSE"}, {"item": ".gitignore"}],
        pool="cm.chtc.wisc.edu",
        schedd="submittest0000.chtc.wisc.edu",
    )

    retrieve(
        result, pool="cm.chtc.wisc.edu", schedd="submittest0000.chtc.wisc.edu",
    )
