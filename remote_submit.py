import datetime
import time

import classad
import htcondor

DEFAULT_REMOVAL_DELAY = datetime.timedelta(days=3).total_seconds()
COMPLETION_DATE = "CompletionDate"
COMPLETED = 4
RETRIEVED = "RETRIEVED"


def get_schedd(pool, schedd):
    collector = htcondor.Collector(pool)

    schedd_ad = collector.locate(htcondor.DaemonTypes.Schedd, schedd)

    return htcondor.Schedd(schedd_ad)


def submit(
    submit, *, count=1, itemdata=None, pool, schedd, removal_delay=DEFAULT_REMOVAL_DELAY
):
    # This is a custom marker attribute, used below in the leave_in_queue expression
    submit[f"My.{RETRIEVED}"] = "false"

    # This expression controls when the job will be allowed to leave the queue.
    # Either:
    # 1. It is completed and a certain amount of time has passed.
    # 2. We mark it retrieved, using the custom attribute above.
    submit[
        "leave_in_queue"
    ] = f"JobStatus == ( {COMPLETED} && ( {COMPLETION_DATE} =?= UNDEFINED || {COMPLETION_DATE} == 0 || ((time() - {COMPLETION_DATE}) < {removal_delay}) ) ) || {RETRIEVED} IS false"

    # Jobs with input files need to submitted on hold.
    # They will be released when we "spool" the input files later.
    submit["hold"] = "true"
    submit["My.HoldReason"] = classad.quote("Spooling input files")
    submit["My.HoldReasonCode"] = "16"

    # We need to rewrite the transfer_output_remaps slightly to be able to
    # get stdout and stderr back when we retrieve output.
    tor = submit.get("transfer_output_remaps", "").replace('"', "").split(";")
    if submit.get("output", ""):
        tor.append(f"_condor_stdout={submit['output']}")
    if submit.get("error", ""):
        tor.append(f"_condor_stdout={submit['error']}")
    submit["transfer_output_remaps"] = classad.quote(" ; ".join(filter(None, tor)))

    # Mostly-normal Python bindings job submission from here on, except
    # that we need to reconstruct the submitted job ads. Hopefully we won't need
    # to do this part in the future, and will be able to get them directly
    # from the submit result.
    itemdata = list(itemdata or [])

    schedd = get_schedd(pool, schedd)

    with schedd.transaction() as txn:
        result = submit.queue_with_itemdata(txn, count=count, itemdata=iter(itemdata))

    clusterid = result.cluster()
    ads = list(submit.jobs(count=count, itemdata=iter(itemdata), clusterid=clusterid))

    # Spool the input files. This also releases the jobs.
    schedd.spool(ads)

    return result


def retrieve(cluster_id, pool, schedd):
    constraint = f"ClusterID == {cluster_id}"

    schedd = get_schedd(pool, schedd)

    while True:
        time.sleep(5)

        statuses = [
            ad["JobStatus"]
            for ad in schedd.query(constraint=constraint, projection=["JobStatus"])
        ]
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
        result.cluster(),
        pool="cm.chtc.wisc.edu",
        schedd="submittest0000.chtc.wisc.edu",
    )
