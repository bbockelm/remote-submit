# remote-submit

This repository has two pieces:
1. A script, `get_remote_submit_token.py`, which can be used to request an
   HTCondor "token" which allows you to **submit jobs to an HTCondor pool remotely**.
   "Remotely" means "not on the same machine as the HTCondor schedd".
1. An example of how to actually do the job submission, 
   how to get the input files to the submit host ("spooling" the input files),
   and how to retrieve the results.


## Acquiring a Remote Submit Token

**If you don't care about the technical details: 
run `python3 get_remote_submit_token.py` on the computer you would
like to submit jobs from and follow the prompts.
Skip to the next section.**

The `get_remote_submit_token.py` script acquires an 
[HTCondor IDTOKEN](https://htcondor.readthedocs.io/en/latest/admin-manual/security.html#token-authentication)
that is authorized to `READ` and `WRITE`.
`READ` allows the token holder to query the pool about jobs
(and other things, but we care about jobs).
Similarly, `WRITE` allows you to submit jobs to the pool,
take management actions on those jobs (like holding them),
and edit their attributes.

The token can be acquired by requesting it from any HTCondor daemon in the
trust domain you plan to submit to.
If you were an administrator generating arbitrary tokens,
you would likely just request them from the central manager,
and since you are an administrator you could then approve your own
arbitrary token requests.
As a user who does not have administrator privileges,
you need a different path.

It turns out that any user can approve a token request that is made for the
same identity that they authenticate as (as long as it only has authorizations
that they also have).
Thus, a user can request and approve a token for themselves as long as it only
allows them to do things they can already do.
Since CHTC users are given `READ` and `WRITE` authorizations through
identities of the form `<username>@fs` on submit hosts
(i.e., they authenticate through the filesystem),
they can also approve token requests sent to submit hosts 
for those authorizations
with identities of the form `<username>@fs`.

To actually make the token request, the client must authenticate.
We do this anonymously over SSL.
The script sets up SSL authentication for this purpose - there is some
duplication here that would not be necessary if HTCondor was installed on the
remote-submitting machine.

To approve the token request, the user logs in to their submit host as normal
(likely via SSH) and runs a `condor_token_request_approve` command generated
by the script. As noted above, they are allowed to do this by virtue of
their identity and authorizations provided by the existing CHTC security setup.

Once we have the token, we need to make sure HTCondor is configured to read it
later when submitting.
Therefore, the script also writes an HTCondor user configuration file
to `~/.condor/user_config` that sets `SEC_TOKEN_DIRECTORY` to the same location
the token was saved in (`~/.condor/tokens.d`).
This will be picked up in the future by the Python bindings, and they will use
the acquired token automatically.

The token request is roughly equivalent to running something like
```console
$ condor_token_request -pool cm.chtc.wisc.edu -name submit3.chtc.wisc.edu -type SCHEDD -authz READ -authz WRITE -identity karpel@fs
```
except that a full HTCondor system does not need to be installed on the 
remote-submitting machine (the HTCondor Python bindings do, though).


## Submitting and Managing Jobs Remotely

HTCondor is mostly indifferent to whether jobs are submitted 
"locally" or "remotely".
The main difference is this:
**HTCondor file transfer to and from jobs can only occur on the submit host itself**.
This impacts several potential use cases:
1. Files in `transfer_input_files` that are not URL transfers.
1. Files in `transfer_output_files` that are not remapped to URL transfers.
1. Capturing of `stdout` and `stderr` from the job.

Each of these is handled by a different mechanism. 
All three mechanisms are shown in `remote_submit.py`, with comments
highlighting which parts do what.
Below, I describe the theory behind each mechanism in more detail.
The `submit` function in `remote_submit.py` can likely be used without
any modification, but you may wish to write a different version of `retrieve`
depending on your particular requirements.

If you do not care about capturing streams and all of your input and output
files (including your executable) are either already present in the job
sandbox (e.g., your executable is baked into a Docker container) 
or can be transferred via URL-like transfer (HTTP, S3, etc.), you
may not need any of this machinery.
In that case, just submit your jobs normally, without spooling or retrieval.


### Input File Transfer (Spooling)

*The advice in this section is subject to change in a future version of HTCondor,
since we hope to make this process significantly easier
(see [this ticket](https://htcondor-wiki.cs.wisc.edu/index.cgi/tktview?tn=7771)).*

In this use case, you have a submit description that depends on some files
stored on the machine you are remote-submitting from.
Perhaps these are input files for analysis, or your job's `executable`.
Because HTCondor cannot transfer these files from your computer to job (only
from the submit host to the job), these files must be explicitly transferred
to the submit host before the job is allowed to run.
This process is called **spooling the input sandbox**, or just spooling.

Since spooling is a common feature of remote submit workflows, HTCondor will
help you do it.
The [`Schedd.spool`](https://htcondor.readthedocs.io/en/latest/apis/python-bindings/api/htcondor.html#htcondor.Schedd.spool)
method takes a list of job ads for submitted jobs and spools their input files.

Because the jobs should not be run until their inputs are spooled, you must
submit the job in the `held` state by setting `held = true` in the submit
description, with the appropriate hold reason code 
(see `remote_submit.py` for an example).
`Schedd.spool` will also release the jobs once it is finished transferring 
input files.

The list of job ads can be generated in several ways.
Right now, `remote_submit.py` uses the
[`Submit.jobs`](https://htcondor.readthedocs.io/en/latest/apis/python-bindings/api/htcondor.html#htcondor.Submit.jobs)
method, along with some information from the submit result, to generate the job
ads.


### Output File Transfer (Retrieving)

In this use case, you have some output files that are *not* URL transfers
(i.e., HTCondor is going to transfer them back to the submit host).
You must retrieve this output manually.

Again, since this is a common feature of remote submit workflows, HTCondor will
help you do it.
We can use the `Schedd.retrieve` method, passing an expression that says which
jobs to retrieve output for.
In our case, a simple `ClusterID` constraint based on the id we
get from the submit result will do.
The job outputs will all be placed in the current working directory.
The method will not fail when job output is not available; you will need to
query the schedd to determine whether the jobs are finished or not.

In addition, you must ensure that the job output is actually available to be 
retrieved.
Normally, when a job finishes, HTCondor transfers the output files back to the
submit host, then removes the job from the queue, assuming that the output files
moved to some permanent location.
For a remote submit, they do not: you must tell HTCondor to leave the job in the
queue until you have a change to retrieve the output files.
In `remote_submit.py`, we implement this by writing a `leave_in_queue`
expression that leaves completed jobs in the queue for up to three days after
completion, or until we mark the output as retrieved by changing the
`RETRIEVED` attribute of the job ad.

The example `retrieve` function in `remote_submit.py` puts this all together by
querying the schedd in a loop to determine the job status, using
[`Schedd.query`](https://htcondor.readthedocs.io/en/latest/apis/python-bindings/api/htcondor.html#htcondor.Schedd.query).
When all of the jobs in the given cluster are done, it retrieves all of their
output, then sets `RETRIEVED` to `true`, allowing the job to leave the queue.
A more advanced version of `retrieve` could return partial results as individual
jobs in the cluster complete.

**In real life, don't query more than once every few minutes.
Depending on your necessary turnaround time,
as little as one check per hour or per day might be sufficient.**


### Job Output Stream Capture

HTCondor automatically captures the `stdout` and `stderr` of jobs.
The submit descriptors `output` and `error` specify the files to put the
captured streams in.
Those files are normally on the submit host.
To get them back to the remote-submitting computer, they must be caught by the
`Schedd.retrieve` method that is also used for output file transfer.
These files are not normally transferred by `Schedd.retrieve` 
due to special naming rules;
to bypass these rules, use `transfer_output_remaps` to rename the files as
they are transferred back from the job.

*Note: you could also use `transfer_output_remaps` to simply send those files
to some other location, like a normal URL transfer.*
