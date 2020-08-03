import htcondor

from functions import submit, retrieve

sub = htcondor.Submit(
    {
        "executable": "/bin/cat",
        "arguments": "$(input_file_name)",
        "transfer_input_files": "$(input_file_path)",
        "output": "test-$(ProcID).out",
        "error": "test-$(ProcID).err",
        "request_cpus": "1",
        "request_memory": "1GB",
        "request_disk": "1GB",
        # signing happens on the submit machine
        "aws_access_key_id_file": "/home/karpel/.chtc_s3/access.key",
        "aws_secret_access_key_file": "/home/karpel/.chtc_s3/secret.key",
    }
)

host = "s3dev.chtc.wisc.edu"
in_bucket = "test-remote-in"
files = ["1.txt", "2.txt", "3.txt"]

result = submit(
    sub,
    count=1,
    itemdata=[
        {"input_file_path": f"s3://{host}/{in_bucket}/{file}", "input_file_name": file,}
        for file in files
    ],
    pool="cm.chtc.wisc.edu",
    schedd="submittest0000.chtc.wisc.edu",
)

retrieve(
    result, pool="cm.chtc.wisc.edu", schedd="submittest0000.chtc.wisc.edu",
)
