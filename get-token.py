import socket
from pathlib import Path

import htcondor
import classad

DEFAULT_PORT = 9618
TOKEN_BOUNDING_SET = ["READ", "WRITE"]


def make_token_request(identity, collector_ad):
    req = htcondor.TokenRequest(identity, bounding_set=TOKEN_BOUNDING_SET)
    req.submit(collector_ad)

    # TODO: temporary fix for https://github.com/HTPhenotyping/registration/issues/10
    # Yes, we could, in principle, hit the recursion limit here, but we would have to
    # get exceedingly unlucky, and this is a simple, straightforward fix.
    # Once we upgrade the server to whatever version of HTCondor this is fixed in,
    # we can drop this code entirely.
    if req.request_id.startswith("0"):
        return make_token_request(identity, collector_ad)

    return req


if __name__ == "__main__":
    identity = "karpel@fs"

    pool = "cm.chtc.wisc.edu"

    if ":" in pool:
        alias, port = pool.split(":")
    else:
        alias = pool
        port = DEFAULT_PORT

    ip, port = socket.getaddrinfo(alias, int(port), socket.AF_INET)[0][4]

    coll_ad = classad.ClassAd(
        {
            "MyAddress": "<{}:{}?alias={}>".format(ip, port, alias),
            "MyType": "Collector",
        }
    )

    htcondor.param["SEC_CLIENT_AUTHENTICATION_METHODS"] = "SSL"

    req = make_token_request(identity, coll_ad)

    print(req.request_id)

    token = req.result(0)

    token_name = "remote-submit-test"

    token.write(token_name)
