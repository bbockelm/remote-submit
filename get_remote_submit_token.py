#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

import htcondor
import classad


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="Request a token that allow remote submission of jobs to an HTCondor pool."
    )

    parser.add_argument(
        "--submit-host",
        default=None,
        help="The name of the submit host to request a token from (e.g., submit3.chtc.wisc.edu). If omitted, you will be asked for this interactively.",
    )
    parser.add_argument(
        "--username",
        default=None,
        help="Your username on the submit host you are requesting a token from. If omitted, you will be asked for this interactively.",
    )

    default_pool = "cm.chtc.wisc.edu"
    parser.add_argument(
        "--pool",
        default=default_pool,
        help=f"The address of the pool's central manager. The default pool is {default_pool}",
    )

    default_token_dir = Path.home() / ".condor" / "tokens.d"
    parser.add_argument(
        "--token-dir",
        default=default_token_dir,
        type=Path,
        help=f"The directory to store the acquired token in. Defaults to {default_token_dir}",
    )

    default_bounding_set = ["READ", "WRITE"]
    parser.add_argument(
        "--authz",
        dest="authorizations",
        nargs="*",
        default=default_bounding_set,
        help=f"Which authorizations to request. Defaults to {' '.join(default_bounding_set)}, the minimum necessary for remote submission and management.",
    )

    parser.add_argument(
        "--debug",
        default=False,
        action="store_true",
        help="Enable HTCondor debug logging.",
    )

    args = parser.parse_args(args)

    return args


def main(
    submit_host=None,
    username=None,
    pool=None,
    authorizations=None,
    token_dir=None,
    modify_config=True,
    debug=False,
):
    if debug:
        htcondor.param["TOOL_DEBUG"] = "D_SECURITY D_FULLDEBUG"
        htcondor.enable_debug()

    # We need to override existing settings and authenticate via SSL (anonymously)
    htcondor.param["SEC_CLIENT_AUTHENTICATION_METHODS"] = "SSL"
    if not sys.platform.startswith("win32"):
        # on Ubuntu, set to the correct path (the default is for CentOS)
        if not Path("/etc/pki/tls/certs/ca-bundle.crt").exists():
            htcondor.param[
                "AUTH_SSL_CLIENT_CAFILE"
            ] = "/etc/ssl/certs/ca-certificates.crt"

    token_dir.mkdir(parents=True, exist_ok=True)
    token_dir.chmod(0o700)
    htcondor.param["SEC_TOKEN_DIRECTORY"] = str(token_dir)
    if modify_config:
        config_path = Path.home() / ".condor" / "user_config"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_lines = [
            f"SEC_TOKEN_DIRECTORY = {str(token_dir)}",
        ]

        if not config_path.exists() or any(
            config_line not in config_path.read_text() for config_line in config_lines
        ):
            with config_path.open(mode="a") as f:
                f.writelines(config_lines)

            print(f"Added config to {config_path}")

    if submit_host is None:
        submit_host = input(
            "What is the address of your submit host (e.g., submit3.chtc.wisc.edu)? "
        )

    if username is None:
        username = input("What is your username on your submit host? ")
    identity = f"{username}@fs"

    if pool is None:
        pool = input("What is the address of your pool's central manager? ")

    if authorizations is None:
        authorizations = [
            authz.upper()
            for authz in input(
                "What are the authorizations you need (e.g., READ WRITE)? "
            ).split()
        ]

    collector = htcondor.Collector(pool)
    target = collector.locate(htcondor.DaemonTypes.Schedd, submit_host)
    # Pretend that the target is a collector
    target["MyType"] = "Collector"

    print("Requesting token...")

    request = make_token_request(identity, authorizations, target)

    lines = [
        f"Your token request id is: {request.request_id}",
        f"To approve the token request, log in to {username}@{submit_host} via SSH and run:",
        f"condor_token_request_approve -name {submit_host} -type SCHEDD -reqid {request.request_id}",
    ]
    print("\n".join(lines))

    token = request.result(0)

    token_name = f"remote-submit-for-{submit_host}"

    token.write(token_name)

    print(f"Success! Token saved to {token_dir / token_name}")


def make_token_request(identity, authorizations, target_ad):
    req = htcondor.TokenRequest(identity, bounding_set=authorizations)
    req.submit(target_ad)

    # TODO: temporary fix for https://htcondor-wiki.cs.wisc.edu/index.cgi/tktview?tn=7641
    # Yes, we could, in principle, hit the recursion limit here, but we would have to
    # get exceedingly unlucky, and this is a simple, straightforward fix.
    # Once we upgrade the server to whatever version of HTCondor this is fixed in,
    # we can drop this code entirely.
    if req.request_id.startswith("0"):
        return make_token_request(identity, authorizations, target_ad)

    return req


if __name__ == "__main__":
    args = parse_args()
    main(**vars(args))
