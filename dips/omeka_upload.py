#!/usr/bin/env python
"""
Uploads a DIP to Omeka-S

Sends a DIP stored on the Storage Service to the Omeka-S host using it's REST API. Grabs metadata from the DIP METS file to create a new Omeka-S record and then attaches the DIP access file to it by way of URL Ingest. The DIP must be directly accessible via http for this to work.
"""

import argparse
import logging
import logging.config  # Has to be imported separately
import os
import json
import subprocess
import shutil
import sys
import amclient
import metsrw
import requests


THIS_DIR = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger("dip_workflow")


def setup_logger(log_file, log_level="INFO"):
    """Configures the logger to output to console and log file"""
    if not log_file:
        log_file = os.path.join(THIS_DIR, "dip_workflow.log")

    CONFIG = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "default": {
                "format": "%(levelname)-8s  %(asctime)s  %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            }
        },
        "handlers": {
            "console": {"class": "logging.StreamHandler", "formatter": "default"},
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "default",
                "filename": log_file,
                "backupCount": 2,
                "maxBytes": 10 * 1024,
            },
        },
        "loggers": {
            "dip_workflow": {"level": log_level, "handlers": ["console", "file"]}
        },
    }

    logging.config.dictConfig(CONFIG)


def main(
    omeka_api_url,
    omeka_api_key_identity,
    omeka_api_key_credential,
    ss_url,
    ss_user,
    ss_api_key,
    dip_uuid,
):
    """Sends the DIP to the AtoM host and a deposit request to the AtoM instance"""
    LOGGER.info("Downloading DIP %s from the storage service", dip_uuid)

    try:
        print(json.dumps(get_dip(ss_url, ss_user, ss_api_key, dip_uuid), indent=4))
    except Exception as e:
        LOGGER.error("Download of DIP from Storage Service failed: %s", e)
        return 2

    LOGGER.info("Starting upload to Omeka-S with DIP %s", dip_uuid)

    """

    try:
        deposit(omeka_api_url, omeka_api_key_identity, omeka_api_key_credential, dip_uuid)
    except Exception as e:
        LOGGER.error("Deposit request to AtoM failed: %s", e)
        return 2
    """

    LOGGER.info("DIP deposited in AtoM")


def get_dip(ss_url, ss_user, ss_api_key, dip_uuid):
    # USE AM Client to get info on the DIP
    am_client = amclient.AMClient(
        package_uuid=dip_uuid,
        ss_url=ss_url,
        ss_user_name=ss_user,
        ss_api_key=ss_api_key,
    )
    dip_details = am_client.get_package_details()

    dip_info = {}
    dip_info["dip-uuid"] = dip_details["uuid"]
    dip_info["dip-path"] = dip_details["current_full_path"]
    dip_info["dip-location"] = os.path.basename(
        os.path.dirname(dip_details["current_location"])
    )
    dip_info["aip-uuid"] = os.path.basename(
        os.path.dirname(dip_details["related_packages"][0])
    )
    # get mets file
    mets_name = "METS." + dip_info["aip-uuid"] + ".xml"
    am_client.relative_path = mets_name
    mets_file = am_client.extract_file()
    mets = metsrw.METSDocument.fromstring(mets_file)
    fsentries = mets.all_files()
    for fsentry in fsentries:
        print(fsentry.use)
    # get related AIP package info
    am_client.package_uuid = dip_info["aip-uuid"]
    aip_details = am_client.get_package_details()
    dip_info["aip-path"] = aip_details["current_full_path"]
    dip_info["aip-location"] = os.path.basename(
        os.path.dirname(aip_details["current_location"])
    )
    locations = am_client.list_storage_locations()["objects"]
    for location in locations:
        if location["uuid"] == dip_info["dip-location"]:
            dip_info["dip-bucket"] = os.path.basename(
                os.path.dirname(location["space"])
            )
        elif location["uuid"] == dip_info["aip-location"]:
            dip_info["aip-bucket"] = os.path.basename(
                os.path.dirname(location["space"])
            )

    return dip_info


def deposit(omeka_api_url, omeka_api_key_identity, omeka_api_key_credential, dip_uuid):
    """
    Generate and make deposit request to AtoM.

    :param str omeka_api_url: URL to the Omeka-S api
    :param str omeka_api_key_identity: AtoM user email
    :param str omeka_api_key_credential: AtoM user password
    :param str dip_uuid: absolute path to a DIP folder
    :raises Exception: if the AtoM response is not expected
    :returns: None
    """
    # Build headers dictionary for the deposit request
    headers = {}
    headers["User-Agent"] = "Archivematica"
    headers["X-Packaging"] = "http://purl.org/net/sword-types/METSArchivematicaDIP"
    headers["Content-Type"] = "application/zip"
    headers["X-No-Op"] = "false"
    headers["X-Verbose"] = "false"
    headers["Content-Location"] = "file:///{}".format(os.path.basename(dip_uuid))

    # Build URL and auth
    url = "{}/sword/deposit/{}".format(omeka_api_url)
    auth = requests.auth.HTTPBasicAuth(omeka_api_key_identity, omeka_api_key_credential)

    # Make request (disable redirects)
    LOGGER.info("Making deposit request to: %s", url)
    response = requests.request(
        "POST", url, auth=auth, headers=headers, allow_redirects=False
    )

    # AtoM returns 302 instead of 202, but Location header field is valid
    LOGGER.debug("Response code: %s", response.status_code)
    LOGGER.debug("Response location: %s", response.headers.get("Location"))
    LOGGER.debug("Response content:\n%s", response.content)

    # Check response status code
    if response.status_code not in [200, 201, 202, 302]:
        raise Exception("Response status code not expected")

    # Location is a must, if it is not included something went wrong
    if response.headers.get("Location") is None:
        raise Exception("Location header is missing in the response")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--omeka-api-url", metavar="URL", required=True, help="Omeka-S API endpoint."
    )
    parser.add_argument(
        "--omeka-api-key-identity",
        metavar="IDENTITY",
        required=True,
        help="Omeka user's API key identity.",
    )
    parser.add_argument(
        "--omeka-api-key-credential",
        metavar="CREDENTIAL",
        required=True,
        help="Omeka user's API key credential.",
    )
    parser.add_argument(
        "--ss-url",
        metavar="URL",
        help="Storage Service URL. Default: http://127.0.0.1:8000",
        default="http://127.0.0.1:8000",
    )
    parser.add_argument(
        "--ss-user",
        metavar="USERNAME",
        required=True,
        help="Username of the Storage Service user to authenticate as.",
    )
    parser.add_argument(
        "--ss-api-key",
        metavar="KEY",
        required=True,
        help="API key of the Storage Service user.",
    )
    parser.add_argument(
        "--dip-uuid",
        metavar="UUID",
        required=True,
        help="UUID of the the DIP to upload.",
    )

    # Logging
    parser.add_argument(
        "--log-file", metavar="FILE", help="Location of log file", default=None
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase the debugging output.",
    )
    parser.add_argument(
        "--quiet", "-q", action="count", default=0, help="Decrease the debugging output"
    )
    parser.add_argument(
        "--log-level",
        choices=["ERROR", "WARNING", "INFO", "DEBUG"],
        default=None,
        help="Set the debugging output level. This will override -q and -v",
    )

    args = parser.parse_args()

    log_levels = {2: "ERROR", 1: "WARNING", 0: "INFO", -1: "DEBUG"}
    if args.log_level is None:
        level = args.quiet - args.verbose
        level = max(level, -1)  # No smaller than -1
        level = min(level, 2)  # No larger than 2
        log_level = log_levels[level]
    else:
        log_level = args.log_level

    setup_logger(args.log_file, log_level)

    sys.exit(
        main(
            omeka_api_url=args.omeka_api_url,
            omeka_api_key_identity=args.omeka_api_key_identity,
            omeka_api_key_credential=args.omeka_api_key_credential,
            ss_url=args.ss_url,
            ss_user=args.ss_user,
            ss_api_key=args.ss_api_key,
            dip_uuid=args.dip_uuid,
        )
    )
