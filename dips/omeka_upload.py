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
from lxml import etree
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
        dip_info, mets = get_dip(ss_url, ss_user, ss_api_key, dip_uuid)
    except Exception as e:
        LOGGER.error("Download of DIP from Storage Service failed: %s", e)
        return 2

    LOGGER.info("Parsing metadata from METS file for DIP %s", dip_uuid)
    try:
        data = parse_mets(
            omeka_api_url,
            omeka_api_key_identity,
            omeka_api_key_credential,
            dip_info,
            mets,
        )
    except Exception as e:
        LOGGER.error("Unable to parse METS file and build json for upload: %s", e)
        return 2

    LOGGER.info("Starting upload to Omeka-S with DIP %s", dip_uuid)

    try:
        deposit(
            omeka_api_url, omeka_api_key_identity, omeka_api_key_credential, data,
        )
    except Exception as e:
        LOGGER.error("Deposit request to Omeka-S failed: %s", e)
        return 2

    LOGGER.info("DIP deposited in Omeka-S")


def get_dip(ss_url, ss_user, ss_api_key, dip_uuid):
    # USE AM Client to get info on the DIP and AIP
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
    temp_dir = "/var/archivematica/sharedDirectory/tmp"
    am_client.directory = temp_dir
    mets_name = "METS." + dip_info["aip-uuid"] + ".xml"
    am_client.relative_path = mets_name
    try:
        am_client.extract_file()
        mets = metsrw.METSDocument.fromfile(os.path.join(temp_dir, mets_name))
    except Exception as e:
        LOGGER.error("Unable to extract and load METS file: %s", e)
        return 2
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

    return dip_info, mets


def parse_mets(
    omeka_api_url, omeka_api_key_identity, omeka_api_key_credential, dip_info, mets
):
    namespaces = metsrw.utils.NAMESPACES.copy()
    namespaces["premis"] = "http://www.loc.gov/premis/v3"
    dc_xml = mets.tree.find(
        'mets:dmdSec/mets:mdWrap[@MDTYPE="DC"]/mets:xmlData/dcterms:dublincore',
        namespaces=namespaces,
    )
    custom_xml = mets.tree.find(
        'mets:dmdSec/mets:mdWrap[@MDTYPE="OTHER"]/mets:xmlData', namespaces=namespaces,
    )
    # set items as private as default
    data = {"o:is_public": 0}
    # get properties for correct id
    params = {
        "key_identity": omeka_api_key_identity,
        "key_credential": omeka_api_key_credential,
    }
    vocabularies = requests.get(omeka_api + "vocabularies", params=params).json()
    dcTerms = next(item for item in vocabularies if item["o:prefix"] == "dcterms")
    dcType = next(item for item in vocabularies if item["o:prefix"] == "dctype")
    properties = requests.get(
        omeka_api + "properties?per_page=100&vocabulary_id=" + str(dcTerms["o:id"]),
        params=params,
    ).json()
    types = requests.get(
        omeka_api
        + "resource_classes?per_page=100&vocabulary_id="
        + str(dcType["o:id"]),
        params=params,
    ).json()
    # add to processing set if exist, else create
    sets = requests.get(omeka_api + "item_sets", params=params).json()
    processing_set_id = ""
    if sets is not None:
        for set in sets:
            if set["o:title"] == "Processing":
                processing_set_id = set["o:id"]
    if processing_set_id == "":
        property = next(
            item for item in properties if item["o:term"] == ("dcterms:title")
        )
        set_json = {
            "o:is_public": 0,
            "dcterms:title": [
                {
                    "type": "literal",
                    "property_id": property["o:id"],
                    "@value": "Processing",
                }
            ],
        }
        set_response = requests.post(
            omeka_api + "item_sets", params=params, json=set_json,
        )
        print(set_response.json())
        processing_set_id = set_response.json()["o:id"]
    data["o:item_set"] = [{"o:id": processing_set_id}]

    if dc_xml is not None:
        for element in dc_xml:
            if etree.QName(element).localname == "identifier":
                property = next(
                    item
                    for item in properties
                    if item["o:term"] == ("dcterms:" + etree.QName(element).localname)
                )
                appending_data = {
                    "type": "uri",
                    "@id": element.text,
                    "o:label": "Reference Code",
                    "property_id": property["o:id"],
                }
            elif etree.QName(element).localname == "type":
                # use to set resource class as well
                type = next(item for item in types if item["o:label"] == element.text)
                if type is not None:
                    data["o:resource_class"] = {"o:id": type["o:id"]}
                property = next(
                    item
                    for item in properties
                    if item["o:term"] == ("dcterms:" + etree.QName(element).localname)
                )
                appending_data = {
                    "type": "literal",
                    "@value": element.text,
                    "property_id": property["o:id"],
                }
            else:
                property = next(
                    item
                    for item in properties
                    if item["o:term"] == ("dcterms:" + etree.QName(element).localname)
                )
                appending_data = {
                    "type": "literal",
                    "@value": element.text,
                    "property_id": property["o:id"],
                }

            if ("dcterms:" + etree.QName(element).localname) in data:
                data["dcterms:" + etree.QName(element).localname].append(appending_data)
            else:
                data["dcterms:" + etree.QName(element).localname] = []
                data["dcterms:" + etree.QName(element).localname].append(appending_data)

    if custom_xml is not None:
        for customElement in custom_xml:
            property = next(
                item for item in properties if item["o:term"] == ("dcterms:identifier")
            )
            appending_data = {
                "type": "uri",
                "@id": customElement.text,
                "o:label": etree.QName(customElement).localname,
                "property_id": property["o:id"],
                # set these identifiers as private as default
                "is_public": 0,
            }
            if ("dcterms:identifier") in data:
                data["dcterms:identifier"].append(appending_data)
            else:
                data["dcterms:identifier"] = []
                data["dcterms:identifier"].append(appending_data)

    # if there is no metadata at all, use the premis original name as identifier

    if (dc_xml is None) and (custom_xml is None):
        premis_xml = mets.tree.find(
            'mets:dmdSec/mets:mdWrap[@MDTYPE="PREMIS:OBJECT"]/mets:xmlData/premis:object/premis:originalName',
            namespaces=namespaces,
        )
        property = next(
            item for item in properties if item["o:term"] == ("dcterms:identifier")
        )
        appending_data = {
            "type": "uri",
            "@id": premis_xml.text,
            "o:label": "Premis Identifier",
            "property_id": property["o:id"],
            # set these identifiers as private as default
            "is_public": 0,
        }
        if ("dcterms:identifier") in data:
            data["dcterms:identifier"].append(appending_data)
        else:
            data["dcterms:identifier"] = []
            data["dcterms:identifier"].append(appending_data)

    # add dip/aip info to data
    property = next(
        item for item in properties if item["o:term"] == ("dcterms:identifier")
    )
    dip_data = {
        "type": "uri",
        "@id": dip_info["dip-uuid"],
        "o:label": "dip-uuid",
        "property_id": property["o:id"],
        # set these identifiers as private as default
        "is_public": 0,
    }
    if ("dcterms:identifier") in data:
        data["dcterms:identifier"].append(dip_data)
    else:
        data["dcterms:identifier"] = []
        data["dcterms:identifier"].append(dip_data)
    aip_data = {
        "type": "uri",
        "@id": dip_info["aip-uuid"],
        "o:label": "aip-uuid",
        "property_id": property["o:id"],
        # set these identifiers as private as default
        "is_public": 0,
    }
    if ("dcterms:identifier") in data:
        data["dcterms:identifier"].append(aip_data)
    else:
        data["dcterms:identifier"] = []
        data["dcterms:identifier"].append(aip_data)

    # create media
    if type is not None:
        if type["o:label"] == "Still Image" or type["o:label"] == "Image":
            data["o:media"] = [
                {
                    "o:ingester": "image",
                    "master": "https://"
                    + dip_info["aip-bucket"]
                    + ".s3.amazonaws.com/"
                    + dip_info["aip-path"],
                }
            ]
    return data


def deposit(omeka_api_url, omeka_api_key_identity, omeka_api_key_credential, data):
    # Deposits json data into Omeka-s
    LOGGER.info("Posting data to Omeka-S")
    params = {
        "key_identity": omeka_api_key_identity,
        "key_credential": omeka_api_key_credential,
    }
    response = requests.post(omeka_api_url + "items", params=params, json=data,)

    #
    LOGGER.debug("Response code: %s", response.status_code)
    LOGGER.debug("Response content:\n%s", response.content)

    # Check response status code
    if response.status_code not in [200, 201, 202, 302]:
        raise Exception("Response status code not expected")
    else:
        if ("@id") in response.json():
            LOGGER.info("Created new Omeka-S resource: %s", response.json()["@id"])


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
