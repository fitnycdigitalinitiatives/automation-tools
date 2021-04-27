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
import base64
from lxml import etree
import requests
import urllib
from pyquery import PyQuery


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
    omeka_api,
    omeka_api_key_identity,
    omeka_api_key_credential,
    ss_url,
    ss_user,
    ss_api_key,
    pipeline_uuid,
    processing_uuid,
    s3_uuid,
    shared_directory,
    dip_path,
    imgser_url,
):
    LOGGER.info("Checking for DIP's in the processing directory")

    try:
        dip_list = get_dip_list(
            ss_url,
            ss_user,
            ss_api_key,
            pipeline_uuid,
            processing_uuid,
            shared_directory,
            dip_path,
        )
    except Exception as e:
        LOGGER.error("Unable to located DIP's in processing directory: %s", e)
        return 2

    for dip in dip_list:
        LOGGER.info("Processing DIP %s", dip)
        try:
            dip_info, mets = process_dip(
                ss_url,
                ss_user,
                ss_api_key,
                dip,
                pipeline_uuid,
                processing_uuid,
                s3_uuid,
                shared_directory,
                dip_path,
            )
        except Exception as e:
            LOGGER.error("Processing of DIP failed: %s", e)
            return 2

        LOGGER.info("Parsing metadata from METS file for DIP %s", dip_info["dip-uuid"])
        try:
            data = parse_mets(
                omeka_api,
                omeka_api_key_identity,
                omeka_api_key_credential,
                dip_info,
                mets,
                imgser_url,
            )
        except Exception as e:
            LOGGER.error("Unable to parse METS file and build json for upload: %s", e)
            return 2

        LOGGER.info("Starting upload to Omeka-S with DIP %s", dip_info["dip-uuid"])

        try:
            deposit(
                omeka_api, omeka_api_key_identity, omeka_api_key_credential, data,
            )
        except Exception as e:
            LOGGER.error("Deposit request to Omeka-S failed: %s", e)
            return 2

        LOGGER.info("DIP deposited in Omeka-S")

        # Delete local copies of DIP
        LOGGER.info("Deleting local copies of the DIP.")
        try:
            shutil.rmtree(os.path.join(shared_directory, dip_path, dip))
        except (OSError, shutil.Error) as e:
            LOGGER.warning("DIP removal failed: %s", e)

    LOGGER.info("All DIP's processed and deposited in Omeka-S")


def get_dip_list(
    ss_url,
    ss_user,
    ss_api_key,
    pipeline_uuid,
    processing_uuid,
    shared_directory,
    dip_path,
):
    url = ss_url + "/api/v2/location/" + processing_uuid + "/browse/"
    params = {"username": ss_user, "api_key": ss_api_key}
    params["path"] = base64.b64encode(dip_path)

    browse_info = requests.get(url, params).json()
    entries = browse_info["directories"]
    if entries:
        entries = [base64.b64decode(e.encode("utf8")) for e in entries]
        return entries
    else:
        LOGGER.error("Unable to locate any DIP's to process.")


def process_dip(
    ss_url,
    ss_user,
    ss_api_key,
    dip,
    pipeline_uuid,
    processing_uuid,
    s3_uuid,
    shared_directory,
    dip_path,
):
    # remove ContentDm file
    contentdm = os.path.join(shared_directory, dip_path, dip, "objects/compound.txt")
    try:
        os.remove(contentdm)
    except Exception as e:
        LOGGER.warning("Unable to remove contentDM file: %s", e)

    # Get AIP UUID
    aip_uuid = dip[-36:]

    # USE AM Client to get info on the AIP
    am_client = amclient.AMClient(
        package_uuid=aip_uuid,
        ss_url=ss_url,
        ss_user_name=ss_user,
        ss_api_key=ss_api_key,
    )
    try:
        aip_details = am_client.get_package_details()
    except Exception as e:
        LOGGER.error("Unable to locate valid AIP package: %s", e)
        return 2

    # Get file list in DIP
    object_list = []
    thumbnail_list = []
    object_path = os.path.join(shared_directory, dip_path, dip, "objects")
    for root, _, files in os.walk(object_path):
        for name in files:
            rel_dir = os.path.relpath(
                root, os.path.join(shared_directory, dip_path, dip)
            )
            object_list.append(os.path.join(rel_dir, name))
    if not object_list:
        LOGGER.error("Unable to find any access files in the DIP.")
        return 2

    thumbnail_path = os.path.join(shared_directory, dip_path, dip, "thumbnails")
    for root, _, files in os.walk(thumbnail_path):
        for name in files:
            rel_dir = os.path.relpath(
                root, os.path.join(shared_directory, dip_path, dip)
            )
            thumbnail_list.append(os.path.join(rel_dir, name))

    # get mets file
    mets_name = "METS." + aip_uuid + ".xml"
    try:
        mets = metsrw.METSDocument.fromfile(
            os.path.join(shared_directory, dip_path, dip, mets_name)
        )
    except Exception as e:
        LOGGER.error("Unable to extract and load METS file: %s", e)
        return 2

    # Compile data for upload to S3
    size = 0
    for dirpath, _, filenames in os.walk(os.path.join(shared_directory, dip_path, dip)):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            size += os.path.getsize(file_path)
    dip_data = {
        "origin_pipeline": "/api/v2/pipeline/" + pipeline_uuid + "/",
        "origin_location": "/api/v2/location/" + processing_uuid + "/",
        "origin_path": os.path.join(dip_path, dip) + "/",
        "current_location": "/api/v2/location/" + s3_uuid + "/",
        "current_path": dip,
        "package_type": "DIP",
        "aip_subtype": "Archival Information Package",  # same as in AM
        "size": size,
        "related_package_uuid": aip_uuid,
    }
    LOGGER.info("Storing DIP in S3 location.")
    url = ss_url + "/api/v2/file/"
    headers = {"Authorization": "ApiKey " + ss_user + ":" + ss_api_key + ""}
    response = requests.post(url, headers=headers, json=dip_data, timeout=86400)
    if response.status_code != requests.codes.created:
        LOGGER.error("Could not store DIP in S3 location: %s", response.text)
        return 2
    else:
        LOGGER.info("DIP stored in S3 location.")
        ret = response.json()
        if "uuid" in ret:
            dip_uuid = ret["uuid"]
            LOGGER.info("Storage Service DIP UUID: %s" % ret["uuid"])
        else:
            LOGGER.error("Storage Service didn't return the DIP UUID")
            return 2

    # USE AM Client to get info on the DIP
    LOGGER.info("Compiling DIP info.")
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
    # get bucket and region
    location_url = ss_url + dip_details["current_location"]
    headers = {"Authorization": "ApiKey " + ss_user + ":" + ss_api_key + ""}
    location_response = requests.get(location_url, headers=headers, timeout=86400)
    space_url = ss_url + location_response.json()['space']
    space_response = requests.get(space_url, headers=headers, timeout=86400)
    dip_info["dip-bucket"] = space_response.json()['bucket']
    dip_info["dip-region"] = space_response.json()['region']


    dip_info["object-list"] = object_list
    dip_info["thumbnail-list"] = thumbnail_list
    dip_info["aip-uuid"] = aip_uuid

    # get related AIP package info
    dip_info["aip-path"] = aip_details["current_full_path"]
    # get bucket and region
    location_url = ss_url + aip_details["current_location"]
    headers = {"Authorization": "ApiKey " + ss_user + ":" + ss_api_key + ""}
    location_response = requests.get(location_url, headers=headers, timeout=86400)
    space_url = ss_url + location_response.json()['space']
    space_response = requests.get(space_url, headers=headers, timeout=86400)
    dip_info["aip-bucket"] = space_response.json()['bucket']
    dip_info["aip-region"] = space_response.json()['region']
    # GET REPLICATED AIP PACKAGE INFO
    if aip_details["replicas"]:
        replica_uuid = os.path.basename(aip_details["replicas"][0][:-1])
        am_client = amclient.AMClient(
            package_uuid=replica_uuid,
            ss_url=ss_url,
            ss_user_name=ss_user,
            ss_api_key=ss_api_key,
        )
        replica_details = am_client.get_package_details()
        dip_info["replica-uuid"] = replica_uuid
        dip_info["replica-path"] = replica_details["current_full_path"]
        # get bucket and region
        location_url = ss_url + replica_details["current_location"]
        headers = {"Authorization": "ApiKey " + ss_user + ":" + ss_api_key + ""}
        location_response = requests.get(location_url, headers=headers, timeout=86400)
        space_url = ss_url + location_response.json()['space']
        space_response = requests.get(space_url, headers=headers, timeout=86400)
        dip_info["replica-bucket"] = space_response.json()['bucket']
        dip_info["replica-region"] = space_response.json()['region']
    else:
        dip_info["replica-bucket"] = ""
        dip_info["replica-region"] = ""


    # Return the data
    return dip_info, mets


def parse_mets(
    omeka_api,
    omeka_api_key_identity,
    omeka_api_key_credential,
    dip_info,
    mets,
    imgser_url,
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
    dcTitle = next(item for item in properties if item["o:term"] == ("dcterms:title"))
    # add to processing set if exist, else create
    sets = requests.get(
        omeka_api
        + "item_sets?property[0][property]="
        + str(dcTitle["o:id"])
        + "&property[0][type]=eq&property[0][text]="
        + urllib.quote("Processing"),
        params=params,
    ).json()
    processing_set_id = ""
    if sets is not None:
        for set in sets:
            if set["o:title"] == "Processing":
                processing_set_id = set["o:id"]
    if processing_set_id == "":
        set_json = {
            "o:is_public": 0,
            "dcterms:title": [
                {
                    "type": "literal",
                    "property_id": dcTitle["o:id"],
                    "@value": "Processing",
                }
            ],
        }
        set_response = requests.post(
            omeka_api + "item_sets", params=params, json=set_json,
        )
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
                type = next(
                    item
                    for item in types
                    if item["o:label"].lower() == element.text.lower()
                )
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
            elif (
                etree.QName(element).localname == "rights"
                and "rightsstatements.org" in element.text
            ):
                pq = PyQuery(element.text)
                literal = pq.text()
                uri = pq.attr("href")
                property = next(
                    item
                    for item in properties
                    if item["o:term"] == ("dcterms:" + etree.QName(element).localname)
                )
                appending_data = {
                    "type": "uri",
                    "@id": uri,
                    "o:label": literal,
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
            # only process specific custom elements
            if (
                etree.QName(customElement).localname == "fitdil_recordid"
                or etree.QName(customElement).localname == "fitdil_recordname"
                or etree.QName(customElement).localname == "photo_number"
            ):
                property = next(
                    item
                    for item in properties
                    if item["o:term"] == ("dcterms:identifier")
                )
                appending_data = {
                    "type": "uri",
                    "@id": customElement.text,
                    "o:label": etree.QName(customElement).localname.replace("_", "."),
                    "property_id": property["o:id"],
                    # set these identifiers as private as default
                    "is_public": 0,
                }
                if ("dcterms:identifier") in data:
                    data["dcterms:identifier"].append(appending_data)
                else:
                    data["dcterms:identifier"] = []
                    data["dcterms:identifier"].append(appending_data)
            elif (
                etree.QName(customElement).localname == "archiveondemand_collection"
                or etree.QName(customElement).localname == "sparcdigital_collection"
            ):
                this_set_id = ""
                # need to recheck sets api each time so new ones will show up
                sets = requests.get(
                    omeka_api
                    + "item_sets?property[0][property]="
                    + str(dcTitle["o:id"])
                    + "&property[0][type]=eq&property[0][text]="
                    + urllib.quote(customElement.text),
                    params=params,
                ).json()
                if sets is not None:
                    for set in sets:
                        if set["o:title"] == customElement.text:
                            this_set_id = set["o:id"]
                if this_set_id == "":
                    set_json = {
                        "dcterms:title": [
                            {
                                "type": "literal",
                                "property_id": dcTitle["o:id"],
                                "@value": customElement.text,
                            }
                        ],
                    }
                    set_response = requests.post(
                        omeka_api + "item_sets", params=params, json=set_json,
                    )
                    this_set_id = set_response.json()["o:id"]
                appending_data = {"o:id": this_set_id}
                data["o:item_set"].append(appending_data)

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

    replica_data = {
        "type": "uri",
        "@id": dip_info["replica-uuid"],
        "o:label": "replica-uuid",
        "property_id": property["o:id"],
        # set these identifiers as private as default
        "is_public": 0,
    }
    if ("dcterms:identifier") in data:
        data["dcterms:identifier"].append(replica_data)
    else:
        data["dcterms:identifier"] = []
        data["dcterms:identifier"].append(replica_data)

    # Create media data
    if type is not None:
        # Image type
        if type["o:label"] == "Still Image" or type["o:label"] == "Image":
            data["o:media"] = []
            media_index = 0
            for object in dip_info["object-list"]:
                # construct object urls
                # check if image file or not
                root, ext = os.path.splitext(object)
                if (
                    ext.lower() == ".jpg"
                    or ext.lower() == ".jpeg"
                    or ext.lower() == ".jp2"
                    or ext.lower() == ".j2k"
                    or ext.lower() == ".j2c"
                ):
                    data["o:media"].append({})
                    data["o:media"][media_index]["o:ingester"] = "remoteImage"
                    data["o:media"][media_index]["access"] = (
                        "https://"
                        + dip_info["dip-bucket"]
                        + ".s3."
                        + dip_info["dip-region"]
                        + ".amazonaws.com/"
                        + os.path.join(dip_info["dip-path"], object)
                    )
                    data["o:media"][media_index]["IIIF"] = (
                        imgser_url
                        + os.path.join(dip_info["dip-path"], object).replace("/", "%2F")
                        + "/info.json"
                    )
                    data["o:media"][media_index]["archival"] = (
                        "https://"
                        + dip_info["aip-bucket"]
                        + ".s3."
                        + dip_info["aip-region"]
                        + ".amazonaws.com/"
                        + dip_info["aip-path"]
                    )
                    # GET REPLICATED AIP Path
                    if dip_info["replica-bucket"]:
                        data["o:media"][media_index]["replica"] = (
                            "https://"
                            + dip_info["replica-bucket"]
                            + ".s3."
                            + dip_info["replica-region"]
                            + ".amazonaws.com/"
                            + dip_info["replica-path"]
                        )

                    # set media title and identifiers
                    name, _ = os.path.splitext(os.path.basename(object))
                    # get original info
                    original = next(
                        fsentry
                        for fsentry in mets.all_files()
                        if fsentry.file_uuid == name[:36]
                    )
                    property = next(
                        item
                        for item in properties
                        if item["o:term"] == ("dcterms:title")
                    )
                    data["o:media"][media_index]["dcterms:title"] = [
                        {
                            "property_id": property["o:id"],
                            "@value": name[37:],
                            "type": "literal",
                        }
                    ]
                    property = next(
                        item
                        for item in properties
                        if item["o:term"] == ("dcterms:identifier")
                    )
                    data["o:media"][media_index]["dcterms:identifier"] = [
                        {
                            "type": "uri",
                            "@id": name[37:],
                            "o:label": "Reference Code",
                            "property_id": property["o:id"],
                        },
                        {
                            "type": "uri",
                            "@id": name[:36],
                            "o:label": "file-uuid",
                            "property_id": property["o:id"],
                            # set these identifiers as private as default
                            "is_public": 0,
                        },
                        {
                            "type": "uri",
                            "@id": os.path.basename(object),
                            "o:label": "access-file",
                            "property_id": property["o:id"],
                            # set these identifiers as private as default
                            "is_public": 0,
                        },
                        {
                            "type": "uri",
                            "@id": os.path.basename(original.path),
                            "o:label": "archival-file",
                            "property_id": property["o:id"],
                            # set these identifiers as private as default
                            "is_public": 0,
                        },
                    ]
                    # get associated thumbnail
                    for thumbnail in dip_info["thumbnail-list"]:
                        thumb_name, _ = os.path.splitext(os.path.basename(thumbnail))
                        if name[:36] == thumb_name:
                            data["o:media"][media_index]["thumbnail"] = (
                                "https://"
                                + dip_info["dip-bucket"]
                                + ".s3."
                                + dip_info["dip-region"]
                                + ".amazonaws.com/"
                                + os.path.join(dip_info["dip-path"], thumbnail)
                            )
                            thumb_media = {
                                "type": "uri",
                                "@id": os.path.basename(thumbnail),
                                "o:label": "thumbnail-file",
                                "property_id": property["o:id"],
                                # set these identifiers as private as default
                                "is_public": 0,
                            }
                            data["o:media"][media_index]["dcterms:identifier"].append(
                                thumb_media
                            )

                    if "thumbnail" not in data["o:media"][0]:
                        LOGGER.warning("Not able to locate thumbnail for this item.")
                    media_index += 1

                else:
                    LOGGER.warning("DIP contains file that isn't an image.")

        # Video type
        elif type["o:label"] == "Moving Image":
            data["o:media"] = []
            media_index = 0
            video_object = ""
            captions_object = ""
            transcript_object = ""
            # identify video, captions, transcripts
            # set up to handle single video file
            for fsentry in mets.all_files():
                if fsentry.use == "original":
                    head, tail = os.path.split(fsentry.path)
                    if head == "objects":
                        original_video = fsentry
                        # get matching DIP object
                        video_object = next(
                            object
                            for object in dip_info["object-list"]
                            if original_video.file_uuid == os.path.basename(object)[:36]
                        )
                    elif head == "objects/captions":
                        original_captions = fsentry
                        # get matching DIP object
                        captions_object = next(
                            object
                            for object in dip_info["object-list"]
                            if original_captions.file_uuid
                            == os.path.basename(object)[:36]
                        )
                    elif head == "objects/transcript":
                        original_transcript = fsentry
                        # get matching DIP object
                        transcript_object = next(
                            object
                            for object in dip_info["object-list"]
                            if original_transcript.file_uuid
                            == os.path.basename(object)[:36]
                        )
                    else:
                        # dealing with all other file
                        LOGGER.error(
                            "Video has files that we haven't been set up to process."
                        )
                        return 2

            # process each file
            if video_object != "":
                data["o:media"].append({})
                data["o:media"][media_index]["o:ingester"] = "remoteVideo"
                data["o:media"][media_index]["access"] = (
                    "https://"
                    + dip_info["dip-bucket"]
                    + ".s3."
                    + dip_info["dip-region"]
                    + ".amazonaws.com/"
                    + os.path.join(dip_info["dip-path"], video_object)
                )
                data["o:media"][media_index]["archival"] = (
                    "https://"
                    + dip_info["aip-bucket"]
                    + ".s3."
                    + dip_info["aip-region"]
                    + ".amazonaws.com/"
                    + dip_info["aip-path"]
                )
                # GET REPLICATED AIP Path
                if dip_info["replica-bucket"]:
                    data["o:media"][media_index]["replica"] = (
                        "https://"
                        + dip_info["replica-bucket"]
                        + ".s3."
                        + dip_info["replica-region"]
                        + ".amazonaws.com/"
                        + dip_info["replica-path"]
                    )
                # attach captions file if it exists
                if captions_object != "":
                    data["o:media"][media_index]["captions"] = (
                        "https://"
                        + dip_info["dip-bucket"]
                        + ".s3."
                        + dip_info["dip-region"]
                        + ".amazonaws.com/"
                        + os.path.join(dip_info["dip-path"], captions_object)
                    )
                # set media title and identifiers
                name, _ = os.path.splitext(os.path.basename(video_object))
                property = next(
                    item for item in properties if item["o:term"] == ("dcterms:title")
                )
                data["o:media"][media_index]["dcterms:title"] = [
                    {
                        "property_id": property["o:id"],
                        "@value": name[37:],
                        "type": "literal",
                    }
                ]
                property = next(
                    item
                    for item in properties
                    if item["o:term"] == ("dcterms:identifier")
                )
                data["o:media"][media_index]["dcterms:identifier"] = [
                    {
                        "type": "uri",
                        "@id": name[37:],
                        "o:label": "Reference Code",
                        "property_id": property["o:id"],
                    },
                    {
                        "type": "uri",
                        "@id": name[:36],
                        "o:label": "file-uuid",
                        "property_id": property["o:id"],
                        # set these identifiers as private as default
                        "is_public": 0,
                    },
                    {
                        "type": "uri",
                        "@id": os.path.basename(video_object),
                        "o:label": "access-file",
                        "property_id": property["o:id"],
                        # set these identifiers as private as default
                        "is_public": 0,
                    },
                    {
                        "type": "uri",
                        "@id": os.path.basename(original_video.path),
                        "o:label": "archival-file",
                        "property_id": property["o:id"],
                        # set these identifiers as private as default
                        "is_public": 0,
                    },
                ]
                # process youtube/googledrive data
                if custom_xml is not None:
                    youtubeID = next(
                        customElement
                        for customElement in custom_xml
                        if etree.QName(customElement).localname
                        == ("youtube_identifier")
                    )
                    if youtubeID is not None:
                        data["o:media"][media_index]["YouTubeID"] = youtubeID.text
                    googledriveID = next(
                        customElement
                        for customElement in custom_xml
                        if etree.QName(customElement).localname
                        == ("googledrive_identifier")
                    )
                    if googledriveID is not None:
                        data["o:media"][media_index][
                            "GoogleDriveID"
                        ] = googledriveID.text
                # thumbnails not generated by archivematica
                media_index += 1

            if transcript_object != "":
                LOGGER.error(
                    "This script has not been setup to handle transcript files."
                )
                return 2

        # other files
        else:
            data["o:media"] = []
            media_index = 0
            for object in dip_info["object-list"]:
                # construct object urls
                # check if image file or not
                root, ext = os.path.splitext(object)
                data["o:media"].append({})
                data["o:media"][media_index]["o:ingester"] = "remoteFile"
                data["o:media"][media_index]["access"] = (
                    "https://"
                    + dip_info["dip-bucket"]
                    + ".s3."
                    + dip_info["dip-region"]
                    + ".amazonaws.com/"
                    + os.path.join(dip_info["dip-path"], object)
                )
                data["o:media"][media_index]["archival"] = (
                    "https://"
                    + dip_info["aip-bucket"]
                    + ".s3."
                    + dip_info["aip-region"]
                    + ".amazonaws.com/"
                    + dip_info["aip-path"]
                )
                # GET REPLICATED AIP Path
                if dip_info["replica-bucket"]:
                    data["o:media"][media_index]["replica"] = (
                        "https://"
                        + dip_info["replica-bucket"]
                        + ".s3."
                        + dip_info["replica-region"]
                        + ".amazonaws.com/"
                        + dip_info["replica-path"]
                    )
                # set media title and identifiers
                name, _ = os.path.splitext(os.path.basename(object))
                # get original info
                original = next(
                    fsentry
                    for fsentry in mets.all_files()
                    if fsentry.file_uuid == name[:36]
                )
                property = next(
                    item for item in properties if item["o:term"] == ("dcterms:title")
                )
                data["o:media"][media_index]["dcterms:title"] = [
                    {
                        "property_id": property["o:id"],
                        "@value": name[37:],
                        "type": "literal",
                    }
                ]
                property = next(
                    item
                    for item in properties
                    if item["o:term"] == ("dcterms:identifier")
                )
                data["o:media"][media_index]["dcterms:identifier"] = [
                    {
                        "type": "uri",
                        "@id": name[37:],
                        "o:label": "Reference Code",
                        "property_id": property["o:id"],
                    },
                    {
                        "type": "uri",
                        "@id": name[:36],
                        "o:label": "file-uuid",
                        "property_id": property["o:id"],
                        # set these identifiers as private as default
                        "is_public": 0,
                    },
                    {
                        "type": "uri",
                        "@id": os.path.basename(object),
                        "o:label": "access-file",
                        "property_id": property["o:id"],
                        # set these identifiers as private as default
                        "is_public": 0,
                    },
                    {
                        "type": "uri",
                        "@id": os.path.basename(original.path),
                        "o:label": "archival-file",
                        "property_id": property["o:id"],
                        # set these identifiers as private as default
                        "is_public": 0,
                    },
                ]
                # get associated thumbnail
                for thumbnail in dip_info["thumbnail-list"]:
                    thumb_name, _ = os.path.splitext(os.path.basename(thumbnail))
                    if name[:36] == thumb_name:
                        data["o:media"][media_index]["thumbnail"] = (
                            "https://"
                            + dip_info["dip-bucket"]
                            + ".s3."
                            + dip_info["dip-region"]
                            + ".amazonaws.com/"
                            + os.path.join(dip_info["dip-path"], thumbnail)
                        )
                        thumb_media = {
                            "type": "uri",
                            "@id": os.path.basename(thumbnail),
                            "o:label": "thumbnail-file",
                            "property_id": property["o:id"],
                            # set these identifiers as private as default
                            "is_public": 0,
                        }
                        data["o:media"][media_index]["dcterms:identifier"].append(
                            thumb_media
                        )

                if "thumbnail" not in data["o:media"][0]:
                    LOGGER.warning("Not able to locate thumbnail for this item.")
                media_index += 1

    return data


def deposit(omeka_api, omeka_api_key_identity, omeka_api_key_credential, data):
    # Deposits json data into Omeka-s
    LOGGER.info("Posting data to Omeka-S")
    params = {
        "key_identity": omeka_api_key_identity,
        "key_credential": omeka_api_key_credential,
    }
    response = requests.post(omeka_api + "items", params=params, json=data,)

    #
    LOGGER.debug("Response code: %s", response.status_code)
    LOGGER.debug("Response content:\n%s", response.content)

    # Check response status code
    if response.status_code not in [200, 201, 202, 302]:
        raise Exception("Response status code not expected", response.content)
    else:
        if ("@id") in response.json():
            LOGGER.info("Created new Omeka-S resource: %s", response.json()["@id"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--omeka-api", metavar="URL", required=True, help="Omeka-S API endpoint."
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
        "--ss-url", metavar="URL", required=True, help="Storage Service URL.",
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
        "--pipeline-uuid",
        metavar="UUID",
        required=True,
        help="UUID of the working Pipeline.",
    )
    parser.add_argument(
        "--processing-uuid",
        metavar="UUID",
        required=True,
        help="UUID of the processing directory.",
    )
    parser.add_argument(
        "--s3-uuid",
        metavar="UUID",
        required=True,
        help="UUID of the S3 location to upload the DIP.",
    )
    parser.add_argument(
        "--shared-directory",
        metavar="PATH",
        help="Absolute path to the pipeline's shared directory.",
        default="/var/archivematica/sharedDirectory/",
    )
    parser.add_argument(
        "--dip-path",
        metavar="PATH",
        help="Relative path to upload DIP directory",
        default="watchedDirectories/uploadDIP/",
    )
    parser.add_argument(
        "--imgser-url",
        metavar="URL",
        required=True,
        help="Image Server IIIF Endpoint, ie http://xxx.xxx.com:8182/iiif/2/",
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
            omeka_api=args.omeka_api,
            omeka_api_key_identity=args.omeka_api_key_identity,
            omeka_api_key_credential=args.omeka_api_key_credential,
            ss_url=args.ss_url,
            ss_user=args.ss_user,
            ss_api_key=args.ss_api_key,
            pipeline_uuid=args.pipeline_uuid,
            processing_uuid=args.processing_uuid,
            s3_uuid=args.s3_uuid,
            shared_directory=args.shared_directory,
            dip_path=args.dip_path,
            imgser_url=args.imgser_url,
        )
    )
