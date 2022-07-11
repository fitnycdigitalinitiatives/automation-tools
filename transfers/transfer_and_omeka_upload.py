#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Automate Transfers.

Helper script to automate running transfers through Archivematica.
"""

from __future__ import print_function, unicode_literals

import ast
import atexit
import base64
import logging
import os
import shutil
import subprocess
import sys
import time
import json
import metsrw
import base64
from lxml import etree
import urllib
import mimetypes
import boto3

from amclient import AMClient
import requests
from six.moves import configparser
from sqlalchemy.orm.exc import NoResultFound

# Allow execution as an executable and the script to be run at package level
# by ensuring that it can see itself.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transfers import defaults, errors, loggingconfig, models, utils
from transfers.transferargs import get_parser
from transfers.utils import fsencode, fsdecode

# Directory for various processing decisions, below.
THIS_DIR = os.path.abspath(os.path.dirname(__file__))

# Setup module level logging.
LOGGER = logging.getLogger("transfers")


def setup_automation_execution(pid_file):
    """Setup procedures for transfer.py."""
    atexit.register(manage_automation_execution, pid_file)


def manage_automation_execution(pid_file):
    """Cleanup procedures for transfer.py."""
    LOGGER.info("Running post-execution clean-up. Exiting script")
    os.remove(pid_file)
    models.cleanup_session()


def create_db_session(config_file):
    """Create and return a database session."""
    models.init_session(
        get_setting(config_file, "databasefile", os.path.join(THIS_DIR, "transfers.db"))
    )
    return models.Session()


def get_setting(config_file, setting, default=None):
    """Get an option value from the configuration file."""
    config = configparser.SafeConfigParser()
    section = "transfers"
    try:
        config.read(config_file)
        cfg = config.get(section, setting)
        LOGGER.info("Configuration values read for %s: %s", setting, cfg)
        return cfg
    except configparser.NoOptionError:
        LOGGER.warning("No option provided for %s", setting)
        return default
    except configparser.NoSectionError:
        LOGGER.warning("No section: %s in %s", section, config.sections())
        return default


def get_status(
    am_url,
    am_user,
    am_api_key,
    ss_url,
    ss_user,
    ss_api_key,
    unit_uuid,
    unit_type,
    ts_uuid,
    hide_on_complete=False,
    delete_on_complete=False,
):
    """
    Get status of the SIP or Transfer with unit_uuid.

    :param str unit_uuid: UUID of the unit to query for.
    :param str unit_type: 'ingest' or 'transfer'
    :param bool hide_on_complete: Hide the unit in the dashboard if COMPLETE
    :returns: Dict with status of the unit from Archivematica or None.
    """
    # Get status
    url = "{}/api/{}/status/{}/".format(am_url, unit_type, unit_uuid)
    params = {"username": am_user, "api_key": am_api_key}
    unit_info = utils._call_url_json(url, params)
    if isinstance(unit_info, int):
        if errors.error_lookup(unit_info) is not None:
            return errors.error_lookup(unit_info)
    # If complete, hide in dashboard
    if hide_on_complete and unit_info and unit_info.get("status") == "COMPLETE":
        LOGGER.info("Hiding %s %s in dashboard", unit_type, unit_uuid)
        url = "{}/api/{}/{}/delete/".format(am_url, unit_type, unit_uuid)
        LOGGER.debug("Method: DELETE; URL: %s; params: %s;", url, params)
        response = requests.delete(url, params=params)
        LOGGER.debug("Response: %s", response)
    # If Transfer is complete, get the SIP's status
    if (
        unit_info
        and unit_type == "transfer"
        and unit_info.get("status") == "COMPLETE"
        and unit_info.get("sip_uuid") != "BACKLOG"
    ):
        LOGGER.info(
            "%s is a complete transfer, fetching SIP %s status.",
            unit_uuid,
            unit_info.get("sip_uuid"),
        )
        # Update DB to refer to this one
        unit = models.retrieve_unit_by_type_and_uuid(
            uuid=unit_uuid, unit_type=unit_type
        )
        models.update_unit_type_and_uuid(
            unit=unit, unit_type="ingest", uuid=unit_info.get("sip_uuid")
        )
        # Get SIP status
        url = "{}/api/ingest/status/{}/".format(am_url, unit_info.get("sip_uuid"))
        unit_info = utils._call_url_json(url, params)
        if isinstance(unit_info, int):
            if errors.error_lookup(unit_info) is not None:
                return errors.error_lookup(unit_info)
        # If complete, hide in dashboard
        if hide_on_complete and unit_info and unit_info.get("status") == "COMPLETE":
            LOGGER.info("Hiding SIP %s in dashboard", unit.uuid)
            url = "{}/api/ingest/{}/delete/".format(am_url, unit.uuid)
            LOGGER.debug("Method: DELETE; URL: %s; params: %s;", url, params)
            response = requests.delete(url, params=params)
            LOGGER.debug("Response: %s", response)
        # If complete delete transfer source files
        if delete_on_complete and unit_info and unit_info.get("status") == "COMPLETE":
            delete_transfer(ss_url, ss_user, ss_api_key, unit.uuid, unit.path, ts_uuid)

    # If Ingest is complete, delete transfer source
    if (
        unit_info
        and unit_type == "ingest"
        and unit_info.get("status") == "COMPLETE"
        and delete_on_complete
    ):
        unit = models.retrieve_unit_by_type_and_uuid(
            uuid=unit_uuid, unit_type=unit_type
        )
        delete_transfer(ss_url, ss_user, ss_api_key, unit_uuid, unit.path, ts_uuid)

    return unit_info

def delete_transfer(ss_url, ss_user, ss_api_key, unit_uuid, unit_path, ts_uuid):
    am = AMClient(
        ss_url=ss_url,
        ss_user_name=ss_user,
        ss_api_key=ss_api_key,
        package_uuid=unit_uuid,
    )
    response = am.get_package_details()
    uploadStatus = response.get("status")
    #wait until the package is uploaded before deleting it
    while uploadStatus != "UPLOADED":
        LOGGER.info("Waiting for package to be uploaded before deleting it.")
        time.sleep(2) # Sleep for 2 second before checking again
        response = am.get_package_details()
        uploadStatus = response.get("status")
    LOGGER.info(
        "Deleting source files for SIP %s from watched " "directory: %s",
        unit_uuid,
        unit_path
    )
    try:
        # Get Transfer location absolute path
        url = "{}/api/v2/location/{}/".format(ss_url, ts_uuid)
        params = {"username": ss_user, "api_key": ss_api_key}
        transfer_info = utils._call_url_json(url, params)
        transfer_source_path = transfer_info.get("path")
        unit_abs_path = os.path.join(transfer_source_path, unit_path)
        shutil.rmtree(unit_abs_path)
        LOGGER.info("Source files deleted for SIP %s " "deleted", unit_abs_path)
    except OSError as e:
        LOGGER.warning(
            "Error deleting source files: %s. If "
            "running this module remotely the "
            "script might not have access to the "
            "transfer source",
            e,
        )

def get_accession_id(dirname):
    """
    Call get-accession-number and return literal_eval stdout as accession ID.

    get-accession-number should be in the same directory as transfer.py. Its
    only output to stdout should be the accession number surrounded by
    quotes.  Eg. "accession number"

    :param str dirname: Directory name of folder to become transfer
    :returns: accession number or None.
    """
    script_path = os.path.join(THIS_DIR, "get-accession-number")
    try:
        p = subprocess.Popen(
            [script_path, dirname],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as err:
        LOGGER.warning("Error: %s when trying to run %s", err, script_path)
        return None
    output, err = p.communicate()
    if p.returncode != 0:
        LOGGER.error(
            "Error running %s %s: RC: %s; stdout: %s; stderr: %s",
            script_path,
            dirname,
            p.returncode,
            output,
            err,
        )
        return None
    output = fsdecode(output)
    try:
        return ast.literal_eval(output)
    except (ValueError, SyntaxError) as err:
        LOGGER.warning(
            "Unable to parse output from %s. Output: %s. %s", script_path, output, err
        )
        return None


def run_pre_transfer_scripts(config_file, transfer_path, transfer_type):
    """Wrapper for the run_scripts function. Pre-transfer functions want to
    modify the transfer itself, therefore the transfer path sent by the
    calling function function should at least be a valid one. If run_scripts
    results in an OSError exception then the calling function should take
    responsibility for working with that.
    """
    if not os.path.exists(transfer_path):
        LOGGER.error("Invalid transfer path for the pre-transfer scripts to work with")
    else:
        run_scripts("pre-transfer", config_file, transfer_path, transfer_type)


def run_scripts(directory, config_file, *args):
    """
    Run all executable scripts in directory relative to this file.

    :param str directory: Dir in the same folder as this file to run scripts
    :param args: All other parameters will be passed to called scripts.
    :return: None
    """
    directory = os.path.join(THIS_DIR, directory)
    if not os.path.isdir(directory):
        LOGGER.warning("%s is not a directory. No scripts to run.", directory)
        return
    script_args = list(args)
    LOGGER.debug("script_args: %s", script_args)
    script_extensions = get_setting(config_file, "scriptextensions", "").split(":")
    LOGGER.debug("script_extensions: %s", script_extensions)
    for script in sorted(os.listdir(directory)):
        LOGGER.debug("Script: %s", script)
        script_path = os.path.realpath(os.path.join(directory, script))
        if not os.path.isfile(script_path):
            LOGGER.info("%s is not a file, skipping", script_path)
            continue
        if not os.access(script_path, os.X_OK):
            LOGGER.info("%s is not executable, skipping", script_path)
            continue
        script_name, script_ext = os.path.splitext(script)
        if script_extensions and script_ext not in script_extensions:
            LOGGER.info(
                "'%s' for '%s' not in configured list of script file "
                "extensions, skipping",
                script_ext,
                script_path,
            )
            continue
        LOGGER.info('Running %s "%s"', script_path, '" "'.join(args))
        p = subprocess.Popen(
            [script_path] + script_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = p.communicate()
        LOGGER.info("Return code: %s", p.returncode)
        LOGGER.info("stdout: %s", stdout)
        if stderr:
            LOGGER.warning("stderr: %s", stderr)


def get_next_transfer(
    ss_url,
    ss_user,
    ss_api_key,
    ts_location_uuid,
    path_prefix,
    depth,
    processed,
    see_files,
):
    """
    Helper to find the first directory that doesn't have an associated
    transfer.

    :param ss_url:           URL of the Storage Service to query
    :param ss_user:          User on the Storage Service for authentication
    :param ss_api_key:       API key for user on the Storage Service for
                             authentication
    :param ts_location_uuid: UUID of the transfer source Location
    :param path_prefix:      Relative path inside the Location to work with.
    :param depth:            Depth relative to path_prefix to create a transfer
                             from. Should be 1 or greater.
    :param set processed:    Set of the paths of processed by the automation
                             tools in the database. Ideally, relative to the
                             same transfer source location, including the same
                             path_prefix, and at the same depth. Paths include
                             those currently processing and completed.
    :param bool see_files:   Return files as well as folders to become
                             transfers.
    :returns:                Path relative to TS Location of the new transfer.
    """
    # Get sorted list from source directory.
    url = ss_url + "/api/v2/location/" + ts_location_uuid + "/browse/"
    params = {"username": ss_user, "api_key": ss_api_key}
    if path_prefix:
        params["path"] = base64.b64encode(path_prefix)
    browse_info = utils._call_url_json(url, params)
    if isinstance(browse_info, int):
        if errors.error_lookup(browse_info) is not None:
            LOGGER.error(
                "Error when browsing location: %s", errors.error_lookup(browse_info)
            )
            return None
    if browse_info is None:
        return None
    if see_files:
        entries = browse_info["entries"]
    else:
        entries = browse_info["directories"]
    entries = [base64.b64decode(e.encode("utf8")) for e in entries]
    LOGGER.debug("Entries: %s", entries)
    LOGGER.info("Total files or folders in transfer source location: %s", len(entries))
    entries = [os.path.join(path_prefix, e) for e in entries]
    # If at the correct depth, check if any of these have not been made into
    # transfers yet
    if depth <= 1:
        # Find the directories that are not already in the DB using sets
        entries = set(entries) - processed
        LOGGER.debug("New transfer candidates: %s", entries)
        LOGGER.info("Unprocessed entries to choose from: %s", len(entries))
        # Sort, take the first
        entries = sorted(list(entries))
        if not entries:
            LOGGER.info("All potential transfers in %s have been created.", path_prefix)
            return None
        target = entries[0]
        return target
    else:  # if depth > 1
        # Recurse on each directory
        for entry in entries:
            LOGGER.debug("New path: %s", entry)
            target = get_next_transfer(
                ss_url=ss_url,
                ss_user=ss_user,
                ss_api_key=ss_api_key,
                ts_location_uuid=ts_location_uuid,
                path_prefix=entry,
                depth=depth - 1,
                processed=processed,
                see_files=see_files,
            )
            if target:
                return target
    return None


def call_start_transfer_endpoint(
    am_url, am_user, am_api_key, target, transfer_type, accession, ts_location_uuid
):
    """Make the call to the start_transfer endpoint and return the unapproved
    directory name, and current (absolute path), of the transfer as a tuple.
    """
    url = "{}/api/transfer/start_transfer/".format(am_url)
    params = {"username": am_user, "api_key": am_api_key}
    target_name = os.path.basename(target)
    data = {
        "name": target_name,
        "type": transfer_type,
        "accession": accession,
        "paths[]": [base64.b64encode(fsencode(ts_location_uuid) + b":" + target)],
        "row_ids[]": [""],
    }
    LOGGER.debug("URL: %s; Params: %s; Data: %s", url, params, data)
    response = requests.post(url, params=params, data=data)
    LOGGER.debug("Response: %s", response)
    try:
        resp_json = response.json()
        # Retrieve transfer_name, and the absolute path to the transfer for the
        # calling function.
        transfer_abs_path = resp_json.get("path")
        return os.path.basename(transfer_abs_path.strip(os.sep)), transfer_abs_path
    except ValueError:
        LOGGER.error(
            "Could not parse JSON from response Response: %s: %s, %s",
            response.status_code,
            response.reason,
            response.headers,
        )
        # Debug log, rather than Warning as the response from the server is
        # likely to be HTML and likely to be too verbose to be useful.
        LOGGER.debug("Could not parse JSON from response: %s", response.text)
        return None, None
    if not response.ok or resp_json.get("error"):
        LOGGER.error("Unable to start transfer.")
        LOGGER.error("Response: %s", resp_json)
        return None, None


def start_transfer(
    ss_url,
    ss_user,
    ss_api_key,
    ts_location_uuid,
    ts_path,
    depth,
    am_url,
    am_user,
    am_api_key,
    transfer_type,
    see_files,
    config_file,
):
    """
    Starts a new transfer.

    :param ss_url: URL of the Storage Service to query
    :param ss_user: User on the Storage Service for authentication
    :param ss_api_key: API key for user on the Storage Service for
                       authentication
    :param ts_location_uuid: UUID of the transfer source Location
    :param ts_path: Relative path inside the Location to work with.
    :param depth: Depth relative to ts_path to create a transfer from. Should
                  be 1 or greater.
    :param am_url: URL of Archivematica pipeline to start transfer on
    :param am_user: User on Archivematica for authentication
    :param am_api_key: API key for user on Archivematica for authentication
    :param bool see_files: If true, start transfers from files as well as
                           directories
    :param session: SQLAlchemy session with the DB
    :returns: Tuple of Transfer information about the new transfer or None on
              error.
    """
    # Retrieve the next transfer to process.
    processed = models.get_processed_transfer_paths()
    target = get_next_transfer(
        ss_url=ss_url,
        ss_user=ss_user,
        ss_api_key=ss_api_key,
        ts_location_uuid=ts_location_uuid,
        path_prefix=ts_path,
        depth=depth,
        processed=processed,
        see_files=see_files,
    )
    if not target:
        # Report the location UUID.
        LOGGER.info(
            "All potential transfers in Location ID: %s have been created. " "Exiting",
            ts_location_uuid,
        )
        return None
    LOGGER.info("Starting with %s", target)
    # Get accession ID
    accession = get_accession_id(target)
    LOGGER.info("Accession ID: %s", accession)
    # Call the start transfer endpoint.
    # Retrieve the directory name for Archivematica.
    transfer_name, transfer_abs_path = call_start_transfer_endpoint(
        am_url=am_url,
        am_user=am_user,
        am_api_key=am_api_key,
        target=target,
        transfer_type=transfer_type,
        accession=accession,
        ts_location_uuid=ts_location_uuid,
    )
    if not transfer_name:
        LOGGER.info("Cannot begin transfer with target name: %s", target)
        models.transfer_failed_to_start(target)
        return None
    # Run all pre-transfer scripts on the unapproved transfer directory.
    LOGGER.info("Attempting to run pre-transfer scripts on: %s", transfer_name)
    try:
        run_pre_transfer_scripts(
            config_file=config_file,
            transfer_path=transfer_abs_path,
            transfer_type=transfer_type,
        )
    except OSError as err:
        LOGGER.error("Failed to run pre-transfer scripts: %s", err)
        return None
    # Approve transfer.
    LOGGER.info("Ready to approve transfer")
    retry_count = 3
    for i in range(retry_count):
        result = approve_transfer(transfer_name, am_url, am_api_key, am_user)
        # Mark as started
        if result:
            LOGGER.info("Approved %s", result)
            # Store the absolute path to help users to determine what type
            # the transfer is, and where something it is.
            new_transfer = models.add_new_transfer(uuid=result, path=target)
            LOGGER.info("New transfer: %s", new_transfer)
            break
        LOGGER.info("Failed transfer approval, try %s of %s", i + 1, retry_count)
    else:
        new_transfer = models.failed_to_approve(path=target)
        LOGGER.warning("Transfer not approved: %s", transfer_name)
        return None
    # Start transfer completed successfully.
    LOGGER.info("Finished %s", target)
    return new_transfer


def approve_transfer(dirname, url, am_api_key, am_user):
    """
    Approve transfer with dirname.

    :returns: UUID of the approved transfer or None.
    """
    LOGGER.info("Approving %s", dirname)
    time.sleep(6)
    am = AMClient(am_url=url, am_user_name=am_user, am_api_key=am_api_key)
    try:
        # Find the waiting transfers available to be approved via the am client
        # interface.
        waiting_transfers = am.unapproved_transfers()["results"]
    except (KeyError, TypeError):
        LOGGER.error(
            "Request to unapproved transfers did not return the "
            "expected response, see the request log"
        )
        return None
    if not waiting_transfers:
        LOGGER.warning("There are no waiting transfers.")
        return None
    res = list(
        filter(
            lambda waiting: fsencode(waiting["directory"]) == fsencode(dirname),
            waiting_transfers,
        )
    )
    if not res:
        LOGGER.warning(
            "Requested directory %s not found in the waiting " "transfers list", dirname
        )
        return None
    LOGGER.info("Found waiting transfer: %s", res[0]["directory"])
    # We can reuse the existing AM Client but we didn't know all the kwargs
    # at the outset so we need to set its attributes here.
    am.transfer_type = res[0]["type"]
    am.transfer_directory = dirname
    # Approve the transfer and return the UUID of the transfer approved.
    approved = am.approve_transfer()
    if isinstance(approved, int):
        if errors.error_lookup(approved) is not None:
            LOGGER.error("Error approving transfer: %s", errors.error_lookup(approved))
            return None
    # Get will return None, or the UUID.
    return approved.get("uuid")


def process_dip(
    ss_url,
    ss_user,
    ss_api_key,
    dip,
    pipeline_uuid,
    processing_uuid,
    s3_dip_uuid,
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
    am_client = AMClient(
        package_uuid=aip_uuid,
        ss_url=ss_url,
        ss_user_name=ss_user,
        ss_api_key=ss_api_key,
    )
    try:
        aip_details = am_client.get_package_details()
    except Exception as e:
        LOGGER.error("Unable to locate valid AIP package: %s", e)
        return None

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
        return None

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
        return None

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
        "current_location": "/api/v2/location/" + s3_dip_uuid + "/",
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
        return None
    else:
        LOGGER.info("DIP stored in S3 location.")
        ret = response.json()
        if "uuid" in ret:
            dip_uuid = ret["uuid"]
            LOGGER.info("Storage Service DIP UUID: %s" % ret["uuid"])
        else:
            LOGGER.error("Storage Service didn't return the DIP UUID")
            return None

    # USE AM Client to get info on the DIP
    LOGGER.info("Compiling DIP info.")
    am_client = AMClient(
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
    dip_info["dip-bucket"] = space_response.json().get('bucket', "")
    dip_info["dip-region"] = space_response.json().get('region', "")


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
    dip_info["aip-bucket"] = space_response.json().get('bucket', "")
    dip_info["aip-region"] = space_response.json().get('region', "")
    # GET REPLICATED AIP PACKAGE INFO
    if aip_details["replicas"]:
        replica_uuid = os.path.basename(aip_details["replicas"][0][:-1])
        am_client = AMClient(
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
        dip_info["replica-bucket"] = space_response.json().get('bucket', "")
        dip_info["replica-region"] = space_response.json().get('region', "")
    else:
        dip_info["replica-uuid"] = ""
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
    bibFrameRole = requests.get(omeka_api + "properties?term=bf:role", params=params).json()
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
            if element.text is not None:
                if etree.QName(element).localname == "identifier":
                    property = next(
                        item
                        for item in properties
                        if item["o:term"] == ("dcterms:" + etree.QName(element).localname)
                    )
                    if ":" in element.text:
                        label = element.text.split(":", 1)[0].strip()
                        uri = element.text.split(":", 1)[1].strip()
                        appending_data = {
                            "type": "uri",
                            "o:label": label,
                            "@id": uri,
                            "property_id": property["o:id"],
                        }
                    else:
                        appending_data = {
                            "type": "uri",
                            "@id": element.text,
                            "property_id": property["o:id"],
                        }

                elif etree.QName(element).localname == "type":
                    # use the first type to set the resource class
                    if "o:resource_class" not in data:
                        type = next(
                            (item
                            for item in types
                            if item["o:label"].lower() == element.text.lower()),
                            None
                        )
                        if type is not None:
                            data["o:resource_class"] = {"o:id": type["o:id"]}

                    property = next(
                        item
                        for item in properties
                        if item["o:term"] == ("dcterms:" + etree.QName(element).localname)
                    )
                    if "{" in element.text:
                        label = element.text.split("{")[0].strip()
                        uri = element.text.split("{")[1].split("}")[0].strip()
                        appending_data = {
                            "type": "uri",
                            "o:label": label,
                            "@id": uri,
                            "property_id": property["o:id"],
                        }
                    else:
                        appending_data = {
                            "type": "literal",
                            "@value": element.text,
                            "property_id": property["o:id"],
                        }
                elif etree.QName(element).localname == "contributor":
                    property = next(
                        item
                        for item in properties
                        if item["o:term"] == ("dcterms:" + etree.QName(element).localname)
                    )
                    if "{" in element.text:
                        label = element.text.split("{")[0].strip()
                        uri = element.text.split("{")[1].split("}")[0].strip()
                        appending_data = {
                            "type": "uri",
                            "o:label": label,
                            "@id": uri,
                            "property_id": property["o:id"],
                        }
                        if "[" in label:
                            name = label.split("[")[0].strip()
                            relators = label.split("[")[1].split("]")[0].strip()
                            appending_data["o:label"] = name
                            appending_data["@annotation"] = {"bf:role": []}
                            for relator in relators.split(","):
                                relator_json = {
                                    "property_id": bibFrameRole[0]["o:id"]
                                }
                                if relatorLookup(relator.strip()):
                                    relator_json["type"] = "uri"
                                    relator_json["o:label"] = relator.strip()
                                    relator_json["@id"] = relatorLookup(relator.strip())
                                else:
                                    relator_json["type"] = "literal"
                                    relator_json["@value"] = relator.strip()
                                appending_data["@annotation"]["bf:role"].append(relator_json)
                    else:
                        if "[" in element.text:
                            name = element.text.split("[")[0].strip()
                            relators = element.text.split("[")[1].split("]")[0].strip()
                            appending_data = {
                                "type": "literal",
                                "@value": name,
                                "property_id": property["o:id"],
                                "@annotation": {"bf:role": []}
                            }
                            for relator in relators.split(","):
                                relator_json = {
                                    "property_id": bibFrameRole[0]["o:id"]
                                }
                                if relatorLookup(relator.strip()):
                                    relator_json["type"] = "uri"
                                    relator_json["o:label"] = relator.strip()
                                    relator_json["@id"] = relatorLookup(relator.strip())
                                else:
                                    relator_json["type"] = "literal"
                                    relator_json["@value"] = relator.strip()
                                appending_data["@annotation"]["bf:role"].append(relator_json)
                        else:
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
                    if "{" in element.text:
                        label = element.text.split("{")[0].strip()
                        uri = element.text.split("{")[1].split("}")[0].strip()
                        appending_data = {
                            "type": "uri",
                            "o:label": label,
                            "@id": uri,
                            "property_id": property["o:id"],
                        }
                    else:
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
            if customElement.text is not None:
                # only process specific custom elements
                if etree.QName(customElement).localname == "omeka_itemset":
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
                #process custom fitcore metadata
                elif "fitcore" in etree.QName(customElement).localname:
                    term = etree.QName(customElement).localname.replace("fitcore_", "fitcore:")
                    property_search = requests.get(omeka_api + "properties?term=" + term, params=params).json()
                    if property_search:
                        property = property_search[0]
                        if "{" in customElement.text:
                            label = customElement.text.split("{")[0].strip()
                            uri = customElement.text.split("{")[1].split("}")[0].strip()
                            appending_data = {
                                "type": "uri",
                                "o:label": label,
                                "@id": uri,
                                "property_id": property["o:id"],
                            }
                        else:
                            appending_data = {
                                "type": "literal",
                                "@value": customElement.text,
                                "property_id": property["o:id"],
                            }
                        if (term) in data:
                            data[term].append(appending_data)
                        else:
                            data[term] = []
                            data[term].append(appending_data)

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

    if dip_info["replica-uuid"]:
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
    data["o:media"] = []
    media_index = 0
    # set up aws connnection
    s3 = boto3.resource('s3')
    for object in dip_info["object-list"]:
        # construct object urls
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
        # attached METS file to each media
        mets_name = "METS." + dip_info["aip-uuid"] + ".xml"
        data["o:media"][media_index]["mets"] = (
            "https://"
            + dip_info["dip-bucket"]
            + ".s3."
            + dip_info["dip-region"]
            + ".amazonaws.com/"
            + os.path.join(dip_info["dip-path"], mets_name)
        )
        # set media metadata
        name, _ = os.path.splitext(os.path.basename(object))
        # get original info
        original = mets.get_file(file_uuid=name[:36])

        file_dc_xml = None
        file_custom_xml = None
        for dmdsec in original.dmdsecs:
            if dmdsec.contents.mdtype == "DC":
                file_dc_xml = dmdsec.contents.document
            elif dmdsec.contents.mdtype == "OTHER":
                file_custom_xml = dmdsec.contents.serialize()

        #check for dublin core metadata and add it
        if file_dc_xml is not None:
            for element in file_dc_xml:
                if element.text is not None:
                    property = next(
                        item
                        for item in properties
                        if item["o:term"] == ("dcterms:" + etree.QName(element).localname)
                    )
                    if "{" in element.text:
                        label = element.text.split("{")[0].strip()
                        uri = element.text.split("{")[1].split("}")[0].strip()
                        appending_data = {
                            "type": "uri",
                            "o:label": label,
                            "@id": uri,
                            "property_id": property["o:id"],
                        }
                    else:
                        appending_data = {
                            "type": "literal",
                            "@value": element.text,
                            "property_id": property["o:id"],
                        }

                    if ("dcterms:" + etree.QName(element).localname) in data["o:media"][media_index]:
                        data["o:media"][media_index]["dcterms:" + etree.QName(element).localname].append(appending_data)
                    else:
                        data["o:media"][media_index]["dcterms:" + etree.QName(element).localname] = []
                        data["o:media"][media_index]["dcterms:" + etree.QName(element).localname].append(appending_data)

        #if no title, use identifier as default
        if "dcterms:title" not in data["o:media"][media_index]:
            property = next(
                item for item in properties if item["o:term"] == ("dcterms:title")
            )
            data["o:media"][media_index]["dcterms:title"] = [
                {
                    "property_id": property["o:id"],
                    "@value": os.path.basename(original.path),
                    "type": "literal",
                }
            ]

        # Add default identifiers
        property = next(
            item
            for item in properties
            if item["o:term"] == ("dcterms:identifier")
        )
        default_identifiers = [
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
                "o:label": "original-file",
                "property_id": property["o:id"],
            },
        ]
        if "dcterms:identifier" in data["o:media"][media_index]:
            data["o:media"][media_index]["dcterms:identifier"] = data["o:media"][media_index]["dcterms:identifier"] + default_identifiers
        else:
            data["o:media"][media_index]["dcterms:identifier"] = default_identifiers

        #check for custom file elements, positioning, dimensions (for images) and video file data. Set position to blank as default
        data["o:media"][media_index]["position"] = ""
        if file_custom_xml is not None:
            order = file_custom_xml.find(".//{*}order")
            if order is not None and order.text:
                data["o:media"][media_index]["position"] = order.text

            #get mimetype to check if it's an image
            mime, encoding = mimetypes.guess_type(object)
            if mime is not None:
                if mime.startswith("image"):
                    width = file_custom_xml.find(".//{*}exif_width")
                    height = file_custom_xml.find(".//{*}exif_height")
                    if (width is not None) and (width.text) and (width.text.isdigit()) and (height is not None) and (height.text) and (height.text.isdigit()):
                        #set width/height in s3 metadata
                        key = os.path.join(dip_info["dip-path"], object)
                        s3_object = s3.Object(dip_info["dip-bucket"], key)
                        s3_object.metadata.update({'width':width.text, 'height':height.text})
                        s3_object.copy_from(CopySource={'Bucket':dip_info["dip-bucket"], 'Key':key}, Metadata=s3_object.metadata, ContentType=mime, MetadataDirective='REPLACE')
                        #set width/height in file metadata
                        width_property_search = requests.get(omeka_api + "properties?term=exif:width", params=params).json()
                        height_property_search = requests.get(omeka_api + "properties?term=exif:height", params=params).json()
                        if width_property_search and height_property_search:
                            width_property = width_property_search[0]
                            height_property = height_property_search[0]
                            data["o:media"][media_index]["exif:width"] = [
                                {
                                    "property_id": width_property["o:id"],
                                    "@value": width.text,
                                    "type": "literal",
                                }
                            ]
                            data["o:media"][media_index]["exif:height"] = [
                                {
                                    "property_id": height_property["o:id"],
                                    "@value": height.text,
                                    "type": "literal",
                                }
                            ]

            youtubeID = file_custom_xml.find(".//{*}youtube_identifier")
            if youtubeID is not None and youtubeID.text:
                data["o:media"][media_index]["YouTubeID"] = youtubeID.text

            googledriveID = file_custom_xml.find(".//{*}googledrive_identifier")
            if googledriveID is not None and googledriveID.text:
                data["o:media"][media_index]["GoogleDriveID"] = googledriveID.text

        #set thumbnail to blank
        data["o:media"][media_index]["thumbnail"] = ""
        # get associated thumbnail if available
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
                # use boto3 to set s3 cache-control and content type for thumbnails
                key = os.path.join(dip_info["dip-path"], thumbnail)
                cache_control = 'public, max-age=31536000, immutable'
                content_type = 'image/jpeg'
                s3.Object(dip_info["dip-bucket"], key).copy_from(
                    CopySource={'Bucket': dip_info["dip-bucket"], 'Key': key},
                    CacheControl=cache_control,
                    ContentType=content_type,
                    MetadataDirective='REPLACE',
                )

        media_index += 1

    #sort media data so it reflect position
    sorted_media = sorted(data["o:media"], key = lambda i: i['position'])
    data["o:media"] = sorted_media

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
        else:
            LOGGER.debug("API DID NOT RETURN AN ID.")

def relatorLookup(relator_label):
    relatorsData = {
      "Abridger": "http://id.loc.gov/vocabulary/relators/abr",
      "Actor": "http://id.loc.gov/vocabulary/relators/act",
      "Adapter": "http://id.loc.gov/vocabulary/relators/adp",
      "Addressee": "http://id.loc.gov/vocabulary/relators/rcp",
      "Analyst": "http://id.loc.gov/vocabulary/relators/anl",
      "Animator": "http://id.loc.gov/vocabulary/relators/anm",
      "Annotator": "http://id.loc.gov/vocabulary/relators/ann",
      "Appellant": "http://id.loc.gov/vocabulary/relators/apl",
      "Appellee": "http://id.loc.gov/vocabulary/relators/ape",
      "Applicant": "http://id.loc.gov/vocabulary/relators/app",
      "Architect": "http://id.loc.gov/vocabulary/relators/arc",
      "Arranger": "http://id.loc.gov/vocabulary/relators/arr",
      "Art copyist": "http://id.loc.gov/vocabulary/relators/acp",
      "Art director": "http://id.loc.gov/vocabulary/relators/adi",
      "Artist": "http://id.loc.gov/vocabulary/relators/art",
      "Artistic director": "http://id.loc.gov/vocabulary/relators/ard",
      "Assignee": "http://id.loc.gov/vocabulary/relators/asg",
      "Associated name": "http://id.loc.gov/vocabulary/relators/asn",
      "Attributed name": "http://id.loc.gov/vocabulary/relators/att",
      "Auctioneer": "http://id.loc.gov/vocabulary/relators/auc",
      "Author": "http://id.loc.gov/vocabulary/relators/aut",
      "Author in quotations or text abstracts": "http://id.loc.gov/vocabulary/relators/aqt",
      "Author of afterword, colophon, etc.": "http://id.loc.gov/vocabulary/relators/aft",
      "Author of dialog": "http://id.loc.gov/vocabulary/relators/aud",
      "Author of introduction, etc.": "http://id.loc.gov/vocabulary/relators/aui",
      "Autographer": "http://id.loc.gov/vocabulary/relators/ato",
      "Bibliographic antecedent": "http://id.loc.gov/vocabulary/relators/ant",
      "Binder": "http://id.loc.gov/vocabulary/relators/bnd",
      "Binding designer": "http://id.loc.gov/vocabulary/relators/bdd",
      "Blurb writer": "http://id.loc.gov/vocabulary/relators/blw",
      "Book designer": "http://id.loc.gov/vocabulary/relators/bkd",
      "Book producer": "http://id.loc.gov/vocabulary/relators/bkp",
      "Bookjacket designer": "http://id.loc.gov/vocabulary/relators/bjd",
      "Bookplate designer": "http://id.loc.gov/vocabulary/relators/bpd",
      "Bookseller": "http://id.loc.gov/vocabulary/relators/bsl",
      "Braille embosser": "http://id.loc.gov/vocabulary/relators/brl",
      "Broadcaster": "http://id.loc.gov/vocabulary/relators/brd",
      "Calligrapher": "http://id.loc.gov/vocabulary/relators/cll",
      "Cartographer": "http://id.loc.gov/vocabulary/relators/ctg",
      "Caster": "http://id.loc.gov/vocabulary/relators/cas",
      "Censor": "http://id.loc.gov/vocabulary/relators/cns",
      "Choreographer": "http://id.loc.gov/vocabulary/relators/chr",
      "Cinematographer": "http://id.loc.gov/vocabulary/relators/cng",
      "Client": "http://id.loc.gov/vocabulary/relators/cli",
      "Collection registrar": "http://id.loc.gov/vocabulary/relators/cor",
      "Collector": "http://id.loc.gov/vocabulary/relators/col",
      "Collotyper": "http://id.loc.gov/vocabulary/relators/clt",
      "Colorist": "http://id.loc.gov/vocabulary/relators/clr",
      "Commentator": "http://id.loc.gov/vocabulary/relators/cmm",
      "Commentator for written text": "http://id.loc.gov/vocabulary/relators/cwt",
      "Compiler": "http://id.loc.gov/vocabulary/relators/com",
      "Complainant": "http://id.loc.gov/vocabulary/relators/cpl",
      "Complainant-appellant": "http://id.loc.gov/vocabulary/relators/cpt",
      "Complainant-appellee": "http://id.loc.gov/vocabulary/relators/cpe",
      "Composer": "http://id.loc.gov/vocabulary/relators/cmp",
      "Compositor": "http://id.loc.gov/vocabulary/relators/cmt",
      "Conceptor": "http://id.loc.gov/vocabulary/relators/ccp",
      "Conductor": "http://id.loc.gov/vocabulary/relators/cnd",
      "Conservator": "http://id.loc.gov/vocabulary/relators/con",
      "Consultant": "http://id.loc.gov/vocabulary/relators/csl",
      "Consultant to a project": "http://id.loc.gov/vocabulary/relators/csp",
      "Contestant": "http://id.loc.gov/vocabulary/relators/cos",
      "Contestant-appellant": "http://id.loc.gov/vocabulary/relators/cot",
      "Contestant-appellee": "http://id.loc.gov/vocabulary/relators/coe",
      "Contestee": "http://id.loc.gov/vocabulary/relators/cts",
      "Contestee-appellant": "http://id.loc.gov/vocabulary/relators/ctt",
      "Contestee-appellee": "http://id.loc.gov/vocabulary/relators/cte",
      "Contractor": "http://id.loc.gov/vocabulary/relators/ctr",
      "Contributor": "http://id.loc.gov/vocabulary/relators/ctb",
      "Copyright claimant": "http://id.loc.gov/vocabulary/relators/cpc",
      "Copyright holder": "http://id.loc.gov/vocabulary/relators/cph",
      "Corrector": "http://id.loc.gov/vocabulary/relators/crr",
      "Correspondent": "http://id.loc.gov/vocabulary/relators/crp",
      "Costume designer": "http://id.loc.gov/vocabulary/relators/cst",
      "Court governed": "http://id.loc.gov/vocabulary/relators/cou",
      "Court reporter": "http://id.loc.gov/vocabulary/relators/crt",
      "Cover designer": "http://id.loc.gov/vocabulary/relators/cov",
      "Creator": "http://id.loc.gov/vocabulary/relators/cre",
      "Curator": "http://id.loc.gov/vocabulary/relators/cur",
      "Dancer": "http://id.loc.gov/vocabulary/relators/dnc",
      "Data contributor": "http://id.loc.gov/vocabulary/relators/dtc",
      "Data manager": "http://id.loc.gov/vocabulary/relators/dtm",
      "Dedicatee": "http://id.loc.gov/vocabulary/relators/dte",
      "Dedicator": "http://id.loc.gov/vocabulary/relators/dto",
      "Defendant": "http://id.loc.gov/vocabulary/relators/dfd",
      "Defendant-appellant": "http://id.loc.gov/vocabulary/relators/dft",
      "Defendant-appellee": "http://id.loc.gov/vocabulary/relators/dfe",
      "Degree granting institution": "http://id.loc.gov/vocabulary/relators/dgg",
      "Degree supervisor": "http://id.loc.gov/vocabulary/relators/dgs",
      "Delineator": "http://id.loc.gov/vocabulary/relators/dln",
      "Depicted": "http://id.loc.gov/vocabulary/relators/dpc",
      "Depositor": "http://id.loc.gov/vocabulary/relators/dpt",
      "Designer": "http://id.loc.gov/vocabulary/relators/dsr",
      "Director": "http://id.loc.gov/vocabulary/relators/drt",
      "Dissertant": "http://id.loc.gov/vocabulary/relators/dis",
      "Distribution place": "http://id.loc.gov/vocabulary/relators/dbp",
      "Distributor": "http://id.loc.gov/vocabulary/relators/dst",
      "Donor": "http://id.loc.gov/vocabulary/relators/dnr",
      "Draftsman": "http://id.loc.gov/vocabulary/relators/drm",
      "Dubious author": "http://id.loc.gov/vocabulary/relators/dub",
      "Editor": "http://id.loc.gov/vocabulary/relators/edt",
      "Editor of compilation": "http://id.loc.gov/vocabulary/relators/edc",
      "Editor of moving image work": "http://id.loc.gov/vocabulary/relators/edm",
      "Electrician": "http://id.loc.gov/vocabulary/relators/elg",
      "Electrotyper": "http://id.loc.gov/vocabulary/relators/elt",
      "Enacting jurisdiction": "http://id.loc.gov/vocabulary/relators/enj",
      "Engineer": "http://id.loc.gov/vocabulary/relators/eng",
      "Engraver": "http://id.loc.gov/vocabulary/relators/egr",
      "Etcher": "http://id.loc.gov/vocabulary/relators/etr",
      "Event place": "http://id.loc.gov/vocabulary/relators/evp",
      "Expert": "http://id.loc.gov/vocabulary/relators/exp",
      "Facsimilist": "http://id.loc.gov/vocabulary/relators/fac",
      "Field director": "http://id.loc.gov/vocabulary/relators/fld",
      "Film director": "http://id.loc.gov/vocabulary/relators/fmd",
      "Film distributor": "http://id.loc.gov/vocabulary/relators/fds",
      "Film editor": "http://id.loc.gov/vocabulary/relators/flm",
      "Film producer": "http://id.loc.gov/vocabulary/relators/fmp",
      "Filmmaker": "http://id.loc.gov/vocabulary/relators/fmk",
      "First party": "http://id.loc.gov/vocabulary/relators/fpy",
      "Forger": "http://id.loc.gov/vocabulary/relators/frg",
      "Former owner": "http://id.loc.gov/vocabulary/relators/fmo",
      "Funder": "http://id.loc.gov/vocabulary/relators/fnd",
      "Geographic information specialist": "http://id.loc.gov/vocabulary/relators/gis",
      "Honoree": "http://id.loc.gov/vocabulary/relators/hnr",
      "Host": "http://id.loc.gov/vocabulary/relators/hst",
      "Host institution": "http://id.loc.gov/vocabulary/relators/his",
      "Illuminator": "http://id.loc.gov/vocabulary/relators/ilu",
      "Illustrator": "http://id.loc.gov/vocabulary/relators/ill",
      "Inscriber": "http://id.loc.gov/vocabulary/relators/ins",
      "Instrumentalist": "http://id.loc.gov/vocabulary/relators/itr",
      "Interviewee": "http://id.loc.gov/vocabulary/relators/ive",
      "Interviewer": "http://id.loc.gov/vocabulary/relators/ivr",
      "Inventor": "http://id.loc.gov/vocabulary/relators/inv",
      "Issuing body": "http://id.loc.gov/vocabulary/relators/isb",
      "Judge": "http://id.loc.gov/vocabulary/relators/jud",
      "Jurisdiction governed": "http://id.loc.gov/vocabulary/relators/jug",
      "Laboratory": "http://id.loc.gov/vocabulary/relators/lbr",
      "Laboratory director": "http://id.loc.gov/vocabulary/relators/ldr",
      "Landscape architect": "http://id.loc.gov/vocabulary/relators/lsa",
      "Lead": "http://id.loc.gov/vocabulary/relators/led",
      "Lender": "http://id.loc.gov/vocabulary/relators/len",
      "Libelant": "http://id.loc.gov/vocabulary/relators/lil",
      "Libelant-appellant": "http://id.loc.gov/vocabulary/relators/lit",
      "Libelant-appellee": "http://id.loc.gov/vocabulary/relators/lie",
      "Libelee": "http://id.loc.gov/vocabulary/relators/lel",
      "Libelee-appellant": "http://id.loc.gov/vocabulary/relators/let",
      "Libelee-appellee": "http://id.loc.gov/vocabulary/relators/lee",
      "Librettist": "http://id.loc.gov/vocabulary/relators/lbt",
      "Licensee": "http://id.loc.gov/vocabulary/relators/lse",
      "Licensor": "http://id.loc.gov/vocabulary/relators/lso",
      "Lighting designer": "http://id.loc.gov/vocabulary/relators/lgd",
      "Lithographer": "http://id.loc.gov/vocabulary/relators/ltg",
      "Lyricist": "http://id.loc.gov/vocabulary/relators/lyr",
      "Manufacture place": "http://id.loc.gov/vocabulary/relators/mfp",
      "Manufacturer": "http://id.loc.gov/vocabulary/relators/mfr",
      "Marbler": "http://id.loc.gov/vocabulary/relators/mrb",
      "Markup editor": "http://id.loc.gov/vocabulary/relators/mrk",
      "Medium": "http://id.loc.gov/vocabulary/relators/med",
      "Metadata contact": "http://id.loc.gov/vocabulary/relators/mdc",
      "Metal-engraver": "http://id.loc.gov/vocabulary/relators/mte",
      "Minute taker": "http://id.loc.gov/vocabulary/relators/mtk",
      "Moderator": "http://id.loc.gov/vocabulary/relators/mod",
      "Monitor": "http://id.loc.gov/vocabulary/relators/mon",
      "Music copyist": "http://id.loc.gov/vocabulary/relators/mcp",
      "Musical director": "http://id.loc.gov/vocabulary/relators/msd",
      "Musician": "http://id.loc.gov/vocabulary/relators/mus",
      "Narrator": "http://id.loc.gov/vocabulary/relators/nrt",
      "Onscreen presenter": "http://id.loc.gov/vocabulary/relators/osp",
      "Opponent": "http://id.loc.gov/vocabulary/relators/opn",
      "Organizer": "http://id.loc.gov/vocabulary/relators/orm",
      "Originator": "http://id.loc.gov/vocabulary/relators/org",
      "Other": "http://id.loc.gov/vocabulary/relators/oth",
      "Owner": "http://id.loc.gov/vocabulary/relators/own",
      "Panelist": "http://id.loc.gov/vocabulary/relators/pan",
      "Papermaker": "http://id.loc.gov/vocabulary/relators/ppm",
      "Patent applicant": "http://id.loc.gov/vocabulary/relators/pta",
      "Patent holder": "http://id.loc.gov/vocabulary/relators/pth",
      "Patron": "http://id.loc.gov/vocabulary/relators/pat",
      "Performer": "http://id.loc.gov/vocabulary/relators/prf",
      "Permitting agency": "http://id.loc.gov/vocabulary/relators/pma",
      "Photographer": "http://id.loc.gov/vocabulary/relators/pht",
      "Plaintiff": "http://id.loc.gov/vocabulary/relators/ptf",
      "Plaintiff-appellant": "http://id.loc.gov/vocabulary/relators/ptt",
      "Plaintiff-appellee": "http://id.loc.gov/vocabulary/relators/pte",
      "Platemaker": "http://id.loc.gov/vocabulary/relators/plt",
      "Praeses": "http://id.loc.gov/vocabulary/relators/pra",
      "Presenter": "http://id.loc.gov/vocabulary/relators/pre",
      "Printer": "http://id.loc.gov/vocabulary/relators/prt",
      "Printer of plates": "http://id.loc.gov/vocabulary/relators/pop",
      "Printmaker": "http://id.loc.gov/vocabulary/relators/prm",
      "Process contact": "http://id.loc.gov/vocabulary/relators/prc",
      "Producer": "http://id.loc.gov/vocabulary/relators/pro",
      "Production company": "http://id.loc.gov/vocabulary/relators/prn",
      "Production designer": "http://id.loc.gov/vocabulary/relators/prs",
      "Production manager": "http://id.loc.gov/vocabulary/relators/pmn",
      "Production personnel": "http://id.loc.gov/vocabulary/relators/prd",
      "Production place": "http://id.loc.gov/vocabulary/relators/prp",
      "Programmer": "http://id.loc.gov/vocabulary/relators/prg",
      "Project director": "http://id.loc.gov/vocabulary/relators/pdr",
      "Proofreader": "http://id.loc.gov/vocabulary/relators/pfr",
      "Provider": "http://id.loc.gov/vocabulary/relators/prv",
      "Publication place": "http://id.loc.gov/vocabulary/relators/pup",
      "Publisher": "http://id.loc.gov/vocabulary/relators/pbl",
      "Publishing director": "http://id.loc.gov/vocabulary/relators/pbd",
      "Puppeteer": "http://id.loc.gov/vocabulary/relators/ppt",
      "Radio director": "http://id.loc.gov/vocabulary/relators/rdd",
      "Radio producer": "http://id.loc.gov/vocabulary/relators/rpc",
      "Recording engineer": "http://id.loc.gov/vocabulary/relators/rce",
      "Recordist": "http://id.loc.gov/vocabulary/relators/rcd",
      "Redaktor": "http://id.loc.gov/vocabulary/relators/red",
      "Renderer": "http://id.loc.gov/vocabulary/relators/ren",
      "Reporter": "http://id.loc.gov/vocabulary/relators/rpt",
      "Repository": "http://id.loc.gov/vocabulary/relators/rps",
      "Research team head": "http://id.loc.gov/vocabulary/relators/rth",
      "Research team member": "http://id.loc.gov/vocabulary/relators/rtm",
      "Researcher": "http://id.loc.gov/vocabulary/relators/res",
      "Respondent": "http://id.loc.gov/vocabulary/relators/rsp",
      "Respondent-appellant": "http://id.loc.gov/vocabulary/relators/rst",
      "Respondent-appellee": "http://id.loc.gov/vocabulary/relators/rse",
      "Responsible party": "http://id.loc.gov/vocabulary/relators/rpy",
      "Restager": "http://id.loc.gov/vocabulary/relators/rsg",
      "Restorationist": "http://id.loc.gov/vocabulary/relators/rsr",
      "Reviewer": "http://id.loc.gov/vocabulary/relators/rev",
      "Rubricator": "http://id.loc.gov/vocabulary/relators/rbr",
      "Scenarist": "http://id.loc.gov/vocabulary/relators/sce",
      "Scientific advisor": "http://id.loc.gov/vocabulary/relators/sad",
      "Screenwriter": "http://id.loc.gov/vocabulary/relators/aus",
      "Scribe": "http://id.loc.gov/vocabulary/relators/scr",
      "Sculptor": "http://id.loc.gov/vocabulary/relators/scl",
      "Second party": "http://id.loc.gov/vocabulary/relators/spy",
      "Secretary": "http://id.loc.gov/vocabulary/relators/sec",
      "Seller": "http://id.loc.gov/vocabulary/relators/sll",
      "Set designer": "http://id.loc.gov/vocabulary/relators/std",
      "Setting": "http://id.loc.gov/vocabulary/relators/stg",
      "Signer": "http://id.loc.gov/vocabulary/relators/sgn",
      "Singer": "http://id.loc.gov/vocabulary/relators/sng",
      "Sound designer": "http://id.loc.gov/vocabulary/relators/sds",
      "Speaker": "http://id.loc.gov/vocabulary/relators/spk",
      "Sponsor": "http://id.loc.gov/vocabulary/relators/spn",
      "Stage director": "http://id.loc.gov/vocabulary/relators/sgd",
      "Stage manager": "http://id.loc.gov/vocabulary/relators/stm",
      "Standards body": "http://id.loc.gov/vocabulary/relators/stn",
      "Stereotyper": "http://id.loc.gov/vocabulary/relators/str",
      "Storyteller": "http://id.loc.gov/vocabulary/relators/stl",
      "Supporting host": "http://id.loc.gov/vocabulary/relators/sht",
      "Surveyor": "http://id.loc.gov/vocabulary/relators/srv",
      "Teacher": "http://id.loc.gov/vocabulary/relators/tch",
      "Technical director": "http://id.loc.gov/vocabulary/relators/tcd",
      "Television director": "http://id.loc.gov/vocabulary/relators/tld",
      "Television producer": "http://id.loc.gov/vocabulary/relators/tlp",
      "Thesis advisor": "http://id.loc.gov/vocabulary/relators/ths",
      "Transcriber": "http://id.loc.gov/vocabulary/relators/trc",
      "Translator": "http://id.loc.gov/vocabulary/relators/trl",
      "Type designer": "http://id.loc.gov/vocabulary/relators/tyd",
      "Typographer": "http://id.loc.gov/vocabulary/relators/tyg",
      "University place": "http://id.loc.gov/vocabulary/relators/uvp",
      "Videographer": "http://id.loc.gov/vocabulary/relators/vdg",
      "Vocalist": "http://id.loc.gov/vocabulary/relators/#NAME?",
      "Voice actor": "http://id.loc.gov/vocabulary/relators/vac",
      "Witness": "http://id.loc.gov/vocabulary/relators/wit",
      "Wood engraver": "http://id.loc.gov/vocabulary/relators/wde",
      "Woodcutter": "http://id.loc.gov/vocabulary/relators/wdc",
      "Writer of accompanying material": "http://id.loc.gov/vocabulary/relators/wam",
      "Writer of added commentary": "http://id.loc.gov/vocabulary/relators/wac",
      "Writer of added lyrics": "http://id.loc.gov/vocabulary/relators/wal",
      "Writer of added text": "http://id.loc.gov/vocabulary/relators/wat",
      "Writer of introduction": "http://id.loc.gov/vocabulary/relators/win",
      "Writer of preface": "http://id.loc.gov/vocabulary/relators/wpr",
      "Writer of supplementary textual content": "http://id.loc.gov/vocabulary/relators/wst"
    }
    if relator_label in relatorsData:
        return relatorsData[relator_label]
    else:
        return None

def process_transfers(
    am_user,
    am_api_key,
    ss_user,
    ss_api_key,
    ts_uuid,
    ts_path,
    depth,
    am_url,
    ss_url,
    transfer_type,
    see_files,
    omeka_api,
    omeka_api_key_identity,
    omeka_api_key_credential,
    pipeline_uuid,
    processing_uuid,
    s3_aip_uuid,
    s3_dip_uuid,
    shared_directory,
    dip_path,
    hide_on_complete,
    delete_on_complete,
    config_file,
    log_level,
):
    # Check status of last unit
    current_unit = None
    try:
        current_unit = models.get_current_unit()
        unit_uuid = current_unit.uuid
        unit_type = current_unit.unit_type
    except NoResultFound:
        # Assume a new run if no result can be found in the database.
        LOGGER.debug("No current unit", exc_info=True)
        unit_uuid = unit_type = ""
        LOGGER.info("Current unit: unknown.  Assuming new run.")
        status = "UNKNOWN"
    else:
        LOGGER.info("Current unit: %s", current_unit)
        # Get status
        status_info = get_status(
            am_url,
            am_user,
            am_api_key,
            ss_url,
            ss_user,
            ss_api_key,
            unit_uuid,
            unit_type,
            ts_uuid,
            hide_on_complete,
            delete_on_complete,
        )
        LOGGER.info("Status info: %s", status_info)
        if not status_info:
            LOGGER.error("Could not fetch status for %s. Exiting.", unit_uuid)
            return 1
        try:
            status = status_info.get("status")
            models.update_unit_status(current_unit, status)
        except AttributeError as err:
            LOGGER.error(
                "Cannot read response from server for %s: %s", current_unit, err
            )
            return None

    # If processing, exit
    if status == "PROCESSING":
        LOGGER.info("Current transfer still processing, nothing to do.")
        return 0

    # If waiting on input, send email, exit
    elif status == "USER_INPUT":
        LOGGER.info("Waiting on user input, running scripts in user-input directory.")
        microservice = status_info.get("microservice", "")
        run_scripts(
            "user-input",
            config_file,
            microservice,  # Current microservice name
            # String True or False if this is the first time at this prompt
            str(microservice != current_unit.microservice),
            status_info["path"],  # Absolute path
            status_info["uuid"],  # SIP/Transfer UUID
            status_info["name"],  # SIP/Transfer name
            status_info["type"],  # SIP or transfer
        )
        models.update_unit_microservice(current_unit, microservice)
        return 0

    # If completed ingest, upload DIP
    if (
        status == "COMPLETE"
        and status_info.get("type") == "SIP"
        and status_info.get("directory")
    ):
        #move AIP to S3 location
        LOGGER.info("Moving AIP to S3 location from temporary local location.")
        aip_uuid = status_info.get("uuid")
        data = {'location_uuid': s3_aip_uuid}
        url = ss_url + "/api/v2/file/" + aip_uuid + "/move/"
        headers = {"Authorization": "ApiKey " + ss_user + ":" + ss_api_key + ""}
        response = requests.post(url, headers=headers, data=data, timeout=86400)
        if response.status_code != requests.codes.accepted:
            LOGGER.error("Could not move AIP to S3 location: %s", response.text)
            return None
        else:
            LOGGER.info("AIP successfully moved to S3 location.")

        LOGGER.info("Starting process to upload the DIP to Omeka-S")
        dip = status_info.get("directory")
        path = os.path.join(shared_directory, dip_path, dip)
        #check if the dip exists
        if not os.path.isdir(path):
            LOGGER.error("This DIP does not seem to exist: %s", dip)
            return None
        LOGGER.info("Processing DIP %s", dip)
        try:
            dip_info, mets = process_dip(
                ss_url,
                ss_user,
                ss_api_key,
                dip,
                pipeline_uuid,
                processing_uuid,
                s3_dip_uuid,
                shared_directory,
                dip_path,
            )
        except Exception as e:
            LOGGER.error("Processing of DIP failed: %s", e)
            return None

        LOGGER.info("Parsing metadata from METS file for DIP %s", dip_info["dip-uuid"])
        try:
            data = parse_mets(
                omeka_api,
                omeka_api_key_identity,
                omeka_api_key_credential,
                dip_info,
                mets,
            )
        except Exception as e:
            LOGGER.error("Unable to parse METS file and build json for upload: %s", e)
            return None

        LOGGER.info("Starting upload to Omeka-S with DIP %s", dip_info["dip-uuid"])

        try:
            deposit(
                omeka_api, omeka_api_key_identity, omeka_api_key_credential, data,
            )
        except Exception as e:
            LOGGER.error("Deposit request to Omeka-S failed: %s", e)
            return None

        LOGGER.info("DIP deposited in Omeka-S")

        # Delete local copies of DIP
        LOGGER.info("Deleting local copy of the DIP.")
        try:
            shutil.rmtree(os.path.join(shared_directory, dip_path, dip))
        except (OSError, shutil.Error) as e:
            LOGGER.warning("DIP removal failed: %s", e)

        LOGGER.info("DIP successfully processed and uploaded. Hooray!")

    # If failed, rejected, completed etc, start new transfer
    if current_unit:
        models.update_unit_current(current_unit, False)
    new_transfer = start_transfer(
        ss_url,
        ss_user,
        ss_api_key,
        ts_uuid,
        ts_path,
        depth,
        am_url,
        am_user,
        am_api_key,
        transfer_type,
        see_files,
        config_file,
    )
    return 0 if new_transfer else 1

def main(
    am_user,
    am_api_key,
    ss_user,
    ss_api_key,
    ts_uuid,
    ts_path,
    depth,
    am_url,
    ss_url,
    transfer_type,
    see_files,
    omeka_api,
    omeka_api_key_identity,
    omeka_api_key_credential,
    pipeline_uuid,
    processing_uuid,
    s3_aip_uuid,
    s3_dip_uuid,
    shared_directory,
    dip_path,
    hide_on_complete=False,
    delete_on_complete=False,
    config_file=None,
    log_level="INFO",
):
    """Primary entry point for the automation tools script."""
    loggingconfig.setup(
        log_level, get_setting(config_file, "logfile", defaults.TRANSFER_LOG_FILE)
    )

    LOGGER.info("Automation tools waking up")

    # Check for evidence that this is already running
    default_pidfile = os.path.join(THIS_DIR, "pid.lck")
    pid_file = get_setting(config_file, "pidfile", default_pidfile)
    try:
        # Open PID file only if it doesn't exist for read/write
        f = os.fdopen(os.open(pid_file, os.O_CREAT | os.O_EXCL | os.O_RDWR), "w")
    except OSError:
        LOGGER.error(
            "This script is already running. To override this "
            "behavior and start a new run, remove %s",
            pid_file,
        )
        return 0
    else:
        pid = os.getpid()
        f.write(str(pid))
        f.close()

    # Create a database session to work with.
    create_db_session(config_file)

    # Create the callback to automatically remove pid.lck on script completion.
    setup_automation_execution(pid_file=pid_file)

    #begin processing transfers
    #continue processing transfer until there is no more (return 1) or there is an error (return None). Will return 0 when this needs to be repeated
    while process_transfers(
        am_user,
        am_api_key,
        ss_user,
        ss_api_key,
        ts_uuid,
        ts_path,
        depth,
        am_url,
        ss_url,
        transfer_type,
        see_files,
        omeka_api,
        omeka_api_key_identity,
        omeka_api_key_credential,
        pipeline_uuid,
        processing_uuid,
        s3_aip_uuid,
        s3_dip_uuid,
        shared_directory,
        dip_path,
        hide_on_complete,
        delete_on_complete,
        config_file,
        log_level,
    ) == 0:
        LOGGER.info("Waiting 30 seconds before restarting the transfer process.")
        for remaining in range(30, 0, -1):
            sys.stdout.write("\r")
            sys.stdout.write("{:2d} seconds remaining.".format(remaining))
            sys.stdout.flush()
            time.sleep(1)

    return 1 #there are no more transfers to process or there has been an error


if __name__ == "__main__":
    parser = get_parser(__doc__)
    #add addtional args for omeka upload
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
        "--s3-aip-uuid",
        metavar="UUID",
        required=True,
        help="UUID of the S3 location to move the AIP from temporary storage.",
    )
    parser.add_argument(
        "--s3-dip-uuid",
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
    args = parser.parse_args()

    log_level = loggingconfig.set_log_level(args.log_level, args.quiet, args.verbose)

    sys.exit(
        main(
            am_user=args.user,
            am_api_key=args.api_key,
            ss_user=args.ss_user,
            ss_api_key=args.ss_api_key,
            ts_uuid=args.transfer_source,
            ts_path=args.transfer_path,
            depth=args.depth,
            am_url=args.am_url,
            ss_url=args.ss_url,
            transfer_type=args.transfer_type,
            see_files=args.files,
            omeka_api=args.omeka_api,
            omeka_api_key_identity=args.omeka_api_key_identity,
            omeka_api_key_credential=args.omeka_api_key_credential,
            pipeline_uuid=args.pipeline_uuid,
            processing_uuid=args.processing_uuid,
            s3_aip_uuid=args.s3_aip_uuid,
            s3_dip_uuid=args.s3_dip_uuid,
            shared_directory=args.shared_directory,
            dip_path=args.dip_path,
            hide_on_complete=args.hide,
            delete_on_complete=args.delete_on_complete,
            config_file=args.config_file,
            log_level=log_level,
        )
    )
