#!/usr/bin/env python3

'''
The program will pull data from the elastic inventory index and will pass the host down to Ansible to be started.
This version queries Elasticsearch ONLY for specific hosts defined in aws_stop_exclusions.json,
and generates a single Ansible inventory list for these hosts.
Before loading, it fetches the aws_stop_exclusions.json from GitHub using the REST API,
verifies its integrity (downloaded content hash vs. local file hash), and writes it locally.
The script includes a fallback and retry mechanism. If the initial fetch from GitHub fails,
it will use the existing local aws_stop_exclusions.json file and start a background process to retry the download.
The background retry process will now attempt to download the file a limited number of times (NUM_OF_RETRIES).
Once a new file is successfully downloaded, it will process any new hosts that were added.
Also, the script includes a retry mechanism for the Elasticsearch GET request, with a fixed number of retries.

GitHub PAT is retrieved from AWS Secrets Manager (secret:
github_hsv_internal/itsma/service_elastic_auto_HSV) using the aws_ELK role assumed
via IAM Roles Anywhere. The RolesAnywhere credentials are sourced from
/home/logstash/jenkins_config via the AWS_CONFIG_FILE environment variable,
which allows the script to authenticate to AWS even when running as root under cron.
'''

import requests
import jmespath
import subprocess
import argparse
import os
import sys
import datetime
import time
import threading
from pathlib import Path
import helpers.json_log_format as jlf
import json
import hashlib
import urllib3
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Configurations

# Get the directory where this script is located
# Define the log directory as a path relative to the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = "/var/log/agent_restart_automation"

# Ensure the log directory exists, creating if it it does not
if not os.path.exists(logs_dir):
    try:
        os.makedirs(logs_dir)
    except OSError as e:
        print(f"Error creating log directory '{logs_dir}': {e}")
        sys.exit(1)

today_date = datetime.datetime.utcnow().strftime("%m%d%Y")
# Construct the full, absolute path for the log file
logfilename = os.path.join(logs_dir, f"agent_restart_automation-{today_date}.json")
jlf.service_name = Path(__file__).stem
jlf.service_type = 'monitoring-scripts'


jlf.json_logging.init_non_web(custom_formatter=jlf.CustomJSONLog, enable_json=True)
logger = jlf.logging.getLogger(__name__)
logger.setLevel(jlf.logging.DEBUG)
logger.addHandler(jlf.logging.FileHandler(logfilename))

# GitHub repository details - migrated from GHE to github.com/hsv-internal
GITHUB_API_HOST = "https://api.github.com"
GITHUB_API_OWNER = "hsv-internal"
GITHUB_API_REPO = "Config"
GITHUB_API_FILE_PATH = "aws/aws_stop_exclusions.json"
GITHUB_BRANCH = "master" ## Or 'main', or the specific branch/tag you want to fetch from
LOCAL_EXCLUSION_FILEPATH = "aws_stop_exclusions.json"
RETRY_INTERVAL_MINUTES = 15 # Time to wait between retries for GitHub file fetch
NUM_OF_RETRIES = 5 # Number of times the background thread will retry fetching the GitHub file
NUM_RETRY_ELASTIC_REQUEST = 3 # Number of times to retry the Elasticsearch GET request

# AWS Secrets Manager configuration for GitHub PAT retrieval
AWS_CONFIG_FILE_PATH = "/home/logstash/jenkins_config"
AWS_PROFILE_NAME = "default"
AWS_REGION = "us-east-2"
GITHUB_PAT_SECRET_ID = "arn:aws:secretsmanager:us-east-2:469620122115:secret:github_hsv_internal/itsma/service_elastic_auto_HSV-SRVK6D"

# Vars for the api call
apivars = {
    "systemtype": {
        "win": "windows",
        "lin": "centos"
    },
    "uuid": {
        "hst": "mXqbzwdKSvSeNAJ1lM5oIw",
        "hsc": "uXgaSKNVTnSv3BrGSmfmsQ"
    }
}

procvars = {
    "url": {
        "hst": ".hedgeservtest.com",
        "hsc": ".hedgeservcustomers.com",
        "hsm": ".hedgeservmgmt.com",
        "hse": ".funddevelopmentservices.com",
        "hsw": ".hedgeservweb.com",
        "htw": ".hedgeservtestweb.com"
    }
}


def get_github_pat_from_secrets_manager():
    """
    Retrieves the GitHub Personal Access Token from AWS Secrets Manager using
    the aws_ELK role assumed via IAM Roles Anywhere.

    The RolesAnywhere credentials are sourced from /home/logstash/jenkins_config
    by setting AWS_CONFIG_FILE before initializing the boto3 session. This allows
    the script to authenticate even when running as root under cron, which would
    otherwise have no AWS credentials configured.

    The secret is expected to contain the PAT either as:
      - A JSON object with one of these keys: 'github_pat', 'token', 'password'
      - A plain string containing only the PAT

    Returns:
        str: The GitHub PAT, or None if retrieval fails.
    """
    print(f"{datetime.datetime.utcnow().isoformat()} Retrieving GitHub PAT from AWS Secrets Manager...")
    logger.info("Retrieving GitHub PAT from AWS Secrets Manager...")

    # Point boto3 at the RolesAnywhere config file so credential_process resolves
    # correctly regardless of which user is running the script (root via cron,
    # logstash interactively, etc.). Save and restore the original env value to
    # avoid side effects on the rest of the process.
    original_aws_config_file = os.environ.get('AWS_CONFIG_FILE')
    os.environ['AWS_CONFIG_FILE'] = AWS_CONFIG_FILE_PATH

    try:
        session = boto3.Session(profile_name=AWS_PROFILE_NAME, region_name=AWS_REGION)
        client = session.client('secretsmanager')

        response = client.get_secret_value(SecretId=GITHUB_PAT_SECRET_ID)
        secret_string = response.get('SecretString')

        if not secret_string:
            logger.error("SecretString is empty in the Secrets Manager response.")
            print(f"{datetime.datetime.utcnow().isoformat()} Error: SecretString is empty in the response.")
            return None

        # Try to parse as JSON first; fall back to treating it as a plain string
        github_pat = None
        try:
            secret_data = json.loads(secret_string)
            if isinstance(secret_data, dict):
                # Try common key names in order of likelihood
                for key in ('github_pat', 'token', 'password', 'pat', 'GITHUB_PAT'):
                    if key in secret_data and secret_data[key]:
                        github_pat = secret_data[key]
                        logger.info(f"Extracted GitHub PAT from secret using key '{key}'.")
                        break
                if not github_pat:
                    logger.error(f"Secret JSON did not contain any of the expected keys "
                                 f"(github_pat, token, password, pat, GITHUB_PAT). "
                                 f"Found keys: {list(secret_data.keys())}")
                    print(f"{datetime.datetime.utcnow().isoformat()} Error: Secret JSON did not contain expected keys.")
                    return None
            else:
                # JSON was valid but not a dict (e.g. just a quoted string)
                github_pat = str(secret_data)
        except json.JSONDecodeError:
            # Not JSON — treat as plain string PAT
            github_pat = secret_string.strip()
            logger.info("Secret value parsed as plain string (not JSON).")

        if github_pat:
            print(f"{datetime.datetime.utcnow().isoformat()} Successfully retrieved GitHub PAT from Secrets Manager.")
            logger.info("Successfully retrieved GitHub PAT from Secrets Manager.")
            return github_pat
        else:
            logger.error("GitHub PAT value was empty after parsing the secret.")
            return None

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_message = e.response.get('Error', {}).get('Message', str(e))
        print(f"{datetime.datetime.utcnow().isoformat()} AWS ClientError retrieving secret "
              f"({error_code}): {error_message}")
        logger.error(f"AWS ClientError retrieving secret ({error_code}): {error_message}")
        return None
    except BotoCoreError as e:
        print(f"{datetime.datetime.utcnow().isoformat()} BotoCoreError retrieving secret: {e}")
        logger.error(f"BotoCoreError retrieving secret: {e}")
        return None
    except Exception as e:
        print(f"{datetime.datetime.utcnow().isoformat()} Unexpected error retrieving secret: {e}")
        logger.error(f"Unexpected error retrieving secret: {e}")
        return None
    finally:
        # Restore the original AWS_CONFIG_FILE environment variable
        if original_aws_config_file is None:
            os.environ.pop('AWS_CONFIG_FILE', None)
        else:
            os.environ['AWS_CONFIG_FILE'] = original_aws_config_file


def fetch_and_verify_github_file(api_host, owner, repo, file_path_in_repo, branch, local_filepath, github_pat):
    """
    Fetches a file from GitHub (github.com) using the REST API's contents endpoint,
    authenticating with a Personal Access Token (PAT) via the Authorization: token header.
    Calculates its SHA256 hash, writes it to a local file, and then calculates
    the hash of the local file to ensure write integrity.

    Args:
        api_host (str): The base URL for the GitHub API (e.g., "https://api.github.com").
        owner (str): The repository owner/organization name.
        repo (str): The repository name.
        file_path_in_repo (str): The path to the file within the repository (e.g., "path/to/file.json").
        branch (str): The branch name (e.g., "master").
        local_filepath (str): The local path to save the file.
        github_pat (str): GitHub Personal Access Token for authentication.

    Returns:
        bool: True if the file was successfully fetched, written, and its integrity verified, False otherwise.
    """
    # Construct the full GitHub API URL for the contents endpoint
    full_api_url = f"{api_host}/repos/{owner}/{repo}/contents/{file_path_in_repo}?ref={branch}"

    print(f"{datetime.datetime.utcnow().isoformat()} Attempting to fetch {full_api_url} using GitHub API (PAT)...")
    logger.info(f"Attempting to fetch {full_api_url} using GitHub API (PAT)...")

    headers = {
        "Authorization": f"token {github_pat}",
        "Accept": "application/vnd.github.v3.raw",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    try:
        # github.com uses valid TLS certs, so verify=True is safe here
        response = requests.get(full_api_url, headers=headers, verify=True, timeout=10)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        downloaded_content = response.content # Get content as bytes
        downloaded_content_hash = hashlib.sha256(downloaded_content).hexdigest()

        print(f"{datetime.datetime.utcnow().isoformat()} Hash of downloaded content: {downloaded_content_hash}")
        logger.info(f"Hash of downloaded content: {downloaded_content_hash}")

        # Write the content to the local file
        with open(local_filepath, 'wb') as f:
            f.write(downloaded_content)

        # Calculate hash of the locally written file
        with open(local_filepath, 'rb') as f:
            local_file_content = f.read()
            local_file_hash = hashlib.sha256(local_file_content).hexdigest()

        print(f"{datetime.datetime.utcnow().isoformat()} Hash of local file ({local_filepath}): {local_file_hash}")
        logger.info(f"Hash of local file ({local_filepath}): {local_file_hash}")

        if downloaded_content_hash == local_file_hash:
            print(f"{datetime.datetime.utcnow().isoformat()} Successfully fetched, written, and verified integrity of {local_filepath}.")
            logger.info(f"Successfully fetched, written, and verified integrity of {local_filepath}.")
            return True
        else:
            print(f"{datetime.datetime.utcnow().isoformat()} Error: Integrity check failed for {local_filepath}.")
            print(f"Downloaded content hash: {downloaded_content_hash}, Local file hash: {local_file_hash}")
            logger.error(f"Integrity check failed for {local_filepath}. Downloaded: {downloaded_content_hash}, Local: {local_file_hash}")
            return False

    except requests.exceptions.HTTPError as e:
        print(f"{datetime.datetime.utcnow().isoformat()} HTTP error fetching {full_api_url}: {e}")
        logger.error(f"HTTP error fetching {full_api_url}: {e}")
        if e.response.status_code == 401:
            print("Check your GITHUB_PAT and ensure it has the 'repo' scope (or 'contents:read' for fine-grained tokens).")
            logger.error("Check GITHUB_PAT and ensure it has the 'repo' scope (or 'contents:read' for fine-grained tokens).")
        elif e.response.status_code == 403:
            print("Access forbidden. Check your PAT permissions and rate limits.")
            logger.error("Access forbidden. Check PAT permissions and rate limits.")
        elif e.response.status_code == 404:
            print("Repository or file not found. Verify owner, repo, file path, and branch.")
            logger.error("Repository or file not found. Verify owner, repo, file path, and branch.")
        return False
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        # This exception handles the fallback logic
        print(f"{datetime.datetime.utcnow().isoformat()} Connection/Timeout error fetching {full_api_url}: {e}")
        logger.error(f"Connection/Timeout error fetching {full_api_url}: {e}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"{datetime.datetime.utcnow().isoformat()} An unexpected error occurred during request for {full_api_url}: {e}")
        logger.error(f"An unexpected error occurred during request for {full_api_url}: {e}")
        return False
    except Exception as e:
        print(f"{datetime.datetime.utcnow().isoformat()} An unexpected error occurred during file fetch/verification: {e}")
        logger.error(f"An unexpected error occurred during file fetch/verification: {e}")
        return False


def load_exclusion_list(filepath=LOCAL_EXCLUSION_FILEPATH):
    """
    Loads a list of hostnames from a JSON file for inclusion purposes in the ES query.
    The JSON file is expected to have a top-level key "exclusion_list"
    containing an array of hostnames.
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        if "exclusion_list" in data and isinstance(data["exclusion_list"], list):
            logger.info(f"Successfully loaded exclusion list from {filepath}")
            print(f"Successfully loaded exclusion list from {filepath}")
            # Return both lowercase and uppercase versions for each host for case-insensitive ES query
            return [h_case for host in data["exclusion_list"] for h_case in (host.lower(), host.upper())]
        else:
            logger.error(f"Error: 'exclusion_list' key not found or not a list in {filepath}")
            print(f"Error: 'exclusion_list' key not found or not a list in {filepath}")
            return []
    except FileNotFoundError:
        logger.error(f"Error: Exclusion file not found at {filepath}. Ensure it was fetched or exists locally.")
        print(f"Error: Exclusion file not found at {filepath}. Ensure it was fetched or exists locally.")
        return []
    except json.JSONDecodeError:
        logger.error(f"Error: Invalid JSON format in {filepath}")
        print(f"Error: Invalid JSON format in {filepath}")
        return []
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading exclusion list from {filepath}: {e}")
        print(f"An unexpected error occurred while loading exclusion list from {filepath}: {e}")
        return []

def read_data_from_elk(systemtype, domain, hosts_to_query):
    """
    Queries Elasticsearch for hosts that are in the provided hosts_to_query list,
    are 'down', and match the specified systemtype, while excluding 'ip-*' hosts.
    Includes a retry mechanism for the GET request.
    """
    print(datetime.datetime.utcnow().isoformat(), " API Call to ElasticSearch - CCS ...")
    logger.info(f" API Call to ElasticSearch - CCS ...")
    print(f"hosts to query: {hosts_to_query}")
    elastic_api = "ApiKey U0QzLTBuWUJuZlVPVDlXQkE0a1c6YjFoS2VBQUpTVFd4YXFRZmxZY1YtQQ=="
    url = "https://de26459b90754aceb3234fe7969cb1ee.us-east-1.aws.found.io:9243/inventory/_search"
    querystring = {"": "", "filter_path": "aggregations.uhosts.buckets"}

    systemtype_map = {
        "lin": "centos",
        "win": "windows"
    }
    actual_systemtype = systemtype_map.get(systemtype)

    # Dynamically build the Elasticsearch query payload
    payload_dict = {
        "size": 0,
        "sort": [{"_score": {"order": "desc"}}],
        "_source": {"includes": ["beats_state.beat.host"]},
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "beats_state.beat.name.keyword": hosts_to_query # Target only hosts from the exclusion list
                        }
                    },
                    { # Exclude 'ip-*' hosts
                        "bool": {
                            "must_not": {
                                "bool": {
                                    "should": [{"query_string": {"fields": ["beats_state.beat.name.keyword"], "query": "ip-*"}}],
                                    "minimum_should_match": 1
                                }
                            }
                        }
                    }
                ],
                "filter": [
                    {"range": {"timestamp": {"gte": "now-5d", "lt": "now"}}},
                    {"bool": {"should": [
                        {"match_phrase": {"beats_state.beat.type": "metricbeat"}},
                        {"match_phrase": {"beats_state.beat.type": "filebeat"}}
                    ],"minimum_should_match": 1}},
                    {"match_phrase": {"status": "down"}},
                    {"match_phrase": {"beats_state.state.host.os.platform": actual_systemtype.lower()}},
                    {"exists": {"field": "timestamp"}}
                ],
                "should": [],
                "must_not": []
            }
        },
        "aggs": {
            "uhosts": {
                "terms": {
                    "field": "beats_state.beat.name.keyword",
                    "size": 2000
                }
            }
        }
    }

    # If hosts_to_query is empty, the 'terms' query will be empty, naturally returning no results.
    if not hosts_to_query:
        print(f"{datetime.datetime.utcnow().isoformat()} Warning: No hosts provided for Elasticsearch query. It will return no results.")
        logger.warning("No hosts provided for Elasticsearch query. It will return no results.")

    payload = json.dumps(payload_dict) # Convert dictionary to JSON string

    headers = {
        'Authorization': elastic_api,
        'Content-Type': "application/json"
    }

    response = None
    for attempt in range(1, NUM_RETRY_ELASTIC_REQUEST + 1):
        try:
            print(f"{datetime.datetime.utcnow().isoformat()} Attempt {attempt}/{NUM_RETRY_ELASTIC_REQUEST} to query Elasticsearch...")
            logger.info(f"Attempt {attempt}/{NUM_RETRY_ELASTIC_REQUEST} to query Elasticsearch...")
            response = requests.request("GET", url, data=payload, headers=headers, params=querystring, verify=False, timeout=30) # Increased timeout for Elastic
            response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
            print(datetime.datetime.utcnow().isoformat(), " API Call to ElasticSearch - CSS Done!!! Response Code: ", response.status_code)
            logger.info(f" API Call to ElasticSearch - CSS Done!!! Response Code: {response.status_code}")
            print("RAW DATA :")
            print(response.text)
            print(f"payload: {payload}")
            return response
        except requests.exceptions.RequestException as e:
            print(f"{datetime.datetime.utcnow().isoformat()} Error querying Elasticsearch (Attempt {attempt}/{NUM_RETRY_ELASTIC_REQUEST}): {e}")
            logger.error(f"Error querying Elasticsearch (Attempt {attempt}/{NUM_RETRY_ELASTIC_REQUEST}): {e}")
            if attempt == NUM_RETRY_ELASTIC_REQUEST:
                print(f"{datetime.datetime.utcnow().isoformat()} All {NUM_RETRY_ELASTIC_REQUEST} Elasticsearch query retries exhausted. Skipping Elasticsearch data processing.")
                logger.critical(f"All {NUM_RETRY_ELASTIC_REQUEST} Elasticsearch query retries exhausted. Skipping Elasticsearch data processing.")
                # Re-raise the exception or return an empty/error response if needed downstream
                raise # Re-raise the last exception if all retries fail
            time.sleep(5) # Wait for a few seconds before retrying

    return response # This line should ideally not be reached if an exception is re-raised or successful response is returned


def process_data(api_response, systemtype, domain):
    """
    Processes the Elasticsearch API response to extract hostnames and formats them
    into a single Ansible inventory list for the specified DR servers, applying
    domain suffixes based on host prefixes and the selected domain.
    """
    print(f"{datetime.datetime.utcnow().isoformat()} Starting data processing...")

    list_host_down = jmespath.search("aggregations.uhosts.buckets[*].key", api_response)

    inventory_list = []

    if domain == 'hst':
        # For 'hst' domain, apply specific domains based on 'ts' or 'tw' prefixes.
        for host in list_host_down:
            if host.lower().startswith('ts'):
                inventory_list.append(host + procvars["url"]["hst"]) # .hedgeservtest.com
            elif host.lower().startswith('tw'):
                inventory_list.append(host + procvars["url"]["htw"]) # .hedgeservtestweb.com
            # Hosts in list_host_down that do not match these prefixes
            # will not be added to the inventory_list for the 'hst' domain.
    elif domain == 'hsc':
        # For 'hsc' domain, apply specific domains based on *51 prefixes.
        for host in list_host_down:
            if host.lower().startswith('cs'):
                inventory_list.append(host + procvars["url"]["hsc"]) # .hedgeservcustomers.com
            elif host.lower().startswith('ms'):
                inventory_list.append(host + procvars["url"]["hsm"]) # .hedgeservmgmt.com
            elif host.lower().startswith('es'):
                inventory_list.append(host + procvars["url"]["hse"]) # .funddevelopmentservices.com
            elif host.lower().startswith('cw'):
                inventory_list.append(host + procvars["url"]["hsw"]) # .hedgeservweb.com
            # Hosts in list_host_down that do not match these *51 prefixes
            # will not be added to the inventory_list.
    else:
        # Handle unexpected domain argument, or simply leave inventory_list empty
        print(f"Warning: Unknown domain '{domain}'. No hosts processed for this domain.")
        logger.warning(f"Unknown domain '{domain}'. No hosts processed for this domain.")


    if inventory_list: # Add Ansible group tag if hosts exist
        inventory_list.insert(0, f"[{systemtype}_{domain}]") # More descriptive group name
        logger.info(f" Ansible Inventory File contains the following hosts: {inventory_list}")
        print("HOST LIST AFTER PROCESSING : ================")
        print(inventory_list)
    else:
        print(f"No DR agents found in Elasticsearch matching the provided criteria for domain {domain}.")
        logger.info(f"No DR agents found in Elasticsearch matching the provided criteria for domain {domain}.")

    print(f"{datetime.datetime.utcnow().isoformat()} Completed data processing.")

    return inventory_list


def write_data_to_file(data_to_write, systemtype, domain):
    """
    Writes the list of hostnames to an Ansible inventory file.
    The filename will be specific to the DR servers.
    """
    filename = f"{script_dir}/{systemtype}_{domain}" # Explicitly name for DR inventory
    with open(filename, 'w') as inventory_file:
        for element in data_to_write:
            inventory_file.write(element)
            inventory_file.write('\n')
    print(f"{datetime.datetime.utcnow().isoformat()}  File Result: Complete {filename}")
    return filename

def start_ansible(filename, systemtype, domain):
    """
    Executes the Ansible playbook against the specified inventory file.
    The playbook name is derived from the systemtype.
    """
    logger.info(f"Starting ansible playbook for inventory: {filename}...")
    print(f"Starting ansible playbook for inventory: {filename}...")
    cmd = f"ansible-playbook {script_dir}/{systemtype}_start_services.yaml --inventory-file {filename} --vault-password-file {script_dir}/value.yml -e @{script_dir}/vault.yml"
    print(f"{datetime.datetime.utcnow().isoformat()} {cmd}")

    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    output = stdout.decode()
    formatted_output = output.replace('\\n', '\n')
    print(f"{formatted_output}")

    if stdout:
        logger.info("RESULT: " + formatted_output)
        print("RESULT: " + formatted_output)
    if stderr:
        logger.error("ERROR: " + stderr.decode())
        print("ERROR: " + stderr.decode())

def _process_hosts_pipeline(host_list, systemtype, domain):
    """
    A helper function to run the full pipeline of querying ES and running Ansible
    for a given list of hosts. This keeps the main logic DRY.
    """
    print(f"\n{datetime.datetime.utcnow().isoformat()} --- Starting host processing for a new list of hosts. ---")
    logger.info(f"--- Starting host processing for a new list of hosts. ---")

    # 1. Read data from Elasticsearch
    try:
        data = read_data_from_elk(systemtype, domain, host_list)
        api_response = data.json()
    except requests.exceptions.RequestException:
        # If Elasticsearch query failed after retries, stop processing for this pipeline run
        print(f"{datetime.datetime.utcnow().isoformat()} Failed to retrieve data from Elasticsearch after multiple retries. Aborting host processing pipeline for {systemtype} {domain}.")
        logger.error(f"Failed to retrieve data from Elasticsearch after multiple retries. Aborting host processing pipeline for {systemtype} {domain}.")
        return # Exit the function if ES data couldn't be fetched

    # 2. Process the returned hosts into an inventory list
    inventory_list = process_data(api_response, systemtype, domain)

    # 3. Write and run playbook for the single inventory list
    if inventory_list:
        filename = write_data_to_file(inventory_list, systemtype, domain)
        start_ansible(filename, systemtype, domain)

    print(f"{datetime.datetime.utcnow().isoformat()} --- Finished host processing. ---")
    logger.info(f"--- Finished host processing. ---")


def retry_fetch_thread(initial_host_list, systemtype, domain, github_pat):
    """
    This function runs in a separate thread to periodically check for new
    versions of the exclusion list from GitHub and processes any new hosts.
    It now retries for a limited number of times (NUM_OF_RETRIES).

    The GitHub PAT is passed in from main() so we don't have to re-fetch it from
    Secrets Manager on every retry attempt.
    """
    print(f"\n{datetime.datetime.utcnow().isoformat()} Starting background retry thread. Will check for new hosts every {RETRY_INTERVAL_MINUTES} minutes, for up to {NUM_OF_RETRIES} times.")
    logger.info(f"Starting background retry thread. Will check for new hosts every {RETRY_INTERVAL_MINUTES} minutes, for up to {NUM_OF_RETRIES} times.")

    for attempt in range(1, NUM_OF_RETRIES + 1):
        time.sleep(RETRY_INTERVAL_MINUTES * 60) # Wait for the specified interval
        print(f"\n{datetime.datetime.utcnow().isoformat()} Retrying to fetch exclusion list from GitHub (Attempt {attempt}/{NUM_OF_RETRIES})...")
        logger.info(f"Retrying to fetch exclusion list from GitHub (Attempt {attempt}/{NUM_OF_RETRIES})...")

        # Attempt to fetch the new file
        fetch_success = fetch_and_verify_github_file(
            GITHUB_API_HOST, GITHUB_API_OWNER, GITHUB_API_REPO,
            GITHUB_API_FILE_PATH, GITHUB_BRANCH,
            LOCAL_EXCLUSION_FILEPATH, github_pat
        )

        if fetch_success:
            print(f"{datetime.datetime.utcnow().isoformat()} Successfully fetched new exclusion file during retry.")
            logger.info(f"Successfully fetched new exclusion file during retry.")
            new_hosts_list = load_exclusion_list(filepath=LOCAL_EXCLUSION_FILEPATH)

            # Compare the new list with the old list to find new servers
            initial_set = set(initial_host_list)
            new_set = set(new_hosts_list)
            new_servers_for_processing = list(new_set - initial_set)

            if new_servers_for_processing:
                print(f"{datetime.datetime.utcnow().isoformat()} Found new servers to process: {new_servers_for_processing}")
                logger.info(f"Found new servers to process: {new_servers_for_processing}")
                try:
                    _process_hosts_pipeline(new_servers_for_processing, systemtype, domain)
                except requests.exceptions.RequestException:
                    print(f"{datetime.datetime.utcnow().isoformat()} Skipping Ansible run due to Elasticsearch query failure in background thread.")
                    logger.error(f"Skipping Ansible run due to Elasticsearch query failure in background thread.")
            else:
                print(f"{datetime.datetime.utcnow().isoformat()} No new servers found in the updated exclusion list.")
                logger.info(f"No new servers found in the updated exclusion list.")

            break # Exit the loop once successful
        else:
            print(f"{datetime.datetime.utcnow().isoformat()} Retry failed. Will try again in {RETRY_INTERVAL_MINUTES} minutes.")
            logger.info(f"Retry failed. Will try again in {RETRY_INTERVAL_MINUTES} minutes.")

            if attempt == NUM_OF_RETRIES:
                print(f"{datetime.datetime.utcnow().isoformat()} All {NUM_OF_RETRIES} retries exhausted. Background fetch process stopping.")
                logger.error(f"All {NUM_OF_RETRIES} retries exhausted. Background fetch process stopping.")
                # No break here, loop naturally ends after the last attempt.


def main(systemtype, domain):
    """
    Main function to orchestrate the process, including fallback and retry logic.
    """
    # 1. Retrieve GitHub PAT from AWS Secrets Manager (replaces GITHUB_PAT env var)
    github_pat = get_github_pat_from_secrets_manager()

    if not github_pat:
        print(f"{datetime.datetime.utcnow().isoformat()} Error: Failed to retrieve GitHub PAT from AWS Secrets Manager. Exiting.")
        logger.error("Failed to retrieve GitHub PAT from AWS Secrets Manager. Exiting.")
        sys.exit("GitHub PAT could not be retrieved from Secrets Manager.")

    # 2. Attempt to fetch the exclusion list from GitHub initially
    print(f"\n{datetime.datetime.utcnow().isoformat()} --- Initial host list fetch attempt. ---")
    fetch_success = fetch_and_verify_github_file(
        GITHUB_API_HOST, GITHUB_API_OWNER, GITHUB_API_REPO,
        GITHUB_API_FILE_PATH, GITHUB_BRANCH,
        LOCAL_EXCLUSION_FILEPATH, github_pat
    )

    # 3. Handle successful fetch or fallback to local file
    initial_host_list = []
    if fetch_success:
        print(f"{datetime.datetime.utcnow().isoformat()} Initial fetch from GitHub was successful.")
        initial_host_list = load_exclusion_list(filepath=LOCAL_EXCLUSION_FILEPATH)
        # 4. Run the main processing pipeline for the initial host list
        try:
            _process_hosts_pipeline(initial_host_list, systemtype, domain)
        except requests.exceptions.RequestException:
            print(f"{datetime.datetime.utcnow().isoformat()} Initial Elasticsearch query failed. Proceeding with background retry for GitHub file.")
            logger.error(f"Initial Elasticsearch query failed. Proceeding with background retry for GitHub file.")
            # If the initial ES query fails, we still want the background thread to run
            # to potentially fetch a new GitHub file and try processing new hosts later.
            pass # Do not exit, allow background thread to start
    else:
        print(f"{datetime.datetime.utcnow().isoformat()} Initial fetch from GitHub failed. Falling back to the existing local file.")
        logger.warning("Initial GitHub fetch failed. Falling back to local file.")
        initial_host_list = load_exclusion_list(filepath=LOCAL_EXCLUSION_FILEPATH)

        # Check if we have a valid list of hosts to process from the local file
        if not initial_host_list:
            print(f"{datetime.datetime.utcnow().isoformat()} Error: 'aws_stop_exclusions.json' is empty or invalid. Exiting as no DR servers can be queried.")
            logger.error("No valid hosts found in 'aws_stop_exclusions.json'. Exiting as no DR servers can be queried.")
            sys.exit("No hosts in exclusion list to query.")

        # Run the initial processing pipeline with the local file
        try:
            _process_hosts_pipeline(initial_host_list, systemtype, domain)
        except requests.exceptions.RequestException:
            print(f"{datetime.datetime.utcnow().isoformat()} Initial Elasticsearch query failed. Proceeding with background retry for GitHub file.")
            logger.error(f"Initial Elasticsearch query failed. Proceeding with background retry for GitHub file.")
            pass # Do not exit, allow background thread to start

        # Start a background retry thread that will handle any new hosts from future successful fetches
        retry_thread = threading.Thread(target=retry_fetch_thread, args=(initial_host_list, systemtype, domain, github_pat), daemon=True)
        retry_thread.start()

        try:
            print("----------------------------------------------------------------------------------------------------------------------------------")
            print("Script finished main execution. The background retry thread is now running. Press Ctrl+C to stop the script.")
            # The main thread waits here indefinitely for the daemon thread to finish.
            # Since the daemon thread runs until a successful fetch or all retries are exhausted, this keeps the main process alive.
            retry_thread.join()
        except KeyboardInterrupt:
            print("Stopping script and background thread...")

    print("----------------------------------------------------------------------------------------------------------------------------------")
    print("Script execution finished.")
    # The script can exit here, as the daemon thread will be terminated automatically if the main thread completes without a join() call.


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='This script queries Elasticsearch ONLY for specific DR servers listed in aws_stop_exclusions.json that have stopped agents, then generates a single Ansible inventory and runs a playbook on them. Expects two arguments.')
    parser.add_argument('--systemtype', type=str, help='win -> Windows hosts; lin -> Linux hosts')
    parser.add_argument('--domain', type=str, help='hsc -> Hosts in hscustomers.com; hst -> Hosts in hedgeservcustomers.com')
    args = parser.parse_args()

    # We must handle the case where the domain/systemtype is not provided
    if not args.systemtype or not args.domain:
        parser.error("Both --systemtype and --domain are required arguments.")
        sys.exit("Missing required arguments.")

    main(args.systemtype, args.domain)
