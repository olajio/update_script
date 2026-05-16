```
## Summary

Updated /root/agent_restart_automation/agent_restart.py to retrieve the GitHub
Personal Access Token from AWS Secrets Manager instead of relying on the
GITHUB_PAT environment variable. The script authenticates to AWS using the
aws_ELK role assumed via IAM Roles Anywhere, sourcing credentials from
/home/logstash/jenkins_config. This eliminates the need to manage a long-lived
PAT in environment variables or local configuration on the host, and aligns
with the existing IAM Roles Anywhere pattern already in use on the ELK servers.

---

## Background

The script previously read the GitHub PAT from the GITHUB_PAT environment
variable, which required the PAT to be stored on disk or injected via a
wrapper script. Storing the PAT in AWS Secrets Manager centralizes credential
management, enables rotation without code changes, and removes a long-lived
secret from the host.

The secret retrieval needed to work under cron as the root user, which has no
AWS credentials configured by default. The script handles this by
programmatically setting AWS_CONFIG_FILE to point at the existing logstash
user's jenkins_config file, allowing boto3 to use the RolesAnywhere
credential_process regardless of which user runs the script.

---

## Code Changes

### 1. Added imports
  - boto3
  - botocore.exceptions.BotoCoreError
  - botocore.exceptions.ClientError

### 2. Added configuration constants
  AWS_CONFIG_FILE_PATH  = "/home/logstash/jenkins_config"
  AWS_PROFILE_NAME      = "default"
  AWS_REGION            = "us-east-2"
  GITHUB_PAT_SECRET_ID  = "arn:aws:secretsmanager:us-east-2:469620122115:secret:
                          github_hsv_internal/itsma/service_elastic_auto_HSV-SRVK6D"

### 3. Added new function: get_github_pat_from_secrets_manager()
  - Programmatically sets AWS_CONFIG_FILE to /home/logstash/jenkins_config so
    boto3 picks up the RolesAnywhere credential_process. Saves and restores
    the original env value in a finally block to avoid side effects.
  - Creates a boto3 Session with profile_name='default' and region 'us-east-2'.
  - Calls secretsmanager.get_secret_value() on the GitHub PAT secret.
  - Parses the secret value as JSON and extracts the PAT. The secret format
    is {"user": "service-elasticauto_HSV", "password": "ghp_..."} so the
    function looks up the key 'password'. The function also accepts
    'github_pat', 'token', 'pat', 'GITHUB_PAT' for forward compatibility,
    and falls back to treating the secret as a plain string if it isn't JSON.
  - Catches ClientError, BotoCoreError, and generic exceptions; returns None
    on failure so main() can decide whether to exit.

### 4. Updated main()
  Replaced:
    github_pat = os.environ.get('GITHUB_PAT')
    if not github_pat: sys.exit("GITHUB_PAT not set.")
  With:
    github_pat = get_github_pat_from_secrets_manager()
    if not github_pat: sys.exit("GitHub PAT could not be retrieved from Secrets Manager.")

### 5. Updated retry_fetch_thread()
  The original was re-reading os.environ.get('GITHUB_PAT') inside the
  background thread on every retry. Changed it to accept github_pat as a
  function parameter passed from main(). This avoids repeated Secrets Manager
  calls and means the PAT is fetched exactly once per script invocation.

---

## Pre-Production Tests Performed

### Test 1: RolesAnywhere session under the logstash user
  Command:
    aws sts get-caller-identity
  Result: SUCCESS
  Confirmed the role aws_ELK is correctly assumed via RolesAnywhere using
  the jenkins_config credential_process.

### Test 2: Existing secret access (baseline check)
  Command:
    aws secretsmanager describe-secret \
      --secret-id arn:aws:secretsmanager:us-east-2:469620122115:secret:
        onprem/logstash/automation-9M8cvG \
      --region us-east-2
  Result: SUCCESS
  Confirmed Secrets Manager access works under the role for a previously
  working secret.

### Test 3: Target GitHub secret access (initial test)
  Command:
    aws secretsmanager describe-secret \
      --secret-id arn:aws:secretsmanager:us-east-2:469620122115:secret:
        github_hsv_internal/itsma/service_elastic_auto_HSV-SRVK6D \
      --region us-east-2
  Result: AccessDeniedException
  Identified ABAC policy mismatch: the role's abac_code (118) was not
  included in the secret's abac_operator tag value ("620 710").

### Test 4: Secret tag fix
  Updated the abac_operator and abac_admin tags on the GitHub secret from
  "620 710" to "620 710 118" to include the aws_ELK role's abac_code value.

  Re-ran:
    aws secretsmanager describe-secret \
      --secret-id arn:aws:secretsmanager:us-east-2:469620122115:secret:
        github_hsv_internal/itsma/service_elastic_auto_HSV-SRVK6D \
      --region us-east-2
  Result: SUCCESS

### Test 5: Root user access to RolesAnywhere config (cron simulation)
  Commands:
    AWS_CONFIG_FILE=/home/logstash/jenkins_config aws sts get-caller-identity
    AWS_CONFIG_FILE=/home/logstash/jenkins_config aws secretsmanager get-secret-value \
      --secret-id arn:aws:secretsmanager:us-east-2:469620122115:secret:
        github_hsv_internal/itsma/service_elastic_auto_HSV-SRVK6D \
      --region us-east-2
  Result: SUCCESS for both commands
  Confirmed that root can read /home/logstash/jenkins_config, the
  certificate, and the private key — and can successfully assume the
  aws_ELK role and retrieve the secret. This validated that the script
  will work under cron as root.

### Test 6: Secret value structure inspection
  Command:
    AWS_CONFIG_FILE=/home/logstash/jenkins_config aws secretsmanager get-secret-value \
      --secret-id arn:aws:secretsmanager:us-east-2:469620122115:secret:
        github_hsv_internal/itsma/service_elastic_auto_HSV-SRVK6D \
      --region us-east-2 \
      --query SecretString --output text | head -c 50
  Result:
    {"user":"service-elasticauto_HSV","password":"ghp_...
  Confirmed the secret is JSON with the PAT stored under the 'password' key.
  This matches one of the keys already handled in the script's parsing logic.

### Test 7: boto3 availability for cron's Python interpreter
  Command:
    /usr/bin/python3 -c "import boto3; print(boto3.__version__)"
  Result: 1.16.38
  Confirmed boto3 is installed for the Python interpreter cron will use.
  No package installation required.

### Test 8: End-to-end script run under root
  Manually executed the updated script as root with valid arguments to
  confirm the full pipeline works: GitHub PAT retrieval from Secrets
  Manager → GitHub file fetch → Elasticsearch query → Ansible playbook.
  Result: SUCCESS
  Verified log output contained:
    "Successfully retrieved GitHub PAT from Secrets Manager."
    "Extracted GitHub PAT from secret using key 'password'."

---

## Deployment

  - Script deployed to /root/agent_restart_automation/agent_restart.py
  - Existing cron schedule unchanged
  - GITHUB_PAT environment variable no longer required and can be removed
    from any cron wrappers or environment files

## Rollback Plan

  Previous version of agent_restart.py is preserved in source control.
  If issues arise, revert the file and re-add the GITHUB_PAT environment
  variable to the cron environment.

## Status: COMPLETED — In Production ✅
```

One small note — your message says "before putting the **watcher** in production" but this update is for the `agent_restart.py` script, not a watcher. I assumed that was a typo and the Jira card is about the script. If the card actually covers both this script update *and* a related watcher change, let me know and I'll add a watcher section.
