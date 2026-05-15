Before I make the change, I have important questions because this introduces a complication that will block the script from working under cron.

## The Critical Issue: Root vs Logstash User

The `aws sts get-caller-identity` and `describe-secret` commands in `secret.txt` worked because:

1. You were logged in as the **`logstash` user**
2. Inside `/home/logstash/`, there's a `jenkins_config` file that sets up RolesAnywhere credentials via `credential_process`
3. You had `AWS_CONFIG_FILE=jenkins_config` exported (visible in your output)

When the script runs as **`root` via cron**, none of this is true:
- Root's home directory is `/root`, not `/home/logstash`
- Root has no `AWS_CONFIG_FILE` environment variable set
- Cron jobs don't inherit user shell environment variables by default
- Even if root could read `/home/logstash/jenkins_config`, the certificate files (`/etc/pki/tls/certs/ms51-22elkalt01.cer` and `/etc/pki/tls/private/ms51-22elkalt01.key`) likely have permissions that only allow specific users to read them — especially the private key

So before I write the code, I need to know how you want to handle this. There are a few options:

---

## Options for Authentication in the Script

### Option A: Point boto3 explicitly to the jenkins_config file (cleanest if perms allow)
The script sets `AWS_CONFIG_FILE=/home/logstash/jenkins_config` programmatically and uses `profile_name='default'`. This will only work if **root can read the cert/key files**. Let's check that first.

### Option B: Copy/symlink the config to /root and ensure root has cert read access
You'd ensure `/etc/pki/tls/private/ms51-22elkalt01.key` is readable by root (which it usually is by default since root reads everything — unless SELinux is blocking it).

### Option C: Run the cron as the logstash user instead of root
Move the cron entry from root's crontab to `logstash`'s crontab. This is the most "correct" answer if logstash has the permissions the script needs (writing to `/var/log/agent_restart_automation`, running ansible, etc.). But you said the script does ansible-playbook calls and writes to `/var/log/...` — those usually need root.

### Option D: Use a different role/credential path for root
If there's a separate RolesAnywhere setup for root, use that.

---

## My Recommendation

**Option A** is the simplest code change — but it requires verifying that **root can actually execute `aws_signing_helper` with those cert files**. Please run this as root to confirm:

```bash
sudo AWS_CONFIG_FILE=/home/logstash/jenkins_config aws sts get-caller-identity
sudo AWS_CONFIG_FILE=/home/logstash/jenkins_config aws secretsmanager get-secret-value \
  --secret-id arn:aws:secretsmanager:us-east-2:469620122115:secret:github_hsv_internal/itsma/service_elastic_auto_HSV-SRVK6D \
  --region us-east-2
```

If both succeed → I'll proceed with Option A.
If they fail → we need to decide on B, C, or D before I write the code.

---

## Other Things I Need to Confirm

1. **What is the structure of the secret value?** The secret could store the PAT as:
   - A plain string: `"ghp_xxxxxxxxxxxxx"`
   - A JSON object: `{"github_pat": "ghp_xxx"}` or `{"token": "ghp_xxx"}` or `{"username": "...", "password": "ghp_xxx"}`

   This determines how I parse the response. Can you run this to check (or share the structure)?
   ```bash
   aws secretsmanager get-secret-value \
     --secret-id arn:aws:secretsmanager:us-east-2:469620122115:secret:github_hsv_internal/itsma/service_elastic_auto_HSV-SRVK6D \
     --region us-east-2 \
     --query SecretString \
     --output text | head -c 100
   ```
   (only first 100 chars so you don't paste the full secret)

2. **Should the script fail hard if the secret can't be retrieved, or fall back to the `GITHUB_PAT` env var as a backup?** I'd recommend hard failure since that's cleaner, but want to confirm.

3. **Is boto3 already installed on the host?** The script doesn't currently import it, so I need to add it as a dependency.
   ```bash
   /usr/bin/python3 -c "import boto3; print(boto3.__version__)"
   ```

Once you confirm these, I'll make the change with full confidence. I'd rather ask now than write code that breaks silently in cron.
