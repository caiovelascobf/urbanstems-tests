# 🔐 AWS CLI IP Whitelisting for Redshift Access

This guide walks you through:
- ✅ Checking if AWS CLI is installed
- 🔑 Setting up credentials
- 🌐 Automatically adding your **current IP address** to the correct Redshift Security Group
- 🛠 Optional: reusable script + cleanup

---

## ✅ Step 1: Check if AWS CLI is Installed

Open **Git Bash** (recommended) or terminal and run:

```bash
aws --version
```

Expected output:
```
aws-cli/2.15.10 Python/3.11.6 Windows/10 exe/AMD64 prompt/off
```

If not installed, follow [AWS CLI v2 install guide](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).

---

## 🔑 Step 2: Set Up Your AWS CLI Credentials

Use your AWS Access Key and Secret Access Key (generated via the AWS Console):

```bash
aws configure --profile client-ip-updater
```

When prompted:

```
AWS Access Key ID [None]:     <your access key>
AWS Secret Access Key [None]: <your secret key>
Default region name [None]:   us-east-1
Default output format [None]: json
```

---

## 🧪 Step 3: Confirm Authentication

Test your profile:

```bash
aws sts get-caller-identity --profile client-ip-updater
```

Expected output:

```json
{
  "UserId": "AIDA...",
  "Account": "045322402851",
  "Arn": "arn:aws:iam::045322402851:user/caio.velasco@brainforge.ai"
}
```

---

## 🌐 Step 4: Find Redshift Security Group & Port

From the AWS Console:
- Go to **Redshift > Clusters > Your Cluster**
- Confirm:
  - **Security Group ID**: `sg-de3c2ba3`
  - **Port**: `5439` (default Redshift)

Ensure the SG has inbound rules for:
- Type: Redshift
- Protocol: TCP
- Port Range: 5439

---

## 🧠 Step 5: Add Your Current IP to the Security Group

In Git Bash, run:

```bash
MY_IP=$(curl -s https://checkip.amazonaws.com)

aws ec2 authorize-security-group-ingress \
  --group-id sg-de3c2ba3 \
  --protocol tcp \
  --port 5439 \
  --cidr ${MY_IP}/32 \
  --profile client-ip-updater
```

> If the rule already exists, you’ll see:
> `An error occurred (InvalidPermission.Duplicate) ... already exists`  
> ✅ That means you’re already whitelisted.

---

## 🧼 Optional: Remove IP from Previous Security Group

If you previously used another SG (like `sg-04627d4dcd7a552da`) and want to clean it up:

```bash
aws ec2 revoke-security-group-ingress \
  --group-id sg-04627d4dcd7a552da \
  --protocol tcp \
  --port 5439 \
  --cidr ${MY_IP}/32 \
  --profile client-ip-updater
```

---

## 🔁 Optional: Reusable Script

Save the following as `whitelist_my_ip.sh`:

```bash
#!/bin/bash

SG_ID="sg-de3c2ba3"
PORT=5439
PROFILE="client-ip-updater"
MY_IP=$(curl -s https://checkip.amazonaws.com)

aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp \
  --port $PORT \
  --cidr ${MY_IP}/32 \
  --profile $PROFILE

echo "✅ IP ${MY_IP} added to SG ${SG_ID} on port ${PORT}"
```

Make it executable:

```bash
chmod +x whitelist_my_ip.sh
```

Use it whenever your IP changes:

```bash
./whitelist_my_ip.sh
```

---

## ✅ You're Done!

You now have a secure, CLI-based way to update your Redshift IP whitelist:

- No more waiting on manual updates
- Fully self-managed with Git Bash or VS Code
- Organized under the correct SG (`sg-de3c2ba3`) with your name labeled
