# FinOps CloudHealth Manager

## Overview
A toolkit to synchronize AWS account metadata with CloudHealth via the CloudHealth API.

## Workflow Diagram
![Image](https://github.com/user-attachments/assets/59e77c3b-97c4-4d99-8180-3d9e057dfbcd)

## Components
| File              | Purpose                                                                 |
|-------------------|-------------------------------------------------------------------------|
| `aws.py`          | Fetches AWS account metadata and organizational structure               |
| `cloudhealth.py`  | Retrieves CloudHealth account data (`ch_id`, `name`, `tags`, etc.)     |
| `plan.py`         | Generates rename plans for AWS ID-based CloudHealth account names       |
| `apply.py`        | Executes updates via CloudHealth's PUT API                              |

## Installation
```bash
# Install dependencies
pip install -r requirements.txt
```

## Project Structure
```text
.
├── exports/
│   ├── aws_accounts_*.csv
│   └── cloudhealth_accounts_*.csv
├── plan.json
└── finops_account_manager/
    ├── aws.py
    ├── cloudhealth.py
    ├── plan.py
    └── apply.py
```

## Usage

### 1. Fetch AWS Data
```bash
python -m finops_account_manager.aws \
  --profile default \   #or relevant
  --output-dir ./exports \
  --verbose
```

### 2. Fetch CloudHealth Data
```bash
python -m finops_account_manager.cloudhealth \
  --api-key $CH_API_KEY \   #Your CH API Key
  --client-api-id 45374 \   #Zandbak
  --output-dir ./exports
```

### 3. Generate Plan
```bash
python -m finops_account_manager.plan \
  --aws-csv exports/aws_accounts_$(date +%Y%m%d).csv \
  --ch-csv exports/cloudhealth_accounts_$(date +%Y%m%d).csv \
  --out plan.json
```

### 4. Apply Changes
#### Dry Run
```bash
python -m finops_account_manager.apply \
  --plan plan.json \
  --api-key $CH_API_KEY \   #Your CH API Key
  --client-api-id 45374 \   #Zandbak
  --dry-run
```

#### Production Run
```bash
python -m finops_account_manager.apply \
  --plan plan.json \
  --api-key $CH_API_KEY \   #Your CH API Key
  --client-api-id 45374     #Zandbak
```

## Plan Schema
```json
{
  "aws_id": "123456789012",
  "ch_id": "998877666555",
  "old_name": "998877666555",
  "new_name": "core-prod",
  "tags": {
    "ou-level-1": "Platform",
    "ou-level-2": "Core"
  }
}
```
