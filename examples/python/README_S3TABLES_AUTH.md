# Redshift TPC-DS Script with S3 Tables Support

## Overview

The `redshift-tpcds.py` script has been enhanced to support two authentication methods:

1. **Traditional Authentication**: Username and password (original method)
2. **IAM/Federated Authentication**: For S3 Tables access (new method)

## Authentication Methods

### 1. Traditional Username/Password Authentication

Used for standard Redshift clusters:

```bash
python redshift-tpcds.py \
  -f /path/to/sql/files \
  -h <redshift-endpoint> \
  -o /output/directory \
  -u <username> \
  -p <password> \
  -d <database> \
  -s <schema>
```

### 2. IAM/Federated Authentication (for S3 Tables)

Used for accessing Redshift with S3 Tables via Federated user:

```bash
python redshift-tpcds.py \
  -f /path/to/sql/files \
  -h <redshift-endpoint> \
  -o /output/directory \
  -c <cluster-identifier> \
  -d <database> \
  -s <schema> \
  --iam
```

## Parameters

| Parameter | Short | Required | Description |
|-----------|-------|----------|-------------|
| --sqlfiles | -f | Yes | Path to directory containing SQL files |
| --host | -h | Yes | Redshift cluster endpoint |
| --output | -o | No | Output directory for results (default: ./) |
| --cluster_identifier | -c | Yes (for IAM) | Redshift cluster identifier |
| --username | -u | Yes (non-IAM) | Username for authentication |
| --password | -p | Yes (non-IAM) | Password for authentication |
| --database | -d | No | Database name (default: dev) |
| --schema | -s | No | Schema name (default: iceberg) |
| --iam | | No | Enable IAM authentication |

## Prerequisites for IAM Authentication

When using `--iam` flag for S3 Tables access:

1. **AWS Credentials**: Ensure your environment has valid AWS credentials:
   - IAM Role attached to EC2 instance
   - AWS credentials in `~/.aws/credentials`
   - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

2. **IAM Permissions**: The IAM role/user must have:
   - `redshift:GetClusterCredentials` permission
   - Permissions to access S3 Tables
   - Network access to the Redshift cluster

3. **Python Library**: Ensure `redshift_connector` library is installed:
   ```bash
   pip install redshift-connector
   ```

## How It Works

### IAM Authentication Flow

When `--iam` flag is used:

1. The script uses the AWS SDK to obtain temporary credentials
2. Credentials are automatically retrieved from:
   - EC2 instance IAM role (recommended for EC2)
   - AWS credentials file
   - Environment variables
3. The connection uses these temporary credentials instead of username/password
4. No password storage or management required

### Key Changes in the Code

```python
# IAM Authentication
if USE_IAM:
    conn = redshift_connector.connect(
        host=HOST,
        database=DATABASE,
        port=5439,
        iam=True,
        cluster_identifier=CLUSTER_IDENTIFIER,
        timeout=300
    )
# Traditional Authentication
else:
    conn = redshift_connector.connect(
        host=HOST,
        database=DATABASE,
        port=5439,
        user=USERNAME,
        password=PASSWORD,
        timeout=300
    )
```

## Example Usage

### Example 1: Testing with S3 Tables (IAM Authentication)

```bash
python redshift-tpcds.py \
  -f /home/ec2-user/environment/emr-on-eks-benchmark/spark-sql-perf/src/main/resources/tpcds_2_4_redshift \
  -h myredshift.123456789012.us-east-1.redshift.amazonaws.com \
  -o /home/ec2-user/environment/redshift \
  -c my-redshift-cluster \
  -d dev \
  -s s3tables_schema \
  --iam
```

### Example 2: Traditional Authentication

```bash
python redshift-tpcds.py \
  -f /home/ec2-user/environment/emr-on-eks-benchmark/spark-sql-perf/src/main/resources/tpcds_2_4_redshift \
  -h myredshift.123456789012.us-east-1.redshift.amazonaws.com \
  -o /home/ec2-user/environment/redshift \
  -u awsuser \
  -p mypassword \
  -d dev \
  -s iceberg
```

## Output

The script generates a CSV file with results:
- Location: `{OUTPUT}/result_{SCHEMA}.csv`
- Columns: SQL filename, ExecuteTime (seconds), Rows returned

## Troubleshooting

### IAM Authentication Issues

1. **"Unable to load credentials"**
   - Verify AWS credentials are configured
   - Check IAM role is attached to EC2 instance
   - Ensure credentials file exists: `~/.aws/credentials`

2. **"Access Denied"**
   - Verify IAM permissions include `redshift:GetClusterCredentials`
   - Check security group allows connection to Redshift
   - Ensure cluster identifier is correct

3. **"Timeout"**
   - Verify network connectivity to Redshift cluster
   - Check VPC security groups and network ACLs
   - Increase timeout value if needed

### General Issues

- Ensure SQL files are in the specified directory
- Verify schema name matches your Redshift schema
- Check database name is correct

## Security Best Practices

1. **Use IAM Authentication** when possible - no password management
2. **Avoid hardcoding credentials** in scripts
3. **Use EC2 IAM roles** for automatic credential rotation
4. **Restrict IAM permissions** to minimum required
5. **Enable CloudTrail** for authentication auditing

## Compatibility

- Python 3.6+
- redshift_connector library
- AWS credentials (for IAM authentication)
- Network access to Redshift cluster