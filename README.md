# Data Feeds Kit
Handy python tool for joining tables from AWS marketplace to get information on subscribers to aws marketplace products.

## Requirements
- Python 3.6+
    - pandas
    - boto3
- AWS CLI with credentials configured for access to relevant S3 bucket

## Usage
Update the `config.yml` file with the relevant information for the feeds you want to join. Then run the script:
```
$ python3 join_feeds.py
```

Results will be saved to a csv file.