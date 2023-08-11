# Data Feeds Kit
Handy python tool for joining tables from AWS marketplace to get information on subscribers to aws marketplace products.

## Requirements
- Python 3.6+
    - pandas
    - boto3
- AWS CLI with credentials configured for access to relevant S3 bucket

## Usage
Update the `config.yml` file with the relevant information for the feeds you want to join:

```
s3_bucket_name: "imi-subscribers" # name of the bucket where the feeds are stored
s3_folder: "" # folder in the bucket where the feeds are stored
local_feed_path: "feeds" # folder where the feeds will be downloaded to
product_name: "Integrated Methane Inversion" # name of the product to get subscribers for
download_data_feeds: True # whether to download the data feeds from S3
cleanup_feeds: True # whether to delete the downloaded feeds after outputting csv
```

Then run the script:
```
$ python process_feeds.py
```

Results will be saved to an output csv file.