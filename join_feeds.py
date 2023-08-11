import os
import yaml
import boto3
import shutil
import pandas as pd


# get files below path
def compile_file_list(path):
    file_list = []
    for root, _, files in os.walk(path):
        for name in files:
            file_list.append(os.path.join(root, name))

    # filter out non csvs
    file_list = [file for file in file_list if ".csv" in file]

    return file_list


# merge csvs into single df
def join_files_to_df(path, dup_drop=[]):
    file_list = compile_file_list(path)

    df = pd.concat(map(pd.read_csv, file_list), ignore_index=True)
    if len(dup_drop) > 0:
        df = df.drop_duplicates(subset=dup_drop)
    return df


# delete given directory path and contents
def delete_directory(directory_path):
    try:
        shutil.rmtree(directory_path)
        print(f"Directory '{directory_path}' has been successfully deleted.")
    except OSError as e:
        print(f"Error deleting directory '{directory_path}': {e}")


# download files recursively from s3
def download_s3_folder(bucket_name, s3_folder, local_path):
    s3 = boto3.client("s3")

    # List all objects in the S3 folder
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder)

    # Recursively download each object
    for obj in objects.get("Contents", []):
        s3_key = obj["Key"]
        local_file_path = os.path.join(local_path, os.path.relpath(s3_key, s3_folder))

        # Skip directories
        if s3_key.endswith("/"):
            continue

        # Create local directory structure if needed
        local_dir = os.path.dirname(local_file_path)
        os.makedirs(local_dir, exist_ok=True)

        # Download the file
        s3.download_file(bucket_name, s3_key, local_file_path)

        print(f"Downloaded: s3://{bucket_name}/{s3_key} to {local_file_path}")


# extract user info from data feeds and join to product name
def get_user_info(base_path, product_name):
    tables = [
        "AccountFeed_V1",
        "AddressFeed_V1",
        "AgreementFeed",
        "OfferFeed_V1",
        "OfferProductFeed_V1",
        "ProductFeed_V1",
    ]
    df_dict = {}
    for table in tables:
        df_dict[table] = join_files_to_df(f"{base_path}/{table}/")

    df_dict["acct_ag"] = pd.merge(
        df_dict["AgreementFeed"],
        df_dict["AccountFeed_V1"],
        left_on="acceptor_account_id",
        right_on="account_id",
        how="inner",
    )

    df_dict["prod_off_prod"] = pd.merge(
        df_dict["ProductFeed_V1"],
        df_dict["OfferProductFeed_V1"],
        on="product_id",
        how="inner",
    )
    df_dict["prod_off"] = pd.merge(
        df_dict["prod_off_prod"], df_dict["OfferFeed_V1"], on="offer_id", how="inner"
    )
    intermediate = pd.merge(
        df_dict["prod_off"],
        df_dict["acct_ag"],
        left_on="offer_id",
        right_on="origin_offer_id",
        how="inner",
    )[["title", "aws_account_id", "mailing_address_id"]]

    result = pd.merge(
        intermediate,
        df_dict["AddressFeed_V1"],
        how="inner",
        left_on="mailing_address_id",
        right_on="address_id",
    ).drop_duplicates(subset=["title", "aws_account_id"])
    result = result.drop(columns=["address_id", "mailing_address_id"])

    return result.loc[result["title"] == product_name]


###############################################
# Specify options in config.yml file
###############################################
config = yaml.safe_load(open("config.yml"))

# optionally download data feeds from s3
if config["download_data_feeds"]:
    download_s3_folder(
        config["s3_bucket_name"], config["s3_folder"], config["local_feed_path"]
    )

# join feeds to match product name to user information
user_info = get_user_info(config["local_feed_path"], config["product_name"])

# output csv of user info
user_info.to_csv(f"output_{config['product_name'].replace(' ', '_')}.csv", index=False)

# optionally delete data feeds after outputting csv
if config["cleanup_feeds"]:
    delete_directory(config["local_feed_path"])
