import gzip
from datetime import datetime, timedelta , date
import boto3
from bson.json_util import dumps as mongo_dumps
from pymongo import MongoClient
import sys
import logging
import uuid
import json
from typing import Union
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
import pandas

secretsmanager = boto3.client("secretsmanager")
s3_client = boto3.client("s3")

def get_secret_from_arn(secret_arn: str) -> str:
    """Retrieve the Redshift credentials from Secrets Manager
    Args:
        secret_arn (str): The ARN of the secret to retrieve
    Raises:
        e (botocore.exceptions.ClientError): An error encountered when retrieving
                                             the secret from AWS
    Returns:
        str: The JSON encoded credentials object from Secrets Manager
    """
    get_secret_value_response = ""

    try:
        get_secret_value_response = secretsmanager.get_secret_value(SecretId=secret_arn)
    except ClientError as exception:
        if exception.response["Error"]["Code"] == "DecryptionFailureException":
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            raise exception
        elif exception.response["Error"]["Code"] == "InternalServiceErrorException":
            # An error occurred on the server side.
            raise exception
        elif exception.response["Error"]["Code"] == "InvalidParameterException":
            # You provided an invalid value for a parameter.
            raise exception
        elif exception.response["Error"]["Code"] == "InvalidRequestException":
            # You provided a parameter value that is not valid for the current state of the resource.
            raise exception
        elif exception.response["Error"]["Code"] == "ResourceNotFoundException":
            # We can't find the resource that you asked for.
            raise exception
        elif exception.response["Error"]["Code"] == "AccessDeniedException":
            # We don't have access to the resource that you asked for.
            raise exception
    # Decrypts secret using the associated KMS key.
    # Depending on whether the secret is a string or binary, one of these fields will be populated.

    if "SecretString" in get_secret_value_response:
        secret = get_secret_value_response["SecretString"]
        return secret
    else:
        raise Exception("Secret string not found")

def get_uuid_as_str():
    """
    Generate uuid as string
    """
    return str(uuid.uuid4())


def get_glue_options():
    """
    get glue parameters, also state required
    """

    return getResolvedOptions(sys.argv, [
        'SOURCE_TYPE',
        'STEP_FUNC_ID',
        'S3_CONFIG_BUCKET',
        'S3_CONFIG_KEY',
        'S3_LANDING_BUCKET',
        'SNOWFLAKE_CONNECTION_SECRET_ARN',
        'MSSQL_CONNECTION_SECRET_ARN',
        'SOURCE_SERVER_LABEL',
        'OVERRIDE_WATERMARK_START_VALUE',
        'OVERRIDE_WATERMARK_TYPE',
        'OVERRIDE_WATERMARK_STRPTIME_FORMAT',
        'OVERRIDE_MAX_SOURCE_WATERMARK_OFFSET',
        'CONFIG_PARAMETER_01',
        'CONFIG_PARAMETER_02',
        'CONFIG_PARAMETER_03',
        'CONFIG_PARAMETER_04',
        'CONFIG_PARAMETER_05',
        'SNOWFLAKE_ANALYTICS_DATABASE_NAME'
    ])

def load_json_from_s3(bucket, key):
    """
    Loads json file from S3
    """
    logger.info(f'Loading object s3://{bucket}/{key}')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read().decode('utf-8'))


# Generator to iterate MongoDB results in batches
def iterate_by_chunks(cursor, count,chunksize=100000, start_from=0):
    """
    Function to iterate MongoDB results in batches
    """
    chunks = range(start_from, count, int(chunksize))
    num_chunks = len(chunks)

    for i in range(1, num_chunks + 1):
        if i < num_chunks:
            yield cursor[chunks[i - 1] : chunks[i]]
        else:
            yield cursor[chunks[i - 1] : chunks.stop]


# Generator to iterate days between two dates
def date_range(start_date, end_date, step=1):
    """
    Iterates through a range of dates

    Parameters
    ----------
    start_date : str
        The starting day in YYYY-MM-DD format (e.g.: `2021-01-30`)

    end_date : str
        The end point of the sequence (not part of the generated sequence) (e.g.: `2021-02-05`)

    step : int
        Loop increment in number of days

    """
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    if end_date <= start_date:
        raise ValueError("End date should be after start date.")

    current_date = start_date

    while current_date < end_date:
        yield current_date
        current_date = min([current_date + timedelta(days=step), end_date])

def set_fileformat(iteration):
    """
    Output dataframe details to file
    """
    output_path = output_path_format.format(
            S3_LANDING_BUCKET=glue_args['S3_LANDING_BUCKET'],
            system=system,
            source_server=glue_args['SOURCE_SERVER_LABEL'],
            source_database=source_database,
            source_schema=source_collection,
            entity=entity,
            STEP_FUNC_ID=glue_args['STEP_FUNC_ID'],
            GLUE_JOB_ID=GLUE_JOB_ID,
            iteration=iteration,
            output_file_type=output_file_type)

    logger.info(f"output_path {output_path}")
    return output_path

# Function to store records as JSON file
def push_file_to_landing(records,iteration):
    """
    Stores the given list as a gzip compressed JSON file
    """
    output_path = set_fileformat(iteration)
    # Store file as JSON
    if output_file_type == 'json':
        s3_client.put_object(Body=records,Bucket=glue_args['S3_LANDING_BUCKET'],Key=output_path)
    else:
        raise Exception (f"unsupported output_file_type specified: {output_file_type}")
    logger.info("dataframe persisted to s3")


if __name__ == "__main__":
    try:
        # Set up logger
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
       
        # Read glue parameters
        glue_args = get_glue_options()
        logger.info(f"glue_args: {glue_args}")

        GLUE_JOB_ID = get_uuid_as_str()
        logger.info(f"GLUE_JOB_RUN_ID: {GLUE_JOB_ID}")
        
        GLUE_JOB_RUN_AT= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        entity_config = load_json_from_s3(glue_args['S3_CONFIG_BUCKET'], glue_args['S3_CONFIG_KEY'])
        
        config = entity_config.get("source")
        system = config.get("system")
        source_database = config.get("source_database")
        source_collection = config.get( "source_schema")
        entity = config.get("entity")
        output_file_type = config.get("output_file_type",'json')
        output_path_format = config.get("output_path_format")
        BATCH_SIZE = config.get("batch_size", 100000)
        INCREMENTAL = config.get("incremental", False)
        # Read ARN
        secret_value = get_secret_from_arn(glue_args['MSSQL_CONNECTION_SECRET_ARN'])

        mongo_connection_details = json.loads(secret_value)
        logger.info(f"Mongo connection_details Server: {mongo_connection_details['server']}")
        logger.info(f"Mongo connection_details Database: {mongo_connection_details['database']}")
        logger.info(f"Mongo connection_details Username: {mongo_connection_details['username']}")
        #split server into two parts
        tmp_str = str(mongo_connection_details['server'])
        tmp_str = tmp_str.split('@')
        # Set mongo URL from username and password
        MONGODB_URI = tmp_str[0]+mongo_connection_details['username']+':'+mongo_connection_details['password']+'@'+tmp_str[1]
        MONGODB_NAME = str(mongo_connection_details['database'])

        COLLECTION_NAME = config.get("source_schema")
        PREVIOUS_X_DAYS = config.get("previous_x_days",-1)
        
        if PREVIOUS_X_DAYS == -1:
            START_DATE = str(config.get("start_date"))
            END_DATE = str(config.get("end_date",'no_date'))
            if END_DATE == 'no_date' :
                END_DATE = str ((date.today() + timedelta(days=1)).strftime("%Y-%m-%d"))
        else :
            #unit test
            if PREVIOUS_X_DAYS <0 :
                logger.info(f"Previous X days parameter value :{PREVIOUS_X_DAYS} not valid!")
                raise Exception(f"Previous X days parameter value :{PREVIOUS_X_DAYS} not valid!")
            END_DATE = str ((date.today() + timedelta(days=1)).strftime("%Y-%m-%d"))
            START_DATE= str((date.today() + timedelta(days=-PREVIOUS_X_DAYS+1)).strftime("%Y-%m-%d"))
        DAYS_INTERVAL = 1

        MINUTES_INTERVAL = (
            config.get("hours_interval", 1) * 60
            if "hours_interval" in config.keys()
            else config.get("minutes_interval", 60)
        ) 

        # Generate a timestamp for idempotence
        today_str = datetime.now().strftime("%Y%m%d%H%M%S%f")
        logger.info(f"Incremental : {INCREMENTAL}")
        # we only support incremental data load from mongo because of size of data
        if INCREMENTAL :

            # Iterate days between start and end date
            for dt in date_range(
                str(START_DATE),
                str(END_DATE),
                step=DAYS_INTERVAL,
            ):
                date_str = dt.strftime("%Y-%m-%d")
                date_str_file = dt.strftime("%Y%m%d")
                day = datetime.strptime(date_str, "%Y-%m-%d")

                logger.info(f"\n\nLoad from {date_str}\n---------------------\n")

                # Iterate day in minutes intervals
                for lower_limit in range(0, 1440, MINUTES_INTERVAL):
                    upper_limit = min(lower_limit + MINUTES_INTERVAL, 1440)

                    # Styling for task ID
                    hr_lower_limit = int(lower_limit / 60)
                    hr_upper_limit = int(upper_limit / 60)
                    min_lower_limit = int(lower_limit % 60)
                    min_upper_limit = int(upper_limit % 60)

                    # Adding zero padding to task ID
                    z_lower_limit = str(hr_lower_limit).zfill(2) + str(min_lower_limit).zfill(2)
                    z_upper_limit = str(hr_upper_limit).zfill(2) + str(min_upper_limit).zfill(2)

                    logger.info(f"{z_lower_limit} - {z_upper_limit}")

                    # Build interval dates
                    initial_dt = datetime.combine(day, datetime.min.time()) + timedelta(
                        minutes=lower_limit
                    )
                    final_dt = datetime.combine(day, datetime.min.time()) + timedelta(
                        minutes=upper_limit
                    )

                    # Connect to MongoDB
                    mongo_client = MongoClient(MONGODB_URI)
                    mongo_db = mongo_client[MONGODB_NAME]
                    collection = mongo_db[COLLECTION_NAME]

                    # Build aggregation pipeline for MongoDB
                    or_list = []

                    # Check if there is a single field
                    if "date_field" in config.keys():
                        or_list.append(
                            {
                                config["date_field"]: {
                                    "$gte": initial_dt,
                                    "$lt": final_dt,
                                }
                            }
                        )

                    # Check if there are multiple date fields
                    if "date_fields" in config.keys():
                        for date_field in config["date_fields"]:
                            or_list.append(
                                {
                                    date_field: {
                                        "$gte": initial_dt,
                                        "$lt": final_dt,
                                    }
                                }
                            )
                    

                    # Get data from MongoDB
                    or_query ={"$or": or_list}
                    #extra filter
                    if "additional_filter" in config.keys():
                        and_list = []
                        and_list.append(or_query)
                        for filter in config["additional_filter"]:and_list.append(filter)
                        query = {"$and": and_list}
                    else : 
                        query = or_query

                    # Keep track of batch number
                    batch_number = 0
                    logger.info(f"Prepared Statement : {query}")
                
                    collection = list(collection.find(query,batch_size=BATCH_SIZE))
                    count = len(collection)
                    print("Total Raw :",count)
                    
                    #Iterate the query in batches
                    for cursor in iterate_by_chunks(
                        collection, count=count, chunksize=BATCH_SIZE
                    ):

                        # Get data from cursor
                        raw_data = list(cursor)

                        # Check if there are any records
                        if len(raw_data) > 0:

                            logger.info(f"Batch #{batch_number} : Total Raws ({len(raw_data)})")
                            json_str = mongo_dumps(raw_data)
                            json_bytes = json_str.encode("utf-8")
                            #df = pandas.DataFrame(json_bytes)
                            file_pre_fix = f"_{z_lower_limit}-{z_upper_limit}_{date_str}_{today_str}_batch_{batch_number}"
                            # Upload file on landing bucket
                            push_file_to_landing(json_bytes, file_pre_fix)
                        #Next batch of records
                        batch_number += 1
            logger.info("Data Extraction Completed!")
        
    except Exception as ex:
        logger.exception(ex.args)
        raise ex
