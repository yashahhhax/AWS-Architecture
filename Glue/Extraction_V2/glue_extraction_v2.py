"""
Script to extract data from source system and land as files
"""
import sys
import logging
import uuid
from datetime import datetime, timedelta
import json
from typing import Union
import pymssql
import psycopg2
import snowflake.connector
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

secretsmanager = boto3.client("secretsmanager")
s3_client = boto3.client("s3")

#Valid values
VALID_EXTRACTION_TYPES = ['incremental', 'full', 'health_check']
VALID_OUTPUT_FILE_TYPES = ['csv','json','parquet']
VALID_WATERMARK_TYPES = ['integer','datetime']

# Default table to capture glue job information
DEFAULT_GLUE_JOB_INFO_TABLE = 'HISTORIC.SUPPORT_GLUE_JOB_INFO'

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


def prioritise_list(config_list, priority_attribute_name):
    """
    prioritise list of entries
    """
    prioritised_dict = {}
    for item in config_list:
        enabled = get_config_attribute(item, "enabled", mandatory_attribute=False, default_return_value=True)
        if enabled:
            key_name = get_config_attribute(item, priority_attribute_name)
            prioritised_dict[key_name] = item

    return prioritised_dict


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


def get_config_attribute(config, attribute_name,
    mandatory_attribute=True,
    mandatory_attribute_value=True,
    valid_values=None,
    default_return_value=""):
    """
    retrieves a configuration attribute, also checking whether:
        .it's inclusion is mandatory
        .it's value's inclusion is mandatory
        .it's value needs to conform to a list of valid values
        .a default value should be returned if an optional value is not present
    """
    attribute_present = not config.get(attribute_name) is None
    attribute_value_present = attribute_present and len((str(config.get(attribute_name)))) > 0

    if mandatory_attribute and not attribute_present:
        raise Exception(f"mandatory attribute is not present - {attribute_name}")

    # if attrbiute isn't mandatory, the related attrbiute value is automatically not mandatory
    if not mandatory_attribute:
        mandatory_attribute_value = not mandatory_attribute_value

    if mandatory_attribute_value and not attribute_value_present:
        raise Exception(f"mandatory attribute value is not present - {attribute_name}")

    if attribute_present and attribute_value_present:
        attribute_value = config[attribute_name]
        # if valid_values string array has been passed in and retrieved attribute_value is not present, raise an exception
        if valid_values and len(valid_values) > 0 and not attribute_value in valid_values:
            raise Exception(f"attribute value does not contain a valid value - {attribute_name} - valid values {valid_values}")

        logger.info(f'attribute present {attribute_name} and attribute value present {attribute_value}')
        return attribute_value

    logger.info(f'default attribute: {default_return_value} for attribute name {attribute_name}')
    return default_return_value


def get_secret_value_as_json(secret_arn):
    """
    Retrieve secret value and load into dictionary
    """
    secret_value = get_secret_from_arn(secret_arn)

    return json.loads(secret_value)


def get_target_connection():
    """
    returns target database connection
    """
    secret_value = get_secret_from_arn(glue_args['SNOWFLAKE_CONNECTION_SECRET_ARN'])

    snowflake_connection_details = json.loads(secret_value)

    logger.info(f"target connection_details user: {snowflake_connection_details['user']}")
    logger.info(f"target connection_details account: {snowflake_connection_details['account']}")
    logger.info(f"target connection_details warehouse: {snowflake_connection_details['warehouse']}")
    logger.info(f"target connection_details database: {snowflake_connection_details['database']}")
    logger.info(f"target connection_details schema: {snowflake_connection_details['schema']}")
    logger.info(f"target connection_details role: {snowflake_connection_details['role']}")

    # return snowflake.connector.connect(
    #     user=snowflake_connection_details['user'],
    #     password=snowflake_connection_details['password'],
    #     account=snowflake_connection_details['account'],
    #     warehouse=snowflake_connection_details['warehouse'],
    #     database=snowflake_connection_details['database'],
    #     schema=snowflake_connection_details['schema'],
    #     role=snowflake_connection_details['role']
    # )
    if 'passphrase' in snowflake_connection_details:
        #passphrase
        passphrase=snowflake_connection_details['passphrase'].encode('utf-8')
        #private key
        pk_str:str=f"-----BEGIN ENCRYPTED PRIVATE KEY-----\n{snowflake_connection_details['privatekey']}\n-----END ENCRYPTED PRIVATE KEY-----"
        pk_byte = pk_str.encode('utf-8')
        
        p_key= serialization.load_pem_private_key(
            pk_byte,
            password=passphrase,
            backend=default_backend()
        )
        #decription
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
            )
        
        return snowflake.connector.connect(
            user=snowflake_connection_details['user'],
            account=snowflake_connection_details['account'],
            private_key=pkb,
            warehouse=snowflake_connection_details['warehouse'],
            database=snowflake_connection_details['database'],
            schema=snowflake_connection_details['schema'],
            role=snowflake_connection_details['role']
            )
    else:
        raise Exception("Not valid connection details") 
    

def get_source_connection_odbc():
    """
    returns odbc database connection
    """
    
    secret_value = get_secret_from_arn(glue_args['MSSQL_CONNECTION_SECRET_ARN'])

    mssql_connection_details = json.loads(secret_value)
    
    logger.info(f"odbc source connection_details server: {mssql_connection_details['server']}")
    logger.info(f"odbc source connection_details username: {mssql_connection_details['username']}")
    logger.info(f"odbc source connection_details database: {mssql_connection_details['database']}")
    
    return pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + mssql_connection_details['server'] + ';DATABASE=' + mssql_connection_details['database'] + ';UID=' + mssql_connection_details['username'] + ';PWD= '+mssql_connection_details['password'] + ' ')

def get_source_connection_mysql():
    secret_value = get_secret_from_arn(glue_args['MSSQL_CONNECTION_SECRET_ARN'])

    mssql_connection_details = json.loads(secret_value)
    endpoint = mssql_connection_details['endpoint']
    username = mssql_connection_details['username']
    password = mssql_connection_details['password']
    database = mssql_connection_details['database']

    return mysql.connector.connect(user=username, password=password,
                              host=endpoint, database=database)

def get_source_connection():
    """
    returns source database connection
    """
    secret_value = get_secret_from_arn(glue_args['MSSQL_CONNECTION_SECRET_ARN'])

    mssql_connection_details = json.loads(secret_value)

    logger.info(f"source connection_details server: {mssql_connection_details['server']}")
    logger.info(f"source connection_details username: {mssql_connection_details['username']}")
    logger.info(f"source connection_details database: {mssql_connection_details['database']}")

    # return pymssql.connect(
    #     mssql_connection_details['server'],
    #     mssql_connection_details['username'],
    #     mssql_connection_details['password'],
    #     mssql_connection_details['database']
    # )
    
    
    return psycopg2.connect(
        user=mssql_connection_details['username'],
        password=mssql_connection_details['password'],
        host= mssql_connection_details['server'],
        port="5432",
        database=mssql_connection_details['database']
    )


def statement_from_parts(statement_parts):
    """
    combines multiple statement parts into one statement
    """
    return "".join(statement_parts)


def get_scalar_value(conn, statement):
    """
    Retrieve scalar value from database - assumes statement has been configured accordingly
    """
    with conn.cursor() as cursor:

        sql = statement

        cursor.execute(sql)
        value = cursor.fetchone()[0]

        logger.info(f'get_scalar_value {value}')
        return value


def get_watermark_start (conn):
    """
    Read sql statement from entity config and execute against target database
    """
    statement = statement_from_parts(get_config_attribute(watermark_start_config, "statement"))
    value_type = get_config_attribute(watermark_start_config, "type", valid_values=VALID_WATERMARK_TYPES)
    strptime_format = get_config_attribute(watermark_start_config, "strptime_format")
    default_value = get_config_attribute(watermark_start_config, "default")

    #TODO: revist this?
    value = get_scalar_value(conn, statement) or default_value

    if value_type == 'datetime':
        value = datetime.strptime(value, strptime_format)

    return value, value_type, strptime_format


def get_chunk_details():
    """
    Read chunk configuration details
    """
    chunk_config = get_config_attribute(source_config, "chunk")
    chunk_size = get_config_attribute(chunk_config, "size")

    if glue_args['OVERRIDE_MAX_SOURCE_WATERMARK_OFFSET'] == 'NOTSET':
        max_source_watermark_offset = get_config_attribute(chunk_config, "max_source_watermark_offset")
    else:
        max_source_watermark_offset = int(glue_args['OVERRIDE_MAX_SOURCE_WATERMARK_OFFSET'])

    return chunk_size, max_source_watermark_offset


def prepare_df_output(dataframe):
    """
    Validate source to target column lists, inject further meta/lineage data
    """
    # uppercase column names and remove spaces
    dataframe.columns = [x.upper().replace(' ','') for x in dataframe.columns]

    # validate source and target columns
    source_columns = ",".join(dataframe.columns)
    if source_columns != TARGET_COLUMNS:
        not_in_target = [item for item in source_columns if item not in TARGET_COLUMNS]
        not_in_source = [item for item in TARGET_COLUMNS if item not in source_columns]
        logger.warning(f"Missing from source columns: {not_in_source}")
        logger.warning(f"Extra columns in source: {not_in_target}")
        raise Exception(f"source columns do not match target columns. source_columns: {source_columns} target_columns {TARGET_COLUMNS}")

    # inject step function id and glue job id
    dataframe['AWS_STEP_FUNC_ID'] = glue_args['STEP_FUNC_ID']
    dataframe['AWS_GLUE_JOB_ID'] = GLUE_JOB_ID
    dataframe['AWS_GLUE_JOB_RUN_AT'] = GLUE_JOB_RUN_AT

    # 'Handle' null value output
    dataframe = dataframe.fillna(value="NULL_VALUE")

    # convert all values to string type - (primarily for parquet)
    dataframe=dataframe.astype(str)

    return dataframe


def write_df_output(dataframe, iteration):
    """
    Output dataframe details to file
    """
    output_path = output_path_format.format(
            S3_LANDING_BUCKET=glue_args['S3_LANDING_BUCKET'],
            system=system,
            source_server=glue_args['SOURCE_SERVER_LABEL'],
            source_database=source_database,
            source_schema=source_schema,
            entity=entity,
            STEP_FUNC_ID=glue_args['STEP_FUNC_ID'],
            GLUE_JOB_ID=GLUE_JOB_ID,
            iteration=iteration,
            output_file_type=output_file_type)

    logger.info(f"output_path {output_path}")

    if output_file_type == 'csv':
        dataframe.to_csv(path_or_buf=output_path, sep=',',na_rep='',quotechar='"', line_terminator='\n', index=False)
    elif output_file_type == 'json':
        dataframe.to_json(path_or_buf=output_path, orient='records', date_format='iso')
    elif output_file_type == 'parquet':
        dataframe.to_parquet(path=output_path)
    else:
        raise Exception (f"unsupported output_file_type specified: {output_file_type}")

    logger.info("dataframe persisted to s3")


def calculate_upper_watermark(lower_watermark_value:Union[int, datetime],
    max_source_watermark:Union[int, datetime],
    chunk_size:Union[int, timedelta]):
    """
    Calculate next watermark value based upon watermark type and chunk size

    Note. for datetime watermark the offet is handled in days
    """
    if WATERMARK_TYPE == 'integer':
        return lower_watermark_value + chunk_size
    elif WATERMARK_TYPE == 'datetime':
        upper_watermark_value = lower_watermark_value + timedelta(days=chunk_size)

        #ensure upper watermark doesn't exceed max source watermark
        if upper_watermark_value > max_source_watermark:
            upper_watermark_value = max_source_watermark
        return upper_watermark_value
    else:
        raise Exception (f"unsupported WATERMARK_TYPE specified: {WATERMARK_TYPE}")


def calculate_max_source_watermark(lower_watermark_value:Union[int, datetime],
    max_source_watermark_offset:Union[int, datetime]):
    """
    Calculates maximum source watermark to read up to, based upon the watermark type and the
    defined offset

    Note. for datetime watermark the offet is handled in days
    """
    if WATERMARK_TYPE == 'integer':
        return lower_watermark_value + max_source_watermark_offset

    max_source_watermark = lower_watermark_value + timedelta(days=max_source_watermark_offset)
    # no need to go into the far future
    if max_source_watermark > datetime.now():
        return datetime.now() + timedelta(days=1)  # allow for timezones
    return max_source_watermark


def prepare_statement_format():
    """
    Prepare statement format containing relevant items that can be injected into sql statements
    prior to execution if needed.

    Note. 1. replace any config_parameter_01 'NOTSET' values with and empty string
          2. watermark related details will be added during the running of this process/script
    """
    statement_format = {
            'CONFIG_PARAMETER_01':glue_args['CONFIG_PARAMETER_01'] if glue_args['CONFIG_PARAMETER_01'] != "NOTSET" else "",
            'CONFIG_PARAMETER_02':glue_args['CONFIG_PARAMETER_02'] if glue_args['CONFIG_PARAMETER_02'] != "NOTSET" else "",
            'CONFIG_PARAMETER_03':glue_args['CONFIG_PARAMETER_03'] if glue_args['CONFIG_PARAMETER_03'] != "NOTSET" else "",
            'CONFIG_PARAMETER_04':glue_args['CONFIG_PARAMETER_04'] if glue_args['CONFIG_PARAMETER_04'] != "NOTSET" else "",
            'CONFIG_PARAMETER_05':glue_args['CONFIG_PARAMETER_05'] if glue_args['CONFIG_PARAMETER_05'] != "NOTSET" else "",
            'system':system,
            'source_server':glue_args['SOURCE_SERVER_LABEL'],
            'source_database':source_database,
            'source_schema':source_schema,
            'entity':entity
        }

    logger.info(f"prepare_statement_format: {statement_format}")

    return statement_format


def prepare_extraction_statement_incremental(step_value,
    lower_watermark_value:Union[int, datetime],
    upper_watermark_value:Union[int, datetime]):
    """
    Prepare sql statement for incremental extraction
    """
    if WATERMARK_TYPE == 'integer':
        #add/update watermark details to statement_format
        #dictionary prior to sql statement execution
        statement_format["lower_watermark_value"] = lower_watermark_value
        statement_format["upper_watermark_value"] = upper_watermark_value
    elif WATERMARK_TYPE == 'datetime':
        statement_format["lower_watermark_value"] = lower_watermark_value.strftime(WATERMARK_STRPTIME_FORMAT)
        statement_format["upper_watermark_value"] = upper_watermark_value.strftime(WATERMARK_STRPTIME_FORMAT)
    else:
        raise Exception (f"unsupported WATERMARK_TYPE specified: {WATERMARK_TYPE}")

    return prepare_extraction_statement(step_value)


def prepare_extraction_statement(step_value):
    """
    Prepare sql statement for full extraction
    """
    statement = statement_from_parts(get_config_attribute(step_value, "statement"))

    return statement.format(**statement_format)


def execute_extraction_preparation_steps(cursor):
    """
    Read and execute extraction preparation steps

    Note.   chunk_size and watermark_value are not relevant here as extraction preparation
            steps are executed once for a given run
    """
    extraction_preparation_steps = source_config.get("extraction_preparation_steps")

    dict_of_extraction_prep_aration_steps = prioritise_list(
        extraction_preparation_steps,
        'priority')

    for step_key, step_value in sorted(dict_of_extraction_prep_aration_steps.items()):
        logger.info(f'step_key {step_key}')

        statement = prepare_extraction_statement(step_value)

        logger.info(f"prepared extraction_preparation statement: {statement}")
        
        cursor.execute(statement)


def execute_extraction_steps_incremental(cursor):
    """
    Read and execute extraction steps
    """
    lower_watermark_value = WATERMARK_START_VALUE
    logger.info(f'watermark {lower_watermark_value}')

    chunk_size, max_source_watermark_offset = get_chunk_details()
    
    max_source_watermark = calculate_max_source_watermark(lower_watermark_value,
        max_source_watermark_offset)

    logger.info(f'max_source_watermark {max_source_watermark}')

    extraction_steps = source_config.get("extraction_steps")

    dict_of_extraction_steps = prioritise_list(extraction_steps, 'priority')

    current_iteration = 0

    while lower_watermark_value < max_source_watermark:

        for step_key, step_value in sorted(dict_of_extraction_steps.items()):
            logger.info(f'step_key {step_key}')

            upper_watermark_value = calculate_upper_watermark(lower_watermark_value,
                max_source_watermark,
                chunk_size)

            statement = prepare_extraction_statement_incremental(step_value,
                lower_watermark_value,
                upper_watermark_value)

            logger.info(f"prepared extraction statement: {statement}")

            persist = get_config_attribute(step_value, "persist", mandatory_attribute=False, default_return_value=None)
            if persist is None:
                cursor.execute(statement)
            else:
                dataframe = pd.read_sql(statement, source_conn)

                data_available = len(dataframe.index) > 0
                if data_available:

                    dataframe=prepare_df_output(dataframe)

                    write_df_output(dataframe, current_iteration)
                else:
                    logger.info("no data for chunk")

        lower_watermark_value = upper_watermark_value

        logger.info(f'updated watermark {lower_watermark_value}')

        current_iteration += 1

        logger.info(f'current_iteration {current_iteration}')


def execute_extraction_steps_full(cursor):
    """
    Read and execute extraction steps
    """
    extraction_steps = source_config.get("extraction_steps")

    dict_of_extraction_steps = prioritise_list(extraction_steps, 'priority')

    current_iteration = 0

    for step_key, step_value in sorted(dict_of_extraction_steps.items()):
        logger.info(f'step_key {step_key}')

        statement = prepare_extraction_statement(step_value)

        logger.info(f"prepared extraction statement: {statement}")  

        persist = get_config_attribute(step_value, "persist", mandatory_attribute=False)
        if persist is None:
            cursor.execute(statement)
        else:
            dataframe = pd.read_sql(statement, source_conn)

            data_available = len(dataframe.index) > 0
            if data_available:

                dataframe=prepare_df_output(dataframe)

                write_df_output(dataframe, current_iteration)
            else:
                logger.info("no data")

        logger.info(f'current_iteration {current_iteration}')


def prepare_extraction_statement_health_check(step_value, row):
    """
    Prepare sql statement for incremental extraction
    """
    statement_format["TABLE_ID"] = row['TABLE_ID']
    statement_format["DATABASE_NAME"] = row['DATABASE_NAME']
    statement_format["SCHEMA_NAME"] = row['SCHEMA_NAME']
    statement_format["TABLE_NAME"] = row['TABLE_NAME']
    statement_format["SQL_STATEMENT"] = row['SQL_STATEMENT']

    return prepare_extraction_statement(step_value)


def execute_extraction_steps_health_check(cursor, meta_data_dataframe):
    """
    Read and execute extraction health check steps
    """
    extraction_steps = source_config.get("extraction_steps")

    dict_of_extraction_steps = prioritise_list(extraction_steps, 'priority')

    for current_iteration, row in meta_data_dataframe.iterrows():

        for step_key, step_value in sorted(dict_of_extraction_steps.items()):
            logger.info(f'step_key {step_key}')

            statement = prepare_extraction_statement_health_check(step_value, row)

            logger.info(f"prepared extraction health_check statement: {statement}")  

            persist = get_config_attribute(step_value, "persist", mandatory_attribute=False)
            if persist is None:
                cursor.execute(statement)
            else:
                dataframe = pd.read_sql(statement, source_conn)

                data_available = len(dataframe.index) > 0
                if data_available:

                    dataframe=prepare_df_output(dataframe)

                    write_df_output(dataframe, current_iteration)
                else:
                    logger.info("no data")

            logger.info(f'current_iteration {current_iteration}')


def read_health_check_threshold_data():
    """
    Read meta data from snowflake containing threshold configuration
    """
    statement = f"""
                SELECT TABLE_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, SQL_STATEMENT
                FROM {health_check_threshold_table}
                WHERE TABLE_TYPE = 'source'
                AND ENABLED = true
                AND SERVER_NAME = '{glue_args['SOURCE_SERVER_LABEL']}'
                """

    meta_data_dataframe = pd.read_sql(statement, target_conn)
    
    logger.info(f"meta_data_dataframe: {meta_data_dataframe}")

    # print dataframe.
    return meta_data_dataframe


def extraction():
    """
    Perform extraction
    """
    with source_conn.cursor() as cursor:

        execute_extraction_preparation_steps(cursor)

        if extraction_type == "incremental":
            execute_extraction_steps_incremental(cursor)
        elif extraction_type == "full":
            execute_extraction_steps_full(cursor)
        elif extraction_type == "health_check":
            meta_data_dataframe = read_health_check_threshold_data()

            execute_extraction_steps_health_check(cursor, meta_data_dataframe)
        else:
            raise Exception (f"unsupported extraction_type specified: {extraction_type}")


def prepare_log_glue_info_statement():
    """
    Prepare sql statement for logging glue job info data

    Added in code to limit changes to config files although this could be implemented via config gile in future if needed
    """
    statement = """
                INSERT INTO {glue_job_info_table} 
                (entity, source_server, data, created_at, AWS_LOAD_GLUE_JOB_ID, AWS_STEP_FUNC_ID) 
                select 
                    '{entity}' 
                    ,'{source_server}' 
                    ,parse_json($${data}$$)
                    ,getdate() 
                    ,'{GLUE_JOB_ID}'
                    ,'{STEP_FUNC_ID}'
                """

    # prepare data to log
    json_data = json.dumps(statement_format)

    statement_format["data"] = json_data
    statement_format["glue_job_info_table"] = glue_job_info_table
    statement_format["STEP_FUNC_ID"] = glue_args['STEP_FUNC_ID']
    statement_format["GLUE_JOB_ID"] = GLUE_JOB_ID

    return statement.format(**statement_format)


def log_glue_info():
    """
    Log glue details
    """
    with target_conn.cursor() as cursor:

        statement = prepare_log_glue_info_statement()

        logger.info(f"prepared log_glue_info statement: {statement}")  

        cursor.execute(statement)


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

        # Load entity level configuration details
        entity_config = load_json_from_s3(glue_args['S3_CONFIG_BUCKET'], glue_args['S3_CONFIG_KEY'])

        glue_job_info_table = get_config_attribute(entity_config, "glue_job_info_table", mandatory_attribute=False, default_return_value=DEFAULT_GLUE_JOB_INFO_TABLE)

        watermark_start_config = entity_config.get("watermark_start")
        source_config = entity_config.get("source")

        system = get_config_attribute(source_config, "system")
        source_database = get_config_attribute(source_config, "source_database")
        source_schema = get_config_attribute(source_config, "source_schema")
        entity = get_config_attribute(source_config, "entity")

        extraction_type = get_config_attribute(source_config, "extraction_type", valid_values=VALID_EXTRACTION_TYPES)
        
        sql_source_type = glue_args['SOURCE_TYPE']
        
        output_file_type = get_config_attribute(source_config, "output_file_type", valid_values=VALID_OUTPUT_FILE_TYPES)
        output_path_format = get_config_attribute(source_config, "output_path_format")

        target_config = entity_config.get("target")

        target_column_list = get_config_attribute(target_config, "target_column_list")
        # read target columns, also strip spaces and uppercase
        TARGET_COLUMNS = ",".join(target_column_list.keys()).replace(' ', '').upper()

        # Get Target Database connection (Snowflake)
        #   needed for log_glue_info details and optionally reading watermark details from target database
        target_conn = get_target_connection()

        if extraction_type == "incremental":
            # Get starting watermark if not overridden
            if glue_args['OVERRIDE_WATERMARK_START_VALUE'] == "NOTSET":
                WATERMARK_START_VALUE, WATERMARK_TYPE, WATERMARK_STRPTIME_FORMAT = get_watermark_start(target_conn)
            else:
                WATERMARK_START_VALUE = glue_args['OVERRIDE_WATERMARK_START_VALUE']
                WATERMARK_TYPE = glue_args['OVERRIDE_WATERMARK_TYPE']
                WATERMARK_STRPTIME_FORMAT = glue_args['OVERRIDE_WATERMARK_STRPTIME_FORMAT']

        if extraction_type == "health_check":
            health_check_threshold_table = get_config_attribute(source_config, "health_check_threshold_table").format(SNOWFLAKE_ANALYTICS_DATABASE_NAME=glue_args['SNOWFLAKE_ANALYTICS_DATABASE_NAME'])
        
        if sql_source_type == 'ODBC':
            #source_conn = get_source_connection_odbc()
            source_conn = get_source_connection_odbc()
        else:
            source_conn = get_source_connection()

        statement_format = prepare_statement_format()

        # persist glue info to snowflake
        log_glue_info()

        extraction()
    except Exception as ex:
        logger.exception(ex.args)
        raise ex

