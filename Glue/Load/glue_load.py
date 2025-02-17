"""
Script to extract data from source system and land as files
"""
import sys
import logging
import uuid
import json
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
        'STEP_FUNC_ID',
        'S3_CONFIG_BUCKET',
        'S3_CONFIG_KEY',
        'SNOWFLAKE_CONNECTION_SECRET_ARN',
        'SNOWFLAKE_STAGE',
        'SOURCE_SERVER_LABEL',
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

    logger.info(f"connection_details user: {snowflake_connection_details['user']}")
    logger.info(f"connection_details account: {snowflake_connection_details['account']}")
    logger.info(f"connection_details warehouse: {snowflake_connection_details['warehouse']}")
    logger.info(f"connection_details database: {snowflake_connection_details['database']}")
    logger.info(f"connection_details schema: {snowflake_connection_details['schema']}")
    logger.info(f"connection_details role: {snowflake_connection_details['role']}")

    # return snowflake.connector.connect(
    #     user=snowflake_connection_details['user'],
    #     password=snowflake_connection_details['password'],
    #     account=snowflake_connection_details['account'],
    #     warehouse=snowflake_connection_details['warehouse'],
    #     database=snowflake_connection_details['database'],
    #     schema=snowflake_connection_details['schema'],
    #     role=snowflake_connection_details['role'],
    #     autocommit=snowflake_autocommit
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
            role=snowflake_connection_details['role'],
            autocommit=snowflake_autocommit
            )
    else:
        raise Exception("Not valid connection details") 


def statement_from_parts(statement_parts):
    """
    combines multiple statement parts into one statement
    """
    return "".join(statement_parts)


def prepare_statement_format():
    """
    Prepare statement format containing relevant items that can be injected into sql statements
    prior to execution if needed.

    Note. watermark related details will be added during the running of this process/script
    """
    statement_format = {
            'system':system,
            'source_server':glue_args['SOURCE_SERVER_LABEL'],
            'source_database':source_database,
            'source_schema':source_schema,
            'entity':entity,
            'STEP_FUNC_ID':glue_args['STEP_FUNC_ID'],
            'GLUE_JOB_ID':GLUE_JOB_ID,
            'row_count_table':row_count_table,
            'target_schema':target_schema,
            'target_table':target_table,
            'target_columns':TARGET_COLUMNS,
            'variant_target_columns':VARIANT_TARGET_COLUMNS,
            'output_file_type':output_file_type,
            'SNOWFLAKE_STAGE':glue_args['SNOWFLAKE_STAGE']
        }

    logger.info(f"prepare_statement_format: {statement_format}")

    return statement_format


def execute_load_steps(cursor):
    """
    Read and execute extraction preparation steps

    Note.   chunk_size and watermark_value are not relevant here as extraction preparation
            steps are executed once for a given run
    """
    load_steps = target_config.get("load_steps")

    dict_of_load_steps = prioritise_list(
        load_steps,
        'priority')

    for step_key, step_value in sorted(dict_of_load_steps.items()):
        logger.info(f'step_key {step_key}')

        statement = statement_from_parts(get_config_attribute(step_value, "statement"))

        statement = statement.format(**statement_format)

        logger.info(f"statement: {statement}")

        check_rows_loaded = get_config_attribute(step_value, "check_rows_loaded", mandatory_attribute=False, default_return_value=None)
        if check_rows_loaded is None:
            cursor.execute(statement)
        else:

            dataframe = pd.read_sql(statement, target_conn)

            logger.info(f"Last row in DF: {dataframe.iloc[-1:].to_dict()}")
            if 'rows_loaded' in dataframe:
                total_rows_loaded = dataframe['rows_loaded'].sum(axis=0)
            else :
                total_rows_loaded = 0
            logger.info(f'total_rows_loaded {total_rows_loaded}')

            if total_rows_loaded <= 0:
                logger.warning(f'Total Rows loaded : {total_rows_loaded}')

            #Assign rows_loaded to allow for injection into further sql statements
            statement_format["total_rows_loaded"] = total_rows_loaded


def prepare_variant_target_columns():
    """
    Create variant column string
    """
    variant_target_column_list = []
    for k, v in target_column_list.items():
        variant_target_column_list.append(f"IFF($1:{k.upper()} = 'NULL_VALUE',NULL, $1:{k.upper()}::{v})")

    return ",".join(variant_target_column_list)


def load():
    """
    Perform load
    """
    cursor = target_conn.cursor()
    execute_load_steps(cursor)
    # will be ignored if autcommit is set to true
    target_conn.commit()
    cursor.close()


def read_health_check_threshold_data():
    """
    Read meta data from snowflake containing threshold configuration
    """
    statement = f"""
                SELECT TABLE_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, SQL_STATEMENT
                FROM {health_check_threshold_table}
                WHERE TABLE_TYPE = 'target'
                AND SERVER_NAME = '{health_check_target_server_label}'
                """

    logger.info(f"read_health_check_threshold_data statement: {statement}")

    meta_data_dataframe = pd.read_sql(statement, target_conn)
    
    logger.info(f"target meta_data_dataframe: {meta_data_dataframe}")

    # print dataframe.
    return meta_data_dataframe


def generate_target_health_check_entries():
    """
    Retrieve target (snowflake) health check entires and persit in target (snowflake) database
    """
    with target_conn.cursor() as cursor:
        for current_iteration, row in meta_data_dataframe.iterrows():

            statement_format["TABLE_ID"] = row['TABLE_ID']
            statement_format["DATABASE_NAME"] = row['DATABASE_NAME']
            statement_format["SCHEMA_NAME"] = row['SCHEMA_NAME']
            statement_format["TABLE_NAME"] = row['TABLE_NAME']
            statement_format["SQL_STATEMENT"] = row['SQL_STATEMENT']

            statement = f"""
                        INSERT INTO {target_schema}.{target_table} (LOG_DATETIME, TABLE_ID, TABLE_NAME, TOTAL, MAX_DATETIME, ENVIRONMENT, AWS_GLUE_JOB_ID, AWS_STEP_FUNC_ID)
                        SELECT CURRENT_TIMESTAMP() AS LOG_DATETIME, {row['TABLE_ID']} as TABLE_ID, '{row['DATABASE_NAME']}.{row['SCHEMA_NAME']}.{row['TABLE_NAME']}' AS TABLE_NAME
                        ,TOTAL, MAX_DATETIME, '{health_check_target_server_label}' AS ENVIRONMENT
                        ,'{GLUE_JOB_ID}' AS AWS_GLUE_JOB_ID, '{glue_args['STEP_FUNC_ID']}' AS AWS_STEP_FUNC_ID
                        FROM
                        (
                        {row['SQL_STATEMENT']}
                        ) data
                        """

            logger.info(f"health_check {health_check_target_server_label} read statement: {statement}")
    
            cursor.execute(statement)

        target_conn.commit()



def process_health_check_data_results():
    """
    Retrieve and persist health check data
    """
    with target_conn.cursor() as cursor:
    # persist results to SUPPORT.HEALTH_CHECK_ALERT_HISTORY table via sql insert agaisnt view
        statement = f"""
                    INSERT INTO {health_check_alert_history_table} (
                        HEALTH_CHECK_DATETIME,
                        TGT_TABLE_ID,
                        TGT_TABLE_NAME,
                        SRC_ROW_COUNT,
                        TGT_ROW_COUNT,
                        ROW_COUNT_THRESHOLD_PERCENT,
                        ROW_COUNT_DIFFERENCE_PERCENTAGE,
                        ROW_COUNT_ALARM,
                        TGT_MAX_DATETIME,
                        DATA_FRESHNESS_THRESHOLD,
                        DATA_FRESHNESS_DIFFERENCE_MINUTES,
                        DATA_FRESHNESS_ALARM,
                        AWS_STEP_FUNC_ID
                    )
                    SELECT 
                        HEALTH_CHECK_DATETIME,
                        TGT_TABLE_ID,
                        TGT_TABLE_NAME,
                        SRC_ROW_COUNT,
                        TGT_ROW_COUNT,
                        ROW_COUNT_THRESHOLD_PERCENT,
                        ROW_COUNT_DIFFERENCE_PERCENTAGE,
                        ROW_COUNT_ALARM,
                        TGT_MAX_DATETIME,
                        DATA_FRESHNESS_THRESHOLD,
                        DATA_FRESHNESS_DIFFERENCE_MINUTES,
                        DATA_FRESHNESS_ALARM,
                        AWS_STEP_FUNC_ID
                    FROM {health_check_latest_data_view}
                    """

        logger.info(f"health_check results persist statement: {statement}")

        cursor.execute(statement)

        target_conn.commit()

    # read view results to dataframe to interogate
    statement = f"""
                SELECT 
                    HEALTH_CHECK_DATETIME,
                    TGT_TABLE_ID,
                    TGT_TABLE_NAME,
                    SRC_ROW_COUNT,
                    TGT_ROW_COUNT,
                    ROW_COUNT_THRESHOLD_PERCENT,
                    ROW_COUNT_DIFFERENCE_PERCENTAGE,
                    ROW_COUNT_ALARM,
                    TGT_MAX_DATETIME,
                    DATA_FRESHNESS_THRESHOLD,
                    DATA_FRESHNESS_DIFFERENCE_MINUTES,
                    DATA_FRESHNESS_ALARM,
                    AWS_STEP_FUNC_ID
                FROM {health_check_latest_data_view}
                """

    logger.info(f"health_check read statement: {statement}")

    hc_results_dataframe = pd.read_sql(statement, target_conn)

    logger.info(f"health_check results_dataframe: {hc_results_dataframe}")

    # read check_threshold_to_heathcheck_entries
    statement = f"""
                select TOTAL_TARGET_ENTRIES, TOTAL_HEALTH_CHECK_ENTRIES, HEALTH_CHECK_ALARM
                from {health_check_threshold_entries_view}
                """

    logger.info(f"check_threshold_to_heathcheck_entries read statement: {statement}")

    check_threshold_to_heathcheck_entries_dataframe = pd.read_sql(statement, target_conn)

    logger.info(f"check_threshold_to_heathcheck_entries_dataframe: {check_threshold_to_heathcheck_entries_dataframe}")

    return hc_results_dataframe, check_threshold_to_heathcheck_entries_dataframe


def evaluate_health_check_data_results():
    """
    Log health check alarms
    """
    error_found = False
    
    # Check enable source_threshold_entries total - the same number of rows should be present in the alert log
    if (check_threshold_to_heathcheck_entries_dataframe.HEALTH_CHECK_ALARM.bool()):
        error_found = True
        logger.error(f"check threshold to heathcheck entries failed: AWS_STEP_FUNC_ID {glue_args['STEP_FUNC_ID']}")

    # Check for any row_count_alarms
    row_count_alerts_dataframe = hc_results_dataframe.loc[hc_results_dataframe['ROW_COUNT_ALARM'] == 1]
    if len(row_count_alerts_dataframe.index) > 0:
        error_found = True
        for index, row in row_count_alerts_dataframe.iterrows():
            logger.error(f"""ROW_COUNT_ALARM: AWS_STEP_FUNC_ID {glue_args['STEP_FUNC_ID']} Details: {row}""")

    # Check for any row_count_alarms
    data_fresh_alerts_dataframe = hc_results_dataframe.loc[hc_results_dataframe['DATA_FRESHNESS_ALARM'] == 1]
    if len(data_fresh_alerts_dataframe.index) > 0:
        error_found = True
        for index, row in data_fresh_alerts_dataframe.iterrows():
            logger.error(f"""DATA_FRESHNESS_ALARM: AWS_STEP_FUNC_ID {glue_args['STEP_FUNC_ID']} Details: {row}""")

    if health_check_register_glue_job_exception and error_found:
        raise Exception("One or more data health check failures occured")


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

        # Load entity level configuration
        entity_config = load_json_from_s3(glue_args['S3_CONFIG_BUCKET'], glue_args['S3_CONFIG_KEY'])

        source_config = entity_config.get("source")

        system = get_config_attribute(source_config, "system")
        source_database = get_config_attribute(source_config, "source_database")
        source_schema = get_config_attribute(source_config, "source_schema")
        entity = get_config_attribute(source_config, "entity")

        # needed for healthcheck
        extraction_type = get_config_attribute(source_config, "extraction_type", valid_values=VALID_EXTRACTION_TYPES)

        output_file_type = get_config_attribute(source_config, "output_file_type", valid_values=VALID_OUTPUT_FILE_TYPES)

        target_config = entity_config.get("target")

        snowflake_autocommit = get_config_attribute(target_config, "autocommit", mandatory_attribute=False, default_return_value=False)

        row_count_table = get_config_attribute(target_config, "row_count_table")
        target_schema = get_config_attribute(target_config, "target_schema")
        target_table = get_config_attribute(target_config, "target_table")

        target_column_list = get_config_attribute(target_config, "target_column_list")
        # read target columns, also strip spaces and uppercase
        TARGET_COLUMNS = ",".join(target_column_list.keys()).replace(' ', '').upper()
        #create variant target column string
        VARIANT_TARGET_COLUMNS = prepare_variant_target_columns()

        if extraction_type == "health_check":
            health_check_alert_history_table = get_config_attribute(target_config, "health_check_alert_history_table").format(SNOWFLAKE_ANALYTICS_DATABASE_NAME=glue_args['SNOWFLAKE_ANALYTICS_DATABASE_NAME'])
            health_check_latest_data_view = get_config_attribute(target_config, "health_check_latest_data_view").format(SNOWFLAKE_ANALYTICS_DATABASE_NAME=glue_args['SNOWFLAKE_ANALYTICS_DATABASE_NAME'])
            health_check_threshold_entries_view = get_config_attribute(target_config, "health_check_threshold_entries_view").format(SNOWFLAKE_ANALYTICS_DATABASE_NAME=glue_args['SNOWFLAKE_ANALYTICS_DATABASE_NAME'])
            health_check_target_server_label = get_config_attribute(target_config, "health_check_target_server_label")
            health_check_register_glue_job_exception = get_config_attribute(target_config, "health_check_register_glue_job_exception", mandatory_attribute=False,default_return_value=False)

            health_check_threshold_table = get_config_attribute(source_config, "health_check_threshold_table").format(SNOWFLAKE_ANALYTICS_DATABASE_NAME=glue_args['SNOWFLAKE_ANALYTICS_DATABASE_NAME'])

            # Adjust schema reference, for health check the target table will usually be in the analytics database
            target_schema = target_schema.format(SNOWFLAKE_ANALYTICS_DATABASE_NAME=glue_args['SNOWFLAKE_ANALYTICS_DATABASE_NAME'])

        target_conn = get_target_connection()

        statement_format = prepare_statement_format()

        load()

        if extraction_type == "health_check":
            meta_data_dataframe = read_health_check_threshold_data()
            
            generate_target_health_check_entries()

            hc_results_dataframe, check_threshold_to_heathcheck_entries_dataframe = process_health_check_data_results()
            evaluate_health_check_data_results()

    except Exception as ex:
        logger.exception(ex.args)
        raise ex

