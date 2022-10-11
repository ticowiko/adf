# Table of contents
 
- [Overview](#overview)
- [Flow configuration documentation](#flow-configuration-documentation)
- [Implementer configuration documentation](#implementer-configuration-documentation)
- [API documentation](#api-documentation)
- [Processing functions](#processing-functions)

# Overview

## Install

It is highly advised to install the ADF framework in a virtual env running `python3.7`, as this is (currently) the only supported python version for the AWS implementer. In addition, make sure to properly set your `PYSPARK_PYTHON` path for full spark support :

```
mkvirtualenv adf -p `which python3.7`
export PYSPARK_PYTHON=`which python3`
pip install adf
```

## ADF in a nutshell

**Abstract Data Flows (ADF)** is a framework that provides data platform automation without infrastructure commitment. Data processing flows are defined in an infrastructure agnostic manner, and are then plugged into any **implementer** configuration of your choice. This provides all the major benefits of automation (namely, instantly deployable production ready infrastructure) while avoiding its major pitfall : being tied to your choice of infrastructure.

## Getting started

For an easy-to-follow tutorial, please refer to the accompanying [adf_app](https://github.com/ticowiko/adf_app) sister repository and its associated README.

# Flow configuration documentation

- [Global configuration](#global-configuration)
- [Modules](#modules)
- [Flows](#flows)
  - [Starting steps](#starting-steps)
    - [Landing step](#landing-step)
    - [Combination step](#combination-step)
    - [Reception step](#reception-step)
  - [Non-starting step](#non-starting-step)
  - [Metadata configuration](#metadata-configuration)

Each flow configuration file defines an **ADF collection**. The configuration can be broken down into 3 categories of parameters.

## Global configuration

| Parameter               | Obligation                          | Description                          |
|-------------------------|-------------------------------------|--------------------------------------|
| `name`                  | **REQUIRED**                        | The name for the ADF collection.     |
| `BATCH_ID_COLUMN_NAME`  | **OPTIONAL**, advised not to change | Column name to store batch IDs.      |
| `SQL_PK_COLUMN_NAME`    | **OPTIONAL**, advised not to change | Column name to store a PK if needed. |
| `TIMESTAMP_COLUMN_NAME` | **OPTIONAL**, advised not to change | Column name to store timestamps.     |

For example :

```yaml
name: collection-name
BATCH_ID_COLUMN_NAME: modified-batch-id-column-name
SQL_PK_COLUMN_NAME: modified-sql-pk-column-name
TIMESTAMP_COLUMN_NAME: modified-timestamp-column-name
```

## Modules

Modules are listed under the `modules` parameter. Each module must define the following parameters :

| Parameter     | Obligation   | Description                    |
|---------------|--------------|--------------------------------|
| `name`        | **REQUIRED** | Alias to refer to this module. |
| `import_path` | **REQUIRED** | Valid python import path.      |

For example :

```yaml
modules:
  - name: module-alias
    import_path: package.module
```

## Flows

The actual data flows are defined under the `flows` parameter, as a list of named flows, each containing a list of named steps.

| Parameter | Obligation   | Description        |
|-----------|--------------|--------------------|
| `name`    | **REQUIRED** | Unique Identifier. |
| `steps`   | **REQUIRED** | List of steps.     |

For example :

```yaml
collection: collection-name
modules:
  - [...]
  - [...]
flows:
  name: flow-name
  steps:
    - [...] # Step config
    - [...] # Step config
    - [...] # Step config
```

### Starting steps

The first step in a flow is known as a starting step. There are 3 types of starting steps.

#### Landing step

There are passive steps that define where input data is expected to be received. As such, they cannot define a processing function, metadata, or any custom flow control mechanisms.

| Parameter          | Obligation   | Description                            |
|--------------------|--------------|----------------------------------------|
| `start`            | **REQUIRED** | Must be set to `landing`.              |
| `layer`            | **REQUIRED** | Data layer name.                       |
| `name`             | **REQUIRED** | Identifier unique within this flow.    |
| `version`          | **OPTIONAL** | Data version ID.                       |
| `func`             | **BANNED**   | Cannot define a processing function.   |
| `func_kwargs`      | **BANNED**   | Cannot define function keywords.       |
| `meta`             | **BANNED**   | Cannot define custom metadata.         |
| `sequencer`        | **BANNED**   | Cannot define a custom sequencer.      |
| `data_loader`      | **BANNED**   | Cannot define a custom data loader.    |
| `batch_dependency` | **BANNED**   | Cannot define custom batch dependency. |

For example :

```yaml
name: collection-name
flows:
  - name: flow-name
    steps:
      - start: landing
        layer: layer-name
        name: landing-step-name
        version: source-data-version-name
```

#### Combination step

These steps define the start of a flow that takes as input multiple previous steps.

| Parameter          | Obligation   | Description                               |
|--------------------|--------------|-------------------------------------------|
| `start`            | **REQUIRED** | Must be set to `combination`.             |
| `layer`            | **REQUIRED** | Data layer name.                          |
| `name`             | **REQUIRED** | Identifier unique within this flow.       |
| `input_steps`      | **REQUIRED** | Input steps to combine.                   |
| `version`          | **OPTIONAL** | Data version ID.                          |
| `func`             | **OPTIONAL** | The processing function.                  |
| `func_kwargs`      | **OPTIONAL** | Extra kwargs to pass to the function.     |
| `meta`             | **OPTIONAL** | Metadata constraints on output.           |
| `sequencer`        | **OPTIONAL** | Defined for the step itself.              |
| `data_loader`      | **OPTIONAL** | Defined for the step and the input steps. |
| `batch_dependency` | **OPTIONAL** | Defined only for the input steps.         |

A minimal working example :

```yaml
name: collection-name
modules:
  - name: module-alias
    import_path: package.module
flows:
  - name: flow-name-0
    steps:
      - start: landing
        layer: layer-name
        name: landing-step-name
  - name: flow-name-1
    steps:
      - start: landing
        layer: layer-name
        name: landing-step-name
  - name: combination-flow
    steps:
      - start: combination
        layer: layer-name
        name: combination-step-name
        input_steps:
          - flow_name: flow-name-0
            step_name: landing-step
          - flow_name: flow-name-1
            step_name: landing-step
        func: # REQUIRED if there are more than one input steps
          load_as: module
          params:
            module: module-alias
            name: processing_function_name
```

An example with all optional configurations :

```yaml
name: collection-name
modules:
  - name: module-alias
    import_path: package.module
flows:
  - name: flow-name-0
    steps:
      - start: landing
        layer: layer-name
        name: landing-step-name
  - name: flow-name-1
    steps:
      - start: landing
        layer: layer-name
        name: landing-step-name
  - name: combination-flow
    steps:
      - start: combination
        layer: layer-name
        name: combination-step-name
        version: version-name
        input_steps:
          - flow_name: flow-name-0
            step_name: landing-step
            data_loader:
              module: module-alias
              class_name: DataLoaderClassName
              params: [...] # class init params
            batch_dependency:
              module: module-alias
              class_name: BatchDependencyClassName
              params: [...] # class init params
          - flow_name: flow-name-1
            step_name: landing-step
            data_loader:
              module: module-alias
              class_name: DataLoaderClassName
              params: [...] # class init params
            batch_dependency:
              module: module-alias
              class_name: BatchDependencyClassName
              params: [...] # class init params
        func:
          load_as: module
          params:
            module: module-alias
            name: processing_function_name
        func_kwargs: [...] # kwargs dictionary
        meta: [...] # metadata for output
        sequencer:
          module: module-alias
          class_name: SequencerClassName
          params: [...] # class init params
        data_loader:
          module: module-alias
          class_name: DataLoaderClassName
          params: [...] # class init params
```

#### Reception step

A reception step is used when we want a processing step to output more than one data structure. When a processing step is hooked to reception steps, no data will actually be saved at the processing step itself. Instead, the reception steps will serve as storage steps. As a result, much like landing steps, they cannot define a processing function or any custom flow control mechanisms, but they can define metadata.

| Parameter          | Obligation   | Description                            |
|--------------------|--------------|----------------------------------------|
| `start`            | **REQUIRED** | Must be set to `reception`.            |
| `layer`            | **REQUIRED** | Data layer name.                       |
| `name`             | **REQUIRED** | Identifier unique within this flow.    |
| `key`              | **REQUIRED** | Keyword by which to specify this step. |
| `input_steps`      | **REQUIRED** | Upstream steps to store results of.    |
| `meta`             | **OPTIONAL** | Metadata constraints on incoming data. |
| `version`          | **BANNED**   | Uses input step version.               |
| `func`             | **BANNED**   | Cannot define a processing function.   |
| `func_kwargs`      | **BANNED**   | Cannot define function keywords.       |
| `sequencer`        | **BANNED**   | Cannot define a custom sequencer.      |
| `data_loader`      | **BANNED**   | Cannot define a custom data loader.    |
| `batch_dependency` | **BANNED**   | Cannot define custom batch dependency. |

For example :

```yaml
name: collection-name
modules:
  - name: module-alias
    import_path: package.module
flows:
  - name: flow-name
    steps:
      - start: landing
        layer: layer-name
        name: landing-step-name
      - layer: layer-name
        name: processing-step
        func: # see below for example function
          load_as: module
          params:
            module: module-alias
            name: multiple_outputs
  - name: reception-flow-0
    steps:
      - start: reception
        layer: layer-name
        name: reception-step-name
        key: reception-key-0
        input_steps:
          - flow_name: flow-name
            step_name: processing-step
  - name: reception-flow-1
    steps:
      - start: reception
        layer: layer-name
        name: reception-step-name
        key: reception-key-1
        input_steps:
          - flow_name: flow-name
            step_name: processing-step
```

Hooking a processing step into a reception step changes the expected output signature of the processing function. Instead of returning a single ADS, it must now return a dictionary whose keys correspond to the reception step keys. In our case :

```python
def multiple_outputs(
    ads: AbstractDataStructure,
) -> Dict[str, AbstractDataStructure]:
    return {
        "reception-key-0": ads[ads["col_0"] == 0],
        "reception-key-1": ads[ads["col_0"] != 0],
    }
```

### Non-starting step

A non-starting step is any step that is not the first step in a flow. It can customize any and all flow control mechanisms, as well as define a processing function and define metadata.

| Parameter          | Obligation   | Description                           |
|--------------------|--------------|---------------------------------------|
| `layer`            | **REQUIRED** | Data layer name.                      |
| `name`             | **REQUIRED** | Identifier unique within this flow.   |
| `version`          | **OPTIONAL** | Data version ID.                      |
| `func`             | **OPTIONAL** | The processing function.              |
| `func_kwargs`      | **OPTIONAL** | Extra kwargs to pass to the function. |
| `meta`             | **OPTIONAL** | Metadata constraints on output.       |
| `sequencer`        | **OPTIONAL** | Defines batch sequencing.             |
| `data_loader`      | **OPTIONAL** | Defines data loading.                 |
| `batch_dependency` | **OPTIONAL** | Defines batch dependency.             |
| `start`            | **BANNED**   | Must not be set.                      |

A minimal working example :

```yaml
name: collection-name
flows:
  - name: flow-name-0
    steps:
      - start: landing
        layer: layer-name
        name: landing-step-name
      - layer: layer-name
        name: processing-step-name
        func: # if not set, the input data is merely copied
          load_as: eval
          params:
            expr: 'lambda ads: ads[ads["col_0" == 0]]'
```

An example with all optional configurations :

```yaml
name: collection-name
modules:
  - name: module-alias
    import_path: package.module
flows:
  - name: flow-name-0
    steps:
      - start: landing
        layer: layer-name
        name: landing-step-name
      - layer: layer-name
        name: processing-step-name
        version: version-name
        func:
          load_as: eval
          params:
            expr: 'lambda ads: ads[ads["col_0" == 0]]'
        func_kwargs: [...] # kwargs dictionary
        meta: [...] # metadata for output
        sequencer:
          module: module-alias
          class_name: SequencerClassName
          params: [...] # class init params
        data_loader:
          module: module-alias
          class_name: DataLoaderClassName
          params: [...] # class init params
        batch_dependency:
          module: module-alias
          class_name: BatchDependencyClassName
          params: [...] # class init params
```

### Metadata configuration

Metadata is configured by specifying column names, data types, and requested behavior when missing. You can also set the default missing column behavior, as well as what to do with extra columns. All metadata parameters except for the column name are optional.

| Parameter          | Values                                                      | Description                     |
|--------------------|-------------------------------------------------------------|---------------------------------|
| column.name        | Any                                                         | Name of the column              |
| column.cast        | `str`,`int`,`float`,`complex`,`datetime`,`date`,`timedelta` | Data type                       |
| column.on_missing  | `ignore`, `fail`, `fill`                                    | Missing column behavior         |
| column.fill_val    | Any, defaults to `None`                                     | Value to fill column if missing |
| in_partition       | `true`, `false`                                             | Whether to use in partition     |
| on_missing_default | `ignore`, `fail`, `fill`                                    | Default missing column behavior |
| on_extra           | `ignore`, `fail`, `cut`                                     | What to do with extra columns   |

For example :

```yaml
name: collection-name
flows:
  - name: flow-name
    steps:
      - start: landing
        layer: layer-name
        name: landing-step
      - layer: layer-name
        name: meta-step
        meta:
          columns:
            - name: essential_column
              cast: str
              on_missing: fail
              in_partition: true
            - name: integer_column
              cast: int
              on_missing: fill
              fill_val: "FILL_VALUE"
            - name: weakly_defined_column
          on_missing_default: ignore
          on_extra: cut
```

# Implementer configuration documentation

- [Local Implementer](#local-implementer)
- [AWS Implementer](#aws-implementer)
  - [Managed infrastructure AWS Implementer](#managed-infrastructure-aws-implementer)
  - [Prebuilt infrastructure AWS Implementer](#prebuilt-infrastructure-aws-implementer)

While implementer configurations are allowed to vary freely, there is one parameter they must all contain to actually specify which implementer they are destined for. Its value must be a valid python import path.

| Parameter         | Description                                    |
|-------------------|------------------------------------------------|
| `implementer_class` | Module path followed by implementer class name |

For example, if the implementer class is defined in the module `package.module`, and the implementer class name is `ImplementerClass`, the corresponding configuration would be :

```yaml
implementer_class: package.module.ImplementerClass
```

## Local Implementer

The local implementer requires a root path, as well as a list of layer names to associate to each layer type. The available layer types are :

- 3 file based CSV data layers, each of which manipulates data differently to perform computation :
  - `list_of_dicts` : Loads data as a list of dictionaries to perform computation.
  - `pandas` : Loads data as pandas DataFrame to perform computation.
  - `spark` : Loads data as a pyspark DataFrame to perform computation.
- 2 database backed data layers, each of which uses a different database engine to perform storage and computation :
  - `sqlite` : Uses an sqlite database to store and process data.
  - `postgres` : Uses a postgresql database to store and process data.

If at least one `postgres` layer is used, then connection information must also be passed. Admin credentials may also be passed if one wishes for the implementer to create the database and technical user in question.

| Parameter              | Obligation   | Description                                                  |
|------------------------|--------------|--------------------------------------------------------------|
| `implementer_class`    | **REQUIRED** | Module path followed by implementer class name               |
| `root_path`            | **REQUIRED** | Root path to store data and state handler                    |
| `extra_packages`       | **OPTIONAL** | List of local paths to any packages required                 |
| `layers.list_of_dicts` | **OPTIONAL** | List of list of dict based layers                            |
| `layers.pandas`        | **OPTIONAL** | List of pandas based layers                                  |
| `layers.spark`         | **OPTIONAL** | List of pyspark based layers                                 |
| `layers.sqlite`        | **OPTIONAL** | List of sqlite based layers                                  |
| `layers.postgres`      | **OPTIONAL** | List of postgres based layers                                |
| `postgres_config`      | **OPTIONAL** | `host`, `port`, `db`, `user`, `pw`, `admin_user`, `admin_pw` |

For example, to configure a local implementer without any postgres based layers :

```yaml
implementer_class: ADF.components.implementers.MultiLayerLocalImplementer
extra_packages: [.]
root_path: path/to/data
layers:
  pandas:
    - pandas-layer-name-0
    - pandas-layer-name-1
  spark:
    - spark-layer-name-0
    - spark-layer-name-1
  sqlite:
    - sqlite-layer-name-0
    - sqlite-layer-name-1
```

To be able to include postgres based layers, one must add connection information :

```yaml
implementer_class: ADF.components.implementers.MultiLayerLocalImplementer
extra_packages: [.]
root_path: path/to/data
postgres_config:
  db: adf_db           # Required
  user: adf_user       # Required
  pw: pw               # Required
  host: localhost      # Optional, defaults to localhost
  port: 5432           # Optional, defaults to 5432
  admin_user: postgres # Optional, will be used to create db and user if needed
  admin_pw: postgres   # Optional, will be used to create db and user if needed
layers:
  pandas:
    - pandas-layer-name-0
    - pandas-layer-name-1
  spark:
    - spark-layer-name-0
    - spark-layer-name-1
  postgres:
    - postgres-layer-name-0
    - postgres-layer-name-1
```

## AWS Implementer

### Managed infrastructure AWS Implementer

The AWS implementer configuration file is similar in structure to that of the local implementer. When ADF is given free rein over infrastructure deployment, individual layers carry sizing information. In addition, the state handler configuration and sizing must also be specified.

| Parameter               | Obligation   | Description                                                                                                                                                                                                       |
|-------------------------|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `implementer_class`     | **REQUIRED** | Module path followed by implementer class name                                                                                                                                                                    |
| `mode`                  | **REQUIRED** | Set to `managed` to tell your implementer to handle infrastructure deployment                                                                                                                                     |
| `name`                  | **REQUIRED** | An identifier for the implementer                                                                                                                                                                                 |
| `log_folder`            | **REQUIRED** | A local folder in which to store subcommand logs                                                                                                                                                                  |
| `bucket`                | **REQUIRED** | The S3 bucket used for data storage                                                                                                                                                                               |
| `s3_prefix`             | **REQUIRED** | S3 prefix for all data and uploaded configuration                                                                                                                                                                 |
| `state_handler`         | **REQUIRED** | `engine`, `db_name`, `db_instance_class`, `allocated_storage`                                                                                                                                                     |
| `extra_packages`        | **OPTIONAL** | List of local paths to any additional required packages                                                                                                                                                           |
| `lambda_layers`         | **OPTIONAL** | `sep`, `timeout`, `memory`                                                                                                                                                                                        |
| `emr_layers`            | **OPTIONAL** | `master_instance_type`, `slave_instance_type`, `instance_count`, `step_concurrency`, `format`, `landing_format`                                                                                                   |
| `emr_serverless_layers` | **OPTIONAL** | `initial_driver_worker_count`, `initial_driver_cpu`, `initial_driver_memory`, `initial_executor_worker_count`, `initial_executor_cpu`, `initial_executor_memory`, `max_cpu`, `max_memory`, `idle_timeout_minutes` |
| `redshift_layers`       | **OPTIONAL** | `db_name`, `node_type`, `number_of_nodes`                                                                                                                                                                         |
| `athena_layer`          | **OPTIONAL** | `landing_format`                                                                                                                                                                                                  |

For example :

```yaml
implementer_class: ADF.components.implementers.AWSImplementer
extra_packages: [.]
mode: managed # ADF will handle infrastructure deployment
name: implementer-name
log_folder: local/path/to/logs
bucket: YOUR-BUCKET-NAME-HERE
s3_prefix: YOUR_S3_PREFIX/
state_handler:
  engine: postgres # only postgres is currently supported
  db_name: ADF_STATE_HANDLER
  db_instance_class: db.t3.micro
  allocated_storage: 20
lambda_layers:
  lambda-layer-name:
    sep: "," # separator for CSVs
    timeout: 60
    memory: 1024
emr_layers:
  heavy:
    master_instance_type: m5.xlarge
    slave_instance_type: m5.xlarge
    instance_count: 1
    step_concurrency: 5
    format: parquet     # the format in which to store data
    landing_format: csv # the format in which to expect data in landing steps
emr_serverless_layers:
  serverless:
    initial_driver_worker_count: 1
    initial_driver_cpu: "1vCPU"
    initial_driver_memory: "8GB"
    initial_executor_worker_count: 1
    initial_executor_cpu: "1vCPU"
    initial_executor_memory: "8GB"
    max_cpu: "32vCPU",
    max_memory: "256GB",
    idle_timeout_minutes: 15,
redshift_layers:
  expose:
    db_name: expose
    number_of_nodes: 1
    node_type: ds2.xlarge
athena_layers:
  dump:
    landing_format: csv # the format in which to expect data in landing steps
```

### Prebuilt infrastructure AWS Implementer

If you wish to connect your AWS implementer to pre-existing infrastructure, you can do this by changing the implementer mode to `prebuilt`. Once the implementer setup is run, it is possible to output a `prebuilt` configuration and use it moving forward. This is the recommended usage, as `prebuilt` mode requires fewer permissions to run, as well as fewer API calls to determine the current state of the infrastructure. Unlike in `managed` mode, no sizing information is provided. Instead, we pass endpoints and various configurations that define the data layer.

| Parameter               | Obligation   | Description                                                                                                                                                    |
|-------------------------|--------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `implementer_class`     | **REQUIRED** | Module path followed by implementer class name                                                                                                                 |
| `mode`                  | **REQUIRED** | Set to `managed` to tell your implementer to handle infrastructure deployment                                                                                  |
| `name`                  | **REQUIRED** | An identifier for the implementer                                                                                                                              |
| `log_folder`            | **REQUIRED** | A local folder in which to store subcommand logs                                                                                                               |
| `bucket`                | **REQUIRED** | The S3 bucket used for data storage                                                                                                                            |
| `s3_prefix`             | **REQUIRED** | S3 prefix for all data and uploaded configuration                                                                                                              |
| `state_handler_url`     | **REQUIRED** | URL to state handler DB                                                                                                                                        |
| `extra_packages`        | **OPTIONAL** | List of local paths to any additional required packages                                                                                                        |
| `lambda_layers`         | **OPTIONAL** | `lambda_arn`, `lambda_name`, `s3_fcp_template`, `s3_icp`, `sep`, `sqs_arn`, `sqs_name`, `sqs_url`                                                              |
| `emr_layers`            | **OPTIONAL** | `bucket`, `s3_prefix`, `cluster_id`, `cluster_arn`, `name`, `public_dns`, `log_uri`, `format`, `landing_format`                                                |
| `emr_serverless_layers` | **OPTIONAL** | `application_id`, `bucket`, `environ`, `format`, `landing_format`, `role_arn`, `s3_fcp_template`, `s3_icp`, `s3_launcher_key`, `s3_prefix`, `venv_package_key` |
| `redshift_layers`       | **OPTIONAL** | `table_prefix`, `endpoint`, `port`, `db_name`, `user`, `role_arn`                                                                                              |
| `athena_layers`         | **OPTIONAL** | `bucket`, `db_name`, `landing_format`, `s3_prefix`, `table_prefix`                                                                                             |

For example :

```yaml
implementer_class: ADF.components.implementers.AWSImplementer
extra_packages: [.]
mode: prebuilt # ADF will plug into pre-existing infrastructure
name: implementer-name
log_folder: local/path/to/logs
bucket: YOUR-BUCKET-NAME-HERE
s3_prefix: YOUR_S3_PREFIX/
state_handler_url: postgresql://username:password@state.handler.db.url:5432/DB_NAME
lambda_layers:
  light:
    lambda_arn: LAMBDA_ARN
    lambda_name: LAMBDA_FUNCTION_NAME
    s3_fcp_template: s3://TEMPLATE/TO/FCP/PATH/fcp.{collection_name}.yaml
    s3_icp: s3://ICP/PATH/icp.yaml
    sep: ',' # separator for CSVs
    sqs_arn: SQS_ARN
    sqs_name: SQS_QUEUE_NAME
    sqs_url: https://url.to/sqs/queue
emr_layers:
  heavy:
    bucket: YOUR-BUCKET-NAME-HERE
    s3_prefix: S3/PREFIX/ # where to store data in the bucket
    cluster_id: EMR_CLUSTER_ID
    cluster_arn: EMR_CLUSTER_ARN
    name: EMR_CLUSTER_NAME
    public_dns: https://url.to.emr.cluster
    log_uri: s3://PATH/TO/LOGS/
    format: parquet     # the format in which to store data
    landing_format: csv # the format in which to expect data in landing steps
emr_serverless_layers:
  serverless:
    application_id: app-id
    bucket: YOUR-BUCKET-NAME-HERE
    environ:
      AWS_DEFAULT_REGION: aws-region
      RDS_PW: RDS_STATE_HANDLER_PASSWORD
      REDSHIFT_PW: REDSHIFT_PASSWORD
    format: parquet     # the format in which to store data
    landing_format: csv # the format in which to expect data in landing steps
    role_arn: EXECUTION_ROLE_ARN
    s3_fcp_template: s3://TEMPLATE/TO/FCP/PATH/fcp.{collection_name}.yaml
    s3_icp: s3://ICP/PATH/icp.yaml
    s3_launcher_key: KEY/TO/ADF/LAUNCHER/adf-launcher.py
    s3_prefix: S3/PREFIX/ # where to store data in the bucket
    venv_package_key: S3/PREFIX/venv_package.tar.gz
redshift_layers:
  expose:
    table_prefix: TABLE_PREFIX
    endpoint: https://url.to.db
    port: PORT_NUMBER
    db_name: DB_NAME
    user: DB_USERNAME
    role_arn: EXECUTION_ROLE_ARN
athena_layers:
  dump:
    bucket: YOUR-BUCKET-NAME-HERE
    db_name: ATHENA_DB_NAME
    landing_format: csv # the format in which to expect data in landing steps
    s3_prefix: S3/PREFIX/TO/DATA/ # where to store data in the bucket
    table_prefix: 'expose_'
```

# API documentation

- [AbstractDataStructure](#abstractdatastructure)
  - [Column manipulation methods](#column-manipulation-methods)
  - [Data access](#data-access)
  - [Aggregation methods](#aggregation-methods)
- [AbstractDataColumn](#abstractdatacolumn)
  - [Column operations](#column-operations)
  - [Column aggregations](#column-aggregations)
  - [Operators](#operators)
- [AbstractDataInterface](#abstractdatainterface)
- [AbstractStateHandler](#abstractstatehandler)
- [ADFSequencer and ADFCombinationSequencer](#adfsequencer-and-adfcombinationsequencer)
  - [ADFSequencer](#adfsequencer)
  - [ADFCombinationSequencer](#adfcombinationsequencer)
- [ADFDataLoader](#adfdataloader)
- [ADFBatchDependencyHandler](#adfbatchdependencyhandler)

## AbstractDataStructure

An **Abstract Data Structure** (ADS) provides a dataframe like API for data manipulation. This is the native input and output format for your processing functions, barring concretization. The actual execution details of the below methods will depend on which type of ADS the underlying data layer has provided us with (Pandas based, Spark based, SQL based etc.).

### Column manipulation methods

-----

```python
def list_columns(self) -> List[str]
```

Lists columns currently in the ADS.

-----

```python
def col_exists(self, col_name: str) -> bool
```

Check if column `col_name` exists.

-----

```python
def prune_tech_cols(self) -> "AbstractDataStructure"
```

Removes technical columns from the ADS.

-----

```python
def rename(self, names: Dict[str, str]) -> "AbstractDataStructure"
```

Renames columns from the keys of the `names` dictionary to the values of the `names` dictionary.

-----

### Data access

-----

```python
def __getitem__(
    self, key: Union[str, AbstractDataColumn, List[str]]
) -> Union["AbstractDataStructure", AbstractDataColumn]
```

- If `key` is a string, returns the corresponding column : `ads["col_name"]`
- If `key` is an `AbstractDataColumn`, return an ADS filtered based on the truth value of the column : `ads[ads["col_name"] == "filter_value"]`
- If `key` is a list of strings, returns an ADS containing only the subset of columns specified in `key` : `ads[["col_0", "col_1"]]`

-----

```python
def __setitem__(
    self,
    key: Union[str, Tuple[str, AbstractDataColumn]],
    value: Union[Any, AbstractDataColumn],
) -> None
```

- If `value` is an `AbstractDataColumn`, use it to set the specified entries in the ADS : `ads["col_name"] = ads["col_name"]*2`
- If `value` is any other type, fill every specified entry with its value : `ads["col_name"] = "col_value"`
- If `key` is a string, set the values of the corresponding column. Creates the column if it does not already exist.
- If `key` is a `(str, AbstractDataColumn)` type tuple, set the values of the column `key[0]` only for rows filtered by `key[1]`. Note that `key[0]` must necessarily already exist as a column. Can set using either a constant value or another column. For example:

```python
ads["col_0", ads["col_1"] == "FILTER_VAL"] = "SOME_VAL"
ads["col_0", ads["col_1"] == "FILTER_VAL"] = ads["col_2"]
```

-----

```python
def to_list_of_dicts(self) -> List[Dict]
```

Returns a list of dictionaries, each of which corresponds to a single row in the ADS.

-----

### Aggregation methods

-----

```python
def __len__(self) -> int
```

Returns the number of rows in the ADS.

-----

```python
def __bool__(self) -> bool
```

Return `False` if the ADS has 0 rows, `True` otherwise.

-----

```python
def join(
    self,
    other: "AbstractDataStructure",
    left_on: List[str],
    right_on: List[str],
    how: Literal["left", "right", "outer", "inner", "cross"] = "inner",
    l_modifier: Callable[[str], str] = lambda x: x,
    r_modifier: Callable[[str], str] = lambda x: x,
    modify_on: bool = True,
) -> "AbstractDataStructure"
```

Joins 2 ADS objects together.

- `other` : The right-hand ADS in the join.
- `left_on` : The left-hand columns on which to join.
- `right_on` : The right-hand columns on which to join.
- `how`: The join type.
- `l_modifier`: A function that modifies column names for the left-hand ADS.
- `r_modifier`: A function that modifies column names for the right-hand ADS.
- `modify_on`: Specify whether the column name modification functions should apply to the join columns.

-----

```python
def group_by(
    self,
    keys: List[str],
    outputs: Dict[
        str,
        Tuple[
            Callable[["AbstractDataStructure"], Any],
            Type,
        ],
    ],
) -> "AbstractDataStructure"
```

Performs a group by operation on a given ADS.

- `keys` : List of columns on which to group.
- `outputs` : Dictionary defining aggregations to perform. The dict key is the output column name. The dict value is a 2-tuple whose first entry is a callable defining the aggregation, and whose second entry is the output type.

For example, to group on columns `col_0` and `col_1`, and compute the integer maximum and minimum values of column `col_2` for each group, one would write :

```python
ads.group_by(
    keys=["col_0", "col_1"],
    outputs={
        "min_col_2": (lambda ads: ads["col_2"].min(), int),
        "max_col_2": (lambda ads: ads["col_2"].max(), int),
    },
)
```

-----

```python
def union(
    self, *others: "AbstractDataStructure", all: bool = True
) -> "AbstractDataStructure"
```

Performs a union with all given input ADSs.

- `others` : A varargs list of ADSs.
- `all` : If `False`, deduplicate results.

-----

```python
def distinct(self, keys: Optional[List[str]] = None) -> "AbstractDataStructure"
```

Deduplicate entries. Can optionally deduplicate only on a subset of columns by specifying the `keys` arguments.

-----

```python
def apply(
    self, output_column: str, func: Callable[[Dict], Any], cast: Type
) -> "AbstractDataStructure"
```

Apply a User Defined Function (UDF) on the ADS.

- `output_column` : Name of the output column that will contain the result of the UDF.
- `func` : The UDF in question. Takes a dict as input that corresponds to a given row of the ADS.
- `cast` : The output data type.

For example :

```python
ads.apply("output_col", lambda x: str(x["col_0"]).upper(), str)
```

-----

```python
def sort(
    self, *cols: str, asc: Union[bool, List[bool]] = True
) -> "AbstractDataStructure"
```

Sort the ADS along the given columns. Set if ascending or descending order using `asc`.

-----

```python
def limit(self, n: int) -> "AbstractDataStructure"
```

Output a subset of the ADS based on the given number of rows.

-----

## AbstractDataColumn

An **Abstract Data Column** is a column of an ADS. Much like with an ADS, specific execution details vary based on the ADS it originated from (Pandas based, Spark based, SQL based etc.).

### Column operations

-----

```python
def as_type(self, t: Type, **kwargs) -> "AbstractDataColumn"
```

Cast a column to the requested type. Acceptable types are :

- `str`
- `int`
- `float`
- `complex`
- `bool`
- `datetime.datetime` : default conversion options may be overridden by specifying kwargs `auto_convert`, `as_timestamp`, and `datetime_format`.
- `datetime.date`
- `datetime.timedelta`

-----

```python
def isin(self, comp: List) -> "AbstractDataColumn"
```

Returns a boolean column where rows are set to `True` when entries are in the given `comp` list, and `False` otherwise.

### Column aggregations

```python
def min(self) -> Any
```

Returns minimum value of column.

-----

```python
def max(self) -> Any
```

Returns maximum value of column.

-----

```python
def sum(self) -> Any
```

Returns sum of all column entries.

-----

```python
def mean(self) -> Any
```

Returns average value of column entries.

-----

```python
def count(self) -> int
def __len__(self) -> int
```

Returns number of rows in column.

-----

```python
def __bool__(self) -> bool
```

Return `False` if the column has 0 rows, `True` otherwise.

-----

### Operators

All binary and unary operators are supported. For example :

```python
ads["col_0"] * 2
2 - ads["col_0"]
ads["col_0"] / ads["col_1"]
~(ads["col_0"] == ads["col_1"])
```

-----

## AbstractDataInterface

An **Abstract Data Interface** handles all matters related to persisting data. Abstract Data Interfaces correspond either directly to a given data layer, or to a transition between 2 data layers. Much like with an ADS, execution details depend on the underlying persistance details (file based, database based, cloud based etc.).

-----

```python
def read_batch_data(self, step: ADFStep, batch_id: str) -> AbstractDataStructure
```

For a given step and batch ID, return the corresponding ADS.

-----

```python
def read_full_data(self, step: ADFStep) -> AbstractDataStructure
```

For a given step return all available data.

-----

```python
def read_batches_data(
    self, step: ADFStep, batch_ids: List[str]
) -> Optional[AbstractDataStructure]
```

For a given step and a list of batch IDs, return the corresponding ADS. Returns `None` if the input batch ID list is empty.

-----

```python
def write_batch_data(
    self, ads: AbstractDataStructure, step: ADFStep, batch_id: str
) -> None
```

Given an ADS and a target step and batch ID, persist the ADS.

-----

```python
def delete_step(self, step: ADFStep) -> None
```

Delete all data in the given step.

-----

```python
def delete_batch(self, step: ADFStep, batch_id: str) -> None
```

Delete data corresponding to a specific batch ID for a given step.

-----

## AbstractStateHandler

An **Abstract State Handler** contains all information related to the current processing state. In particular, it can list all batch IDs and their current state.

-----

```python
def to_ads(self) -> AbstractDataStructure
```

Returns an ADS describing all batches. The output ADS will always have the following columns :

- `collection_name`
- `flow_name`
- `step_name`
- `version`
- `layer`
- `batch_id`
- `status`
- `datetime`
- `msg`

This method gives you complete read capabilities on the state handler, allowing you to extract any information you need from it. All following methods are merely shortcuts built on top of this one.

-----

```python
def to_step_ads(self, step: ADFStep) -> AbstractDataStructure
```

Returns an ADS containing the processing state of a given ADF step.

-----

```python
def get_entries(
    self,
    collection_name: Optional[str] = None,
    flow_name: Optional[str] = None,
    step_name: Optional[str] = None,
    version: Optional[str] = None,
    layer: Optional[str] = None,
    batch_id: Optional[str] = None,
    status: Optional[str] = None,
) -> List[Dict]
```

Returns a list of batch IDs corresponding to the given filters.

-----

```python
def get_step_submitted(self, step: ADFStep) -> List[str]
def get_step_running(self, step: ADFStep) -> List[str]
def get_step_deleting(self, step: ADFStep) -> List[str]
def get_step_failed(self, step: ADFStep) -> List[str]
def get_step_success(self, step: ADFStep) -> List[str]
```

For a given ADF step, return all batch IDs in the given state (`submitted`, `running`, `deleting`, `failed`, or `success`).

-----

```python
def get_step_all(self, step) -> List[str]
```

For a given ADF step, return all batch IDs.

-----

```python
def get_batch_info(self, step: ADFStep, batch_id: str) -> Dict
```

For a given batch ID of a given ADF step, return a dictionary containing all batch information.

-----

```python
def get_batch_status(self, step: ADFStep, batch_id: str) -> str
```

For a given ADF step, returns the status of a given batch ID. Raises an error if the batch ID is unknown to the state handler.

-----

## ADFSequencer and ADFCombinationSequencer

An **ADF Sequencer** defines the batches to be processed by a given step at any given time. To define your own ADF Sequencer, you must inherit from either the **ADFSequencer** or **ADFCombinationSequencer** base class (the latter should only be used for combination steps). In both cases, there are 2 abstract methods that require defining.

### ADFSequencer

-----

```python
def from_config(cls, config: Dict) -> "ADFSequencer"
```

Given configuration parameters, return an `ADFSequencer` instance. If you define a custom `__init__` for this, make sure it calls `super().__init__()`.

-----

```python
def get_to_process(
    self,
    state_handler: AbstractStateHandler,
    step_in: ADFStep,
    step_out: ADFStep,
) -> List[str]
```

Input arguments :

- `state_handler` : Contains the current processing state.
- `step_in` : The input step.
- `step_out` : The output step.

How the output shapes the flow of data :

- The return value is the list of batch IDs the output step is expected to create.
- By default, previously submitted batches are ignored, there is no need for your method to check for them.
- If you want your sequencer to resubmit such batches, you have to explicitly set `redo` to `True` in your constructor by calling `super().__init__(redo=True)`.

-----

### ADFCombinationSequencer

-----

```python
def from_config(cls, config: Dict) -> "ADFCombinationSequencer"
```

Given configuration parameters, return an `ADFCombinationSequencer` instance. If you define a custom `__init__` for this, make sure it calls `super().__init__()`.

-----

```python
def get_to_process(
    self,
    state_handler: AbstractStateHandler,
    combination_step: ADFCombinationStep,
) -> List[Tuple[List[str], str]]
```

Input arguments :

- `state_handler` : Contains the current processing state.
- `combination_step` : The combination step in question.

How the output shapes the flow of data :

- The return value is a list of 2-tuples :
  - The first entry is a list of batch IDs corresponding to the input steps.
  - The second entry is the corresponding batch ID output by the combination step.
- By default, previously submitted batches are ignored, there is no need for your method to check for them.
- If you want your sequencer to resubmit such batches, you have to explicitly set `redo` to `True` in your constructor by calling `super().__init__(redo=True)`.

-----

## ADFDataLoader

An **ADF Data Loader** defines what data is passed to your processing function at each step. To define your own ADF Data Loader, you must inherit from the `ADFDataLoader` base class. There are 2 abstract methods that then require defining.

-----

```python
def from_config(cls, config: Dict) -> "ADFDataLoader"
```

Given configuration parameters, return an `ADFDataLoader` instance. If you define a custom `__init__` for this, make sure it calls `super().__init__()`.

-----

```python
def get_ads_args(
    self,
    data_interface: AbstractDataInterface,
    step_in: ADFStep,
    step_out: ADFStep,
    batch_id: str,
    state_handler: AbstractStateHandler,
) -> Tuple[
    List[AbstractDataStructure],
    Dict[str, AbstractDataStructure],
]
```

Input arguments :

- `data_interface` : The data persistance interface from which to load data.
- `step_in` : The input step.
- `step_out` : The output step.
- `batch_id` : The output batch ID.
- `state_handler` : Contains the current processing state.

How the output is passed to your data processing functions :

- The first output is a list of ADSs. These are passed as positional arguments.
- The second output is a dict, for which each entry is passed as a keyword argument.

Simply put, if your `get_ads_args` method returns `args, kwargs`, then these are passed to your processing function `func` simply as :

```python
func(*args, **kwargs)
```

-----

## ADFBatchDependencyHandler

An **ADF Batch Dependency Handler** defines which batches are defined as being _downstream_ of batches in previous steps. This is mainly used to define which downstream batches are also deleted when deleting a particular batch of data. To define your own ADF Batch Dependency Handler, you must inherit from the `ADFBatchDependencyHandler` base class. There are 2 abstract methods that then require defining.

-----

```python
def from_config(cls, config: Dict) -> "ADFBatchDependencyHandler"
```

Given configuration parameters, return an `ADFBatchDependencyHandler` instance. If you define a custom `__init__` for this, make sure it calls `super().__init__()`.

-----

```python
def get_dependent_batches(
    self,
    state_handler: AbstractStateHandler,
    step_in: ADFStep,
    step_out: ADFStep,
    batch_id: str,
) -> List[str]
```

Input arguments :

- `state_handler` : Contains the current processing state.
- `step_in` : The input step.
- `step_out` : The output step.
- `batch_id` : The input batch ID.

The return value represents the list of batch IDs in the output step that are considered dependent on the given batch ID in the input step.

-----

# Processing functions

- [Input signature](#input-signature)
  - [Non-starting step](#non-starting-step)
  - [Combination step](#combination-step)
- [Output signature](#output-signature)
- [Concretization](#concretization)

The processing function signature depends on the specific step configuration. In particular, **data loaders** will modify the expected input arguments, and the presence of downstream reception steps will modify the expected output.

## Input signature

### Non-starting step

For non-starting steps, the input arguments will depend on the output `args` and `kwargs` of the step data loader, in addition to any `func_kwargs` defined in the step configuration. For example, consider the builtin `KwargDataLoader`, that merely passes the current batch of data as a keyword argument, meaning `args` is an empty list and `kwargs` is a single entry dictionary whose key is user defined :

```yaml
name: collection-name
modules:
  - name: flow_config
    import_path: ADF.components.flow_config
  - name: module-alias
    import_path: package.module
flows:
  - name: flow-name-0
    steps:
      - start: landing
        layer: layer-name
        name: landing-step-name
      - layer: layer-name
        name: processing-step-name
        version: version-name
        func:
          load_as: module
          params:
            module: module-alias
            name: foo
        func_kwargs:
          extra_kwarg: kwarg_val
        data_loader:
          module: flow_config
          class_name: KwargDataLoader
          params:
            kwarg_name: custom_kwarg_name
```

In this case, the expected function input signature is :

```python
def foo(custom_kwarg_name: AbstractDataStructure, extra_kwarg: str)
```

The `custom_kwarg_name` argument will contain the ADS passed by our data loader, and the `extra_kwarg` argument will contain the value `kwarg_val` as defined in our flow configuration.

### Combination step

For a combination step, the `args` and `kwargs` are the combination of the outputs of the data loaders of all input steps, plus the data loader of the combination step itself. Again, let's use the `KwargDataLoader` to illustrate this, as well as the `FullDataLoader` which loads all available data for a given step.

```yaml
name: collection-name
modules:
  - name: flow_config
    import_path: ADF.components.flow_config
  - name: module-alias
    import_path: package.module
flows:
  - name: flow-name-0
    steps:
      - start: landing
        layer: layer-name
        name: landing-step-name
  - name: flow-name-1
    steps:
      - start: landing
        layer: layer-name
        name: landing-step-name
  - name: combination-flow
    steps:
      - start: combination
        layer: layer-name
        name: combination-step-name
        version: version-name
        input_steps:
          - flow_name: flow-name-0
            step_name: landing-step
            data_loader:
              module: flow_config
              class_name: KwargDataLoader
              params:
                kwarg_name: custom_kwarg_name_0
          - flow_name: flow-name-1
            step_name: landing-step
            data_loader:
              module: flow_config
              class_name: KwargDataLoader
              params:
                kwarg_name: custom_kwarg_name_0
        func:
          load_as: module
          params:
            module: module-alias
            name: foo
        func_kwargs:
          extra_kwarg: kwarg_val
        data_loader:
          module: flow_config
          class_name: FullDataLoader
          params: {}
```

Each input step data loader will add an entry to the input `kwargs`. The data loader of the combination step itself will load the full data of that same step as the sole entry of our output `args`. Finally, there is also the user defined `extra_kwarg`, defined directly in our flow configuration, that will enrich the input `kwargs`. Putting all of this together, we get the following input signature :

```python
def foo(
    full_data: AbstractDataStructure,
    custom_kwarg_name_0: AbstractDataStructure,
    custom_kwarg_name_1: AbstractDataStructure,
    extra_kwarg: str,
)
```

## Output signature

By default, a processing function must output a single ADS :

```python
def foo(ads: AbstractDataStructure) -> AbstractDataStructure:
    return ads[ads["some_col"] == "some_val"]
```

However, hooking a processing step into a reception step changes the expected output signature of the processing function. Instead of returning a single ADS, it must now return a dictionary whose keys correspond to the reception step keys. Take the following flow configuration as an example :

```yaml
name: collection-name
modules:
  - name: module-alias
    import_path: package.module
flows:
  - name: flow-name
    steps:
      - start: landing
        layer: layer-name
        name: landing-step-name
      - layer: layer-name
        name: processing-step
        func:
          load_as: module
          params:
            module: module-alias
            name: multiple_outputs
  - name: reception-flow-0
    steps:
      - start: reception
        layer: layer-name
        name: reception-step-name
        key: reception-key-0
        input_steps:
          - flow_name: flow-name
            step_name: processing-step
  - name: reception-flow-1
    steps:
      - start: reception
        layer: layer-name
        name: reception-step-name
        key: reception-key-1
        input_steps:
          - flow_name: flow-name
            step_name: processing-step
```

A valid corresponding processing function would then be :

```python
def multiple_outputs(
    ads: AbstractDataStructure,
) -> Dict[str, AbstractDataStructure]:
    return {
        "reception-key-0": ads[ads["col_0"] == 0],
        "reception-key-1": ads[ads["col_0"] != 0],
    }
```

## Concretization

It is possible to define processing functions using familiar "concrete" APIs (such as Pandas, PySpark, raw SQL etc.) using a procedure known as **concretization**. However, this comes at the cost that when the corresponding step is mapped to a layer, that layer must support the chosen concretization. For example, concretizing to a PySpark dataframe will fail for an SQL based layer. Our data flows remain abstract, but they become somewhat constrained in their eventual layer mapping.

To define a processing function as concrete, you may use the `concretize` decorator, which takes as input a concretization type. The decorator will transform all input ADSs into the requested type. It will do so in a nested manner, also transforming ADSs within input lists, tuples, or dictionaries. It also expects that type as the function output type. :

```python
from pandas import DataFrame
from ADF.utils import concretize

@concretize(DataFrame)
def foo(df: DataFrame) -> DataFrame:
    return df.drop_duplicates()
```

There are 2 main use cases for concretization that may be worth the slight trade-off of layer constraint :

- Migrating a non ADF pipeline to ADF, allowing reuse of business logic code.
- Exploiting API specific optimizations, such as `broadcast` for a PySpark dataframe.
