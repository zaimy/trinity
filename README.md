# trinity

A tool to synchronize workflows (DAGs) between Codebase, Cloud Storage and Airflow metadata.

## Usage

```sh
$ export GOOGLE_APPLICATION_CREDENTIALS="[PATH]" # https://cloud.google.com/storage/docs/reference/libraries#setting_up_authentication
$ cd PATH_TO_YOUR_CODEBASE
$ trinity --src=DAGS_DIRECTORY --bucket=BUCKET_NAME --composer-env=COMPOSER_ENV_NAME
```

**Note:**
  Since Airflow's REST API is experimental, trinity currently uses the [Cloud SDK](https://cloud.google.com/sdk/) to access Airflow metadata. Setup and authentication are required in advance.

## Behavior

trinity synchronizes the workflows (DAGs) with the codebase as master.

- Add: Workflows that exist only in the codebase are uploaded to Cloud Storage and registered in Airflow.
- Delete: Workflows that only exist in Cloud Storage will be deleted from Cloud Storage and Airflow.
- Update: Workflows that exist in both the codebase and Cloud Storage are compared for hash values*, and if there is a difference, the Cloud Storage workflow is replaced.

*When trinity is executed, a hash value representing the workflow definition is written to .trinity. This file does not need to be under version control.

## Directory Stracture

```
Some Codebase
└ Some directory
　 ├ workflowA
　 │ ├ main.py
　 │ ├ foo.sql
　 │ └ .trinity
　 └ workflowB
　 　 ├ main.py
　 　 ├ bar.ini
　 　 └ .trinity
```

trinity assumes a directory structure in which subdirectories exist for each workflow.

## Installation

```sh
$ go get github.com/zaimy/trinity/...
```

## License

[MIT](https://github.com/zaimy/trinity/blob/master/LICENSE)

## Author

[zaimy](https://github.com/zaimy)
