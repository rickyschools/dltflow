# `dltflow`
![Coverage](./coverage/badges.svg) ![Static Badge](https://img.shields.io/badge/python-3.9%2C_3.10%2C_3.11%2C-blue) ![Static Badge](https://img.shields.io/badge/pyspark-3.4%2C_3.5-blue)

`dltflow` is a Python package that provides authoring utilities and CD patterns for Databricks' DLT product. It intends
make writing and deploying DLT code and pipelines to Databricks as easy as possible.

**NOTE!!!**: This project is in early development. APIs and features are subject to change. Use with caution.

## Why `dltflow`?

Valid question. Here are a few reasons why you might want to use `dltflow`:

- **DLT Pipelines in Python Modules**:
    - DLT Pipelines are a newer feature in Databricks bring data quality, lineage, and observability to your data
      pipelines.
    - DLT, as documented, can only be instrumented via Notebooks.
    - `dltflow` provides a way to author DLT pipelines in Python modules - which leverage meta programming patterns (via
      configuration) and deploy them to Databricks.
- **Deployment of DLT Pipelines**:
    - `dltflow` provides a way to deploy Python Modules as DLT pipelines to Databricks.
    - It builds on the shoulders of `dbx` and `dlt-meta` projects to provide a seamless deployment experience.

This project heavily is inspired by [dbx](https://github.com/databrickslabs/dbx)
and [dlt-meta](https://github.com/databrickslabs/dlt-meta) projects.

The reason for a seperate project is because:

- Generally DLT pipelines are only SQL or Python, and have to live in Notebooks.
- `dbx` is a great tool for deploying Python modules to Databricks, but it doesn't support DLT Pipelines for python
  modules.
- `dlt-meta` has some deployment features that are adopted into this repo.
- `dab` is a new deployment tool by databricks, but suffers from the same problem as `dbx`.

## Getting Started with `dltflow`

### Installation

```bash
pip install dltflow
```

### Initialization

`dltflow`'s audience is for developers who are familiar with Databricks, write PySpark, and want to instrument their
data assets via DLT pipelines.

**Project Initialization and Templating**:

`dltflow` provides a cli command to initialize a project. This command will create a `dltflow.yml` file in the root of
your project. Optionally, you can start your project with a template.

```bash
dltflow init --help
>>> Usage: dltflow init [OPTIONS]

  Initialize the project with a dltflow.yml file.
  Optionally start your project with a template.

  Options:
  -p, --profile TEXT         Databricks profile to use
  -n, --project-name TEXT    Name of the project
  -c, --config-path TEXT     Path to configuration directory
  -w, --workflows-path TEXT  Path to workflows directory
  -t, --build-template       Create a templated project?
  -o, --overwrite            Overwrite existing config file  [default: True]
  -d, --shared               DBFS location to store the project  [default:
                             True]
  --help                     Show this message and exit.
```

Simply running `dltflow init` will bring up a set of prompts which is help you fill out the options listed above. As a
final
question in the prompts, you will be asked if you want to start your project with a template. If you answer `yes`, a
template project will be created in the current working directory.

The structure will be as follows:

```text
git-root/
    my_project/  # code goes here.
    conf/  # configuration to drive your pipelines.
    workflows/  # json or yml definitions for workflows in databricks.
    dltflow.yml  # dltflow config file.
    setup.py  # setup file for python packages.
    pyproject.toml  # pyproject file for python packages.
```

### Authoring

Writing DLT pipelines in Python modules is as simple as writing a Python module. `dltflow` currently is customized to be
integrated with PySpark where pipelines are represented as Python classes.

It integrates relatively seamlessly with OOP-style authoring, as it exposes a `DLTMetaMixin` class. What really powers
`dltflow` is the pattern of configuration-driven meta programming. The configuration approach, along with the easy of
integrating the `DLTMetaMixin` class, intends to allow developers to keep code simple without having to worry about the
complexity of DLT.

Let's take a look at an example; we'll break things down after taking a quick look.

#### Pipeline Configuration

pipeline-code-config.json

```json
{
  "reader": {
    "configuration": "goes_here"
  },
  "writer": {
    "configuration": "goes_here"
  },
  "dlt": {
    "func_name": "orchestrate",
    "kind": "table",
    "expectations": [
      {
        "name": "check_addition",
        "constraint": "result < 10"
      }
    ],
    "expectation_action": "allow"
  }
}
````

The above json file intends to represent a VCS controlled parameter file. It highlights specific components that we can
use to drive our codebase. For the purposes of this repo, we'll be focusing on the `dlt` section of the configuration.
There's a few key noteables.

- `func_name`: This tells `dltflow` what function to decorate with DLT instrumentation.
- `kind`: This tells `dltflow` what kind of DLT object to materialize.
    - Expected values are `table`, `view`.
- `expectations`: This is a list of expectations that you want to enforce on your data.
- `expectation_action`: This is the action to take if an expectation is violated.
    - Expected values are `allow`, `drop`, and `fail`.

Not listed above are a few other options:

- is_streaming_table: This is a boolean flag that tells `dltflow` if the DLT object is a streaming table.
- append_config: This is a dictionary that allows you to perform append based streaming DLT instrumentation patterns.
- apply_cdc_config: This is a dictionary that allows you to perform CDC based streaming DLT instrumentation patterns.

For more details on `dlt` documentation, please refer to
the [Databricks documentation on DLT](https://docs.databricks.com/data/delta/delta-live-tables.html).

#### Pipeline Authoring

Now that we have our configuration, we'll write a very simple pipeline that reads a DataFrame, transforms it, and writes
it to a table. We'll use the configuration to drive the DLT instrumentation.

Based on the configuration, our code has the following technical requirements:

- The pipeline must have a method called `orchestrate`. `DLTMetaMixin` will decorate this method.
- The pipeline expects a column called `result` be present in the Dataframe on the return value of `orchestrate`.

simple-dltflow-pipeline.py

```python
import sys
import pathlib
from typing import Dict, Any
from argparse import ArgumentParser

import yaml
from pyspark.sql import DataFrame
from dltflow.quality import DLTMetaMixin


class MyPipeline(DLTMetaMixin):
    def __init__(self, spark):
        self.spark = spark

    def read(self) -> DataFrame:
        return self.spark.createDataFrame([(1, 2), (5, 8), (1, 5)], ["col1", "col2"])

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("result", df["col1"] + df["col2"])

    def write(self, df):
        df.write.saveAsTable("my_table")

    def orchestrate(self):
        df = self.read()
        df = self.transform(df)
        return df

    @staticmethod
    def _get_conf_file():
        """Uses the arg parser to extract the config location from cli."""
        p = ArgumentParser()
        p.add_argument("--conf-file", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace.conf_file

    @staticmethod
    def _read_config(conf_file) -> Dict[str, Any]:
        config = yaml.safe_load(pathlib.Path(conf_file).read_text())
        return config
```

Now that we have our configuration and our pipeline we can try and execute the pipeline.

**__NOTE__**:

- The above code is a simple example. It is not intended to be a complete example.
- DLT Pipelines can only be executed in Databricks DLT Pipelines.
- Running locally is possible with the `databricks-dlt` stub package - however there's some caveats to this approach.
  Specifically any transformations where we rely on DLT to enforce expectations like `expect or drop`
  or `expect all or drop`.

```bash
python pipelines/simple-dltflow-pipeline.py --conf-file conf/pipeline-code-config.json
```

#### Databricks DLT Pipeline Definitions (Workflow Specs)

In `dltflow`, workflows are deployed to the databricks workspace using a modified version of the Databricks Pipeline
API definition. The workflow spec defined in `dltflow` basically is parsed and converted to a Databricks Pipeline for
upload by the `dbx` Workflow Deployment Manager.

The following is an example of a workflow spec:

```json
{
  "dev": {
    "workflows": [
      {
        "name": "dlflow-example-pipeline",
        "storage": "/mnt/datalake/experiment/dltflow-samples/dlt/simple",
        "target": "dltflow-samples",
        "development": "true",
        "edition": "ADVANCED",
        "continuous": "false",
        "clusters": [
          {
            "label": "default",
            "node_type_id": "Standard_DS3_v2",
            "autoscale": {
              "min_workers": 1,
              "max_workers": 2,
              "mode": "ENHANCED"
            }
          }
        ],
        "pipeline_type": "WORKSPACE",
        "data_sampling": false,
        "tasks": {
          "items": [
            {
              "python_file": "samples/pipelines/simple-dltflow-pipeline.py",
              "parameters": [
                "--conf",
                "conf/pipeline-code-config.json"
              ]
            }
          ],
          "dependencies": [
            {
              "pypi": {
                "package": "pyspark"
              }
            }
          ]
        }
      }
    ]
  }
}

```

The `tasks` key is what's custom to this job spec definition. You can define `n` python files and dependencies perform
workflow. When materialized, each workflow translates to a single Notebook being generated and deploy to the Databricks
workspace.

The same configuration should be possible to express as a `yaml` file.

```yaml
dev:
  workflows:
    - name: "dltflow-example-pipeline"
      storage: "/mnt/datalake/experiment/dltflow-samples/dlt/simple"
      target: "dltflow-samples"
      development: "true"
      edition: "ADVANCED"
      continuous: "false"
      clusters:
        - label: "default"
          node_type_id: Standard_DS3_v2"
          autoscale:
            min_workers: 1
            max_workers: 2
            mode: "ENHANCED"
      pipeline_type: "WORKSPACE"
      data_sampling: false
      tasks:
        items:
          - python_file: "samples/pipelines/simple-dltflow-pipeline.py"
            parameters:
              - "--conf"
              - "conf/pipeline-code-config.json"
        dependencies:
          - whl: "/dbfs/private-site-packages/dltflow-0.0.1b0-py3-none-any.whl"
          - pypi:
              package: "pyspark"
```

### Deployment

Now that we have our pipeline, our pipeline configuration, and our DLT pipeline job spec, we can deploy the pipeline to
Databricks. To do so, we need to use `dltflow`'s `deploy` command from the CLI.

Generally `dltflow` tries to follow the same pattern as `dbx` for deployment. The `deploy` command will look for
a `dltflow.yml` file in the root of the project. This file should contain the necessary configurations for deployment.
See [Initialization docs](#initialization) for more information on the topic.

```bash
dltflow deploy-py-dlt --help
>>> Usage: dltflow deploy-py-dlt [OPTIONS]

  Deploy a DLT pipeline.

Options:
  --deployment-file TEXT  [required]
  --environment TEXT      [required]
  --as-individual         Overrides project settings. Useful for developers as
                          their experimenting with getting their code fully
                          function. The impact of this flag is that any
                          derived DLT pipelines created with have a prefix
                          name of [{profile}_{user_name}] -- this is to not
                          overwrite any existing pipelines with logic that is
                          not yet fully baked..
  --help                  Show this message and exit.

```

And to tie together the full example, here's how we can deploy our example pipeline.

```bash
dltflow deploy-py-dlt --deployment-file conf/dlt/test.json --environment dev
```
