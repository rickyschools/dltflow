# Streaming Append Example

This article shows how to get your hands dirty end-to-end with `dltflow`. In this sample, we'll be going through the
following steps:

- [Code](#code)
- [Configuration](#configuration)
- [Workflow Spec](#workflow-spec)
- [Deployment](#deployment)

:::{include} ./base.md
:::

### Example Pipeline Code

For this example, we will show a simple example with a queue streaming reader.

- Import a `DLTMetaMixin` from `dltflow.quality` and will tell our sample pipeline to inherit from it.
- Generate the example data on the fly and put it into a python queue.
- We will transform it by coercing data types.

You should see that there are no direct calls to `dlt`. This is the beauty and intentional simplicity `dltflow`. It does
not want to get in your way. Rather, it really wants you to focus on your transformation logic to help keep your code
simple and easy to share with other team members.

:::{literalinclude} ../../../examples/pipelines/streaming_append.py
:::

## Configuration

Now that we have our example code, we need to write our configuration to tell the `DLTMetaMixin` how wrap our codebase.

Under the hood, `dltflow` uses `pydantic` to create validation for configuration. When working with `dltflow`, it
requires your configuration to adhere to a specific structure. Namely, file should have the following sections:

- `reader`: This is helpful for telling your pipeline where to read data from.
- `writer`: Used to define where your data is written to after being processed.
- `dlt`: Defines how `dlt` will be used in the project. We use this to dynamically wrap your code with `dlt` commands.

With this brief overview out of the way, lets review our configuration for this sample.

:::{literalinclude} ../../../examples/conf/streaming_append_dlt_pipeline.yml
:::

The `dlt` section has the following keys, though this configuration can also be a list of `dlt` configs.

- `func_name`: The name of the function/method we want `dlt` to decorate.
- `kind`: Tells `dlt` if this query should be materialized as a `table` or `view`
- `expectation_action`: Tells `dlt` how to handle the expectations. `drop`, `fail`, and `allow` are all supported.
- `expectations`: These are a list of constraints we want to apply to our data.
- `is_streaming`: This tells `dltflow` this is a streaming query.
- `append_config`: This tells `dltflow` we're in a streaming append and fills out necessary `dlt` params.

## Workflow Spec

Now that we've gone through the code and configuration, we need to start defining the workflow that we want to deploy
to Databricks so that our pipeline can be registered as a DLT Pipeline. This structure largely follows the [Databricks
Pipeline API]() with the addition of a `tasks` key. This key is used during deployment for transitioning your python
module into a Notebook that can be deployed as a DLT Pipeline.

:::{literalinclude} ../../../examples/workflows/streaming_dlt_pipeline.yml
:::

## Deployment

We're at the final step of this simple example. The last piece of the puzzle here is that we need to deploy our assets
to a Databricks workspace. To do so, we'll use the `dltflow` cli.

:::{literalinclude} ../../../examples/deployment/deploy_streaming_append_pipeline.sh
:::
