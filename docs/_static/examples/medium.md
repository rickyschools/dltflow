# Medium Example - Multi-Method Decoration

This article intends to depict how to use `dltflow` in a slightly more complex example. The core components of this
example will indent to show how `dltflow` can be configured to wrap multiple functions in a single python module.

Our structure for this article will follow that of our simple example.

- [Code](#code)
- [Configuration](#configuration)
- [Workflow Spec](#workflow-spec)
- [Deployment](#deployment)

:::{include} ./base.md
:::

### Example Pipeline Code

For this example we will have a slightly more complex pipeline. In this code base, we will:

- Import a `DLTMetaMixin` from `dltflow.quality` and will tell our sample pipeline to inherit from it.
- Create sample data in memory.
- We'll `transform` that data in two steps.
- In an intermediate step, we will ensure people's ages are between 10 and 100.
- In a following step, we will get a count of people in our sample dataset by age range.
- Both the intermediate and final transformations will be registered in the `dlt` pipeline.
  - This will be as a `view` and a `materialized table` respectively.

:::{literalinclude} ../../../examples/pipelines/medium_dlt_pipeline_with_expectations.py
:::

## Configuration


Now that we have our example code, we need to write our configuration to tell the `DLTMetaMixin` how wrap our codebase.

Under the hood, `dltflow` uses `pydantic` to create validation for configuration. When working with `dltflow`, it
requires your configuration to adhere to a specific structure. Namely, file should have the following sections:

- `reader`: This is helpful for telling your pipeline where to read data from.
- `writer`: Used to define where your data is written to after being processed.
- `dlt`: Defines how `dlt` will be used in the project. We use this to dynamically wrap your code with `dlt` commands.

With this brief overview out of the way, lets review our configuration for this sample.

:::{literalinclude} ../../../examples/conf/medium_dlt_pipeline_with_expectations.yml
:::

The `dlt` section in this example is a list of `DLTConfig`'s.

- `func_name`: The name of the function/method we want `dlt` to decorate.
- `kind`: Tells `dlt` if this query should be materialized as a `table` or `view`
- `expectation_action`: Tells `dlt` how to handle the expectations. `drop`, `fail`, and `allow` are all supported.
- `expectations`: These are a list of constraints we want to apply to our data.

:::{include} ./simple.md
:start-line: 51
:end-line: 58
:::

:::{literalinclude} ../../../examples/workflows/medium_dlt_pipeline_with_expectations.yml
:::

:::{include} ./simple.md
:start-line: 61
:end-line: 66
:::

:::{literalinclude} ../../../examples/deployment/deploy_medium_dlt_pipeline_with_expectations.sh
:::
