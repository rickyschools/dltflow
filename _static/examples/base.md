## Code


### Base Pipeline Code
All pipelines leverage a `base_pipeline.py` file for the example that provides a common `PipelineBase` class that each
pipeline inherits from. This is provided to help standardize how things like `spark`, `logging`, and `configuration` are 
initialized in pipelines. This pattern largely follows `dbx`'s documentation and task templating guide.


:::{literalinclude} ../../../examples/pipelines/base_pipeline.py
:::

Now that we understand that what `base_pipeline.py` does, lets get into our sample code.