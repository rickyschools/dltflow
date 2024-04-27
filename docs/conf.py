# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys

sys.path.insert(0, os.path.abspath("../dltflow"))

project = "dltflow"
copyright = "2024, Ricky Schools"
author = "Ricky Schools"
release = "0.0.1"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration


extensions = [
    "sphinx.ext.napoleon",
    "autodoc2",
    "autoapi.extension",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.autosummary",
    "sphinx_markdown_builder",
    "myst_parser",
    "sphinxcontrib.mermaid",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "venv", "*egg_info*", "*tests*"]

autodoc_default_options = {
    "members": True,
    # The ones below should be optional but work nicely together with
    # example_package/autodoctest/doc/source/_templates/autosummary/class.rst
    # and other defaults in sphinx-autodoc.
    "show-inheritance": True,
    "inherited-members": True,
    "no-special-members": True,
}

# autodoc options
autosummary_generate = True
autosectionlabel_prefix_document = True

# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = True
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = True
napoleon_use_admonition_for_notes = True
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True

# myst settings
myst_heading_anchors = 3

# -- Options for Autodoc2 extension -------------------------------------------
autodoc2_packages = ["../dltflow"]
autodoc2_render_plugin = "myst"
autodoc2_output_dir = "_build/apidocs"

# -- Options for AutoAPI extension -------------------------------------------
autoapi_dirs = ["../dltflow"]

# The master toctree document.
master_doc = "index"
source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}

# myst options
myst_enable_extensions = ["colon_fence", "html_image"]

mermaid_d3_zoom = True

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = ["../docs/_static"]
html_show_sphinx = True
