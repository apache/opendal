# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Build the Apache OpenDAL Python API documentation."""

from importlib.metadata import version as package_version
from pathlib import Path
from typing import Any

from docutils import nodes
from sphinx.addnodes import pending_xref
from sphinx.application import Sphinx
from sphinx.environment import BuildEnvironment
from sphinx.ext.intersphinx import resolve_reference_in_inventory

project = "Apache OpenDAL™ Python"
author = "The Apache Software Foundation"
copyright = "The Apache Software Foundation"  # noqa: A001

release = package_version("opendal")
version = release

extensions = [
    "myst_parser",
    "numpydoc",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_design",
]

autosummary_generate = True
autodoc_typehints = "description"
numpydoc_show_class_members = False
myst_heading_anchors = 3
myst_enable_extensions = ["colon_fence"]

exclude_patterns = ["_build", "examples/*_files"]
nitpick_ignore = [
    ("py:class", "NotRequired"),
    ("py:class", "Required"),
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

html_theme = "pydata_sphinx_theme"
html_title = "Apache OpenDAL™ Python"
html_static_path = ["_static"]
html_js_files = ["scroll-page-toc.js"]
html_baseurl = "https://opendal.apache.org/docs/python/"
html_theme_options = {
    "external_links": [
        {
            "name": "OpenDAL documentation",
            "url": "https://opendal.apache.org/docs/",
        },
    ],
    "github_url": "https://github.com/apache/opendal",
    "navigation_with_keys": True,
    "show_toc_level": 2,
    "use_edit_page_button": True,
}
html_context = {
    "default_mode": "auto",
    "github_user": "apache",
    "github_repo": "opendal",
    "github_version": "main",
    "doc_path": "bindings/python/docs",
}


def remove_pyo3_hidden_parameters(
    _app: Sphinx,
    _what: str,
    _name: str,
    _obj: object,
    _options: object,
    signature: str | None,
    return_annotation: str | None,
) -> tuple[str | None, str | None]:
    """Remove PyO3's hidden receiver from rendered method signatures."""
    if signature is not None:
        signature = signature.replace("($cls, ", "(").replace("($self, ", "(")
    return signature, return_annotation


def edit_notebook_source(
    app: Sphinx,
    pagename: str,
    _templatename: str,
    context: dict[str, Any],
    _doctree: object,
) -> None:
    """Point the theme’s edit link at the notebook behind a generated page."""
    if pagename.startswith("examples/"):
        notebook = Path(app.env.doc2path(pagename)).with_suffix(".ipynb")
        if notebook.is_file():
            context["page_source_suffix"] = ".ipynb"


def resolve_config_timedelta(
    _app: Sphinx,
    env: BuildEnvironment,
    node: pending_xref,
    content: nodes.Element,
) -> nodes.reference | None:
    """Resolve the short timedelta annotation in generated service configs."""
    if node.get("py:module") == "opendal.config" and node["reftarget"] == "timedelta":
        qualified = node.copy()
        qualified["reftarget"] = "datetime.timedelta"
        return resolve_reference_in_inventory(env, "python", qualified, content)
    return None


def setup(app: Sphinx) -> None:
    """Register documentation build hooks."""
    app.connect("autodoc-process-signature", remove_pyo3_hidden_parameters, priority=0)
    app.connect("html-page-context", edit_notebook_source)
    app.connect("missing-reference", resolve_config_timedelta)
