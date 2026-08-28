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

from sphinx.application import Sphinx

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


def setup(app: Sphinx) -> None:
    """Register documentation build hooks."""
    app.connect("autodoc-process-signature", remove_pyo3_hidden_parameters, priority=0)
