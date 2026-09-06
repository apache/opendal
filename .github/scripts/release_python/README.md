# Python Release Helpers

The Python release workflow publishes release candidate tags to TestPyPI and
stable tags to PyPI.

Each source release keeps a stable Python version in
`bindings/python/Cargo.toml`. For a tag such as `v0.58.1-rc.5`, `prepare.py`
temporarily changes the PEP 621 metadata in `bindings/python/pyproject.toml`
from a dynamic Cargo version to the PEP 440 version `0.47.5rc5`. The workflow
runs this preparation independently before building the sdist and every wheel.
It does not commit the generated metadata or change the stable release version.

Stable tags skip this preparation, so maturin continues to read the version
from `bindings/python/Cargo.toml`.

After maturin repairs the wheels, `wheels.py` identifies each `libgcc_s` bundled
under `opendal.libs`. It adds the runtime's path, SHA-256, and
`GPL-3.0-or-later WITH GCC-exception-3.1` license to the existing CycloneDX SBOM,
includes the license and exception texts, labels the wheel metadata, and
regenerates `RECORD`. Wheels without bundled libraries remain unchanged.
Unknown bundled libraries require their own provenance and license mapping.

The license texts in `licenses/` come from GCC 12.4.0's
[`COPYING3`](https://github.com/gcc-mirror/gcc/blob/releases/gcc-12.4.0/COPYING3)
and [`COPYING.RUNTIME`](https://github.com/gcc-mirror/gcc/blob/releases/gcc-12.4.0/COPYING.RUNTIME).
The helper records the actual binary hash without inferring a GCC version.
It does not establish the PMC review of runtime combination required by the
[ASF GCC Runtime Library policy](https://www.apache.org/legal/resolved.html#gcc-runtime-library-exception).

Run the unit tests with:

```bash
python3 -m unittest discover -s .github/scripts/release_python -p "test_*.py"
```
