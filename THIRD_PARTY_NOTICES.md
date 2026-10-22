# Third-party notices

This file retains notices for third-party packages included in BrickHouse distributions. It does not replace each package's full license terms.

## Pinned direct dependencies

These are the exact top-level dependencies declared for the base distribution and the optional `notebooks` extra. BrickHouse does not vendor their implementation code. The version-pinned license texts are retained under `THIRD_PARTY_LICENSES/`; PyArrow's upstream `NOTICE.txt` is included because it names additional components and notices.

| Distribution | Version | Observed license | Retained notice |
|---|---:|---|---|
| `sparkless` | 4.15.2 | MIT | [`sparkless-4.15.2-LICENSE.txt`](THIRD_PARTY_LICENSES/sparkless-4.15.2-LICENSE.txt) |
| `deltalake` | 1.2.1 | Apache-2.0 | [`deltalake-1.2.1-LICENSE.txt`](THIRD_PARTY_LICENSES/deltalake-1.2.1-LICENSE.txt) |
| `pyarrow` | 23.0.1 | Apache-2.0 | [`pyarrow-23.0.1-LICENSE.txt`](THIRD_PARTY_LICENSES/pyarrow-23.0.1-LICENSE.txt) and [`pyarrow-23.0.1-NOTICE.txt`](THIRD_PARTY_LICENSES/pyarrow-23.0.1-NOTICE.txt) |
| `sqlglot` | 30.24.0 | MIT | [`sqlglot-30.24.0-LICENSE.txt`](THIRD_PARTY_LICENSES/sqlglot-30.24.0-LICENSE.txt) |
| `ipykernel` | 7.4.0 | BSD-3-Clause | [`ipykernel-7.4.0-LICENSE.txt`](THIRD_PARTY_LICENSES/ipykernel-7.4.0-LICENSE.txt) |
| `jupyter-client` | 8.10.0 | BSD-3-Clause | [`jupyter-client-8.10.0-LICENSE.txt`](THIRD_PARTY_LICENSES/jupyter-client-8.10.0-LICENSE.txt) |
| `nbclient` | 0.11.0 | BSD-3-Clause | [`nbclient-0.11.0-LICENSE.txt`](THIRD_PARTY_LICENSES/nbclient-0.11.0-LICENSE.txt) |
| `nbformat` | 5.11.1 | BSD-3-Clause | [`nbformat-5.11.1-LICENSE.txt`](THIRD_PARTY_LICENSES/nbformat-5.11.1-LICENSE.txt) |

The archived `sparkless-4.15.1-LICENSE.txt` remains for the superseded owner-review candidate; the current pinned release is 4.15.2. The notebook distributions above are optional and are not installed by the base extra. Transitive dependency notices and platform-native payloads must be checked against each resolved candidate environment as part of B023.2/B023.3. `databricks-sdk` and the CLI/Terraform/SQL clients are test/reference drivers, not BrickHouse runtime dependencies.

BrickHouse is licensed under Apache-2.0; see the root [`LICENSE`](LICENSE). Each third-party dependency retains its own license and notices listed here.

## SQLGlot 30.24.0

SQLGlot is distributed under the MIT License.

Copyright (c) 2026 Toby Mao

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

The pinned universal wheel `sqlglot-30.24.0-py3-none-any.whl` has SHA-256
`9488828cc018453b7a3ba01e65d2749c9a731314c14583695163ca1f358203fe`.
The pinned source archive has SHA-256
`6451aabd1c9994d0f73355c962a5eaf9258381b3f2554e53db916cd9a605552a`.
The same artifact hashes and source metadata are recorded in
[`planning/oss-reuse.json`](planning/oss-reuse.json); neither archive's source
is copied into BrickHouse.

## Optional Jupyter notebook runtime

The optional `brickhouse[notebooks]` extra pins `ipykernel` 7.4.0,
`jupyter-client` 8.10.0, `nbclient` 0.11.0, and `nbformat` 5.11.1. Their
BSD 3-Clause license notices are retained below. The hash-locked wheels and
versions are recorded in `requirements/dev.txt`; the B017.1 conformance report
records the exact local runtime exercised.

Copyright notices:

- Copyright (c) 2015, IPython Development Team (`ipykernel`).
- Copyright (c) 2020-, Jupyter Development Team (`nbclient`).
- Copyright (c) 2001-2015, IPython Development Team (`jupyter-client`, `nbformat`).
- Copyright (c) 2015-, Jupyter Development Team (`jupyter-client`, `nbformat`).

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors
   may be used to endorse or promote products derived from this software
   without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
