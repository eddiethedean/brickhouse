| Stage | Mean ms/request | Share |
| --- | ---: | ---: |
| SQLite calls | 21.09 | 71.2% |
| Sparkless SQL boundary | 3.38 | 11.4% |
| Sparkless Arrow boundary | 1.60 | 5.4% |
| SQL planning, catalog and Delta metadata (outside SQLite) | 1.42 | 4.8% |
| Result limits and JSON | 0.72 | 2.4% |
| Session open and close (outside SQLite) | 0.07 | 0.2% |
| Other lifecycle, HTTP and client residual | 1.34 | 4.5% |
| **Total, instrumented** | **29.60** | **100%** |
