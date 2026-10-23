| Workload | BrickHouse | MiniLake | Go emulator |
| --- | --- | --- | --- |
| Fresh workspace → first verified 64 KiB file | 297.10 / 339.35 ms | 370.35 / 583.05 ms | 58.49 / 83.58 ms |
| Warm 64 KiB file import + export | 2.74 / 4.94 ms | 2.73 / 4.99 ms | 3.31 / 5.27 ms |
| Warm filtered/grouped SQL over 10,000 Delta rows | 29.95 / 33.19 ms | 2.59 / 3.17 ms | 4.86 / 5.53 ms |
| Idle core API server RSS (median only) | 44.98 MiB | 98.19 MiB | 15.26 MiB |
