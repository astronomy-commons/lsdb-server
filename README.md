# lsdb-server

![](https://github.com/schwarzam/lsdb-server/actions/workflows/build.yml/badge.svg)
![](https://github.com/schwarzam/lsdb-server/actions/workflows/codecov.yml/badge.svg)
[![codecov](https://codecov.io/gh/Schwarzam/lsdb-server/graph/badge.svg?token=WFB32324PK)](https://codecov.io/gh/Schwarzam/lsdb-server)

---

A lightweight, fast and easy to use server for the [lsdb server test branch](https://github.com/Schwarzam/lsdb/tree/server-test).

---

#### TODO list:

- [ ] Accept lower case column names
- [ ] Get columns by index
- [ ] Exclude columns by name
- [ ] Fix `_hipscat_index` parquet schema

### Configuring the server

`lsdb-server` works with [lsdb server test branch](https://github.com/Schwarzam/lsdb/tree/server-test) if you have the hips partitioned catalogs in your server. 

Generate the hips using [hipscat-import](https://lsdb.readthedocs.io/en/latest/tutorials/import_catalogs.html). 

---

### Running the server

To run the server from source, install rust and run:

```bash
cargo run --release
```

---

Point nginx to the directory containing the hips and just point requests with args to the server.

```nginx

server {
    listen 80;

    ...
    location /hips {
        # folder or parent folder containing the HiPSCat
        alias /path/to/hips;

        if ($args) {
            # This is the lsdb_server ip
            proxy_pass http://localhost:5000; 
        }
    }
    ...
}

```
---

