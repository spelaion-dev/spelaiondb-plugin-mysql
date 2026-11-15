# spelaiondb-plugin-mysql

MySQL and MariaDB for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), the desktop
database client: the engines' driver, served to SpelaionDB as a plugin.

SpelaionDB's engines are programs it starts and talks to over their standard input and output
(its ADR-041). This is the one for MySQL and MariaDB: one program serves both. It is built here, in its own repository, and
SpelaionDB pins by hash the exact release it was tested with (its ADR-042): you do not install
this by hand, it comes with SpelaionDB.

- **Protocol:** [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
  version 1.0. Experimental until SpelaionDB 1.0.
- **Engines:** `mysql` and `mariadb`. `mysql_async` over rustls (patched:
  `vendor/mysql_async/PATCHED.md`).
- **License:** Apache-2.0.

## Working on it

```bash
ci/services.sh
```

starts the test servers (MySQL 8.4 and MariaDB 11.4 in Docker, with their certificates, and
TiDB and GreptimeDB, which speak MySQL's protocol), and

```bash
SPELAION_TEST_MYSQL=127.0.0.1:13306 SPELAION_TEST_MARIADB=127.0.0.1:13307 \
  SPELAION_TEST_TIDB=127.0.0.1:14000 SPELAION_TEST_GREPTIME=127.0.0.1:14002 cargo test
```

runs the type zoo and the protocol's conformance set against this plugin's own program. A
server whose variable is not set is skipped, which does not count as tested.

To try a build with SpelaionDB, point a development build of it at the program
(`SPELAION_DEV_PLUGIN_MYSQL` and `SPELAION_DEV_PLUGIN_MARIADB`, both
`/path/to/target/debug/spelaiondb-plugin-mysql`, in SpelaionDB's repository; a release build
ignores those variables).

`AGENTS.md` has the rules. Reports and questions go to
[SpelaionDB's issues](https://github.com/spelaion-dev/spelaiondb/issues).
