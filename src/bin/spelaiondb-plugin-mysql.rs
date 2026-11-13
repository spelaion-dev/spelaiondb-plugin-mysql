//! MySQL and MariaDB as an engine plugin of SpelaionDB (ADR-041): this crate's driver, served
//! over the plugin protocol on the program's standard input and output. One program serves
//! both engines.

use std::sync::Arc;

use spelaion_driver_mysql::MysqlDriver;
use spelaion_plugin_sdk::{Plugin, run};

#[tokio::main]
async fn main() {
    run(Plugin::new(
        "spelaiondb-plugin-mysql",
        env!("CARGO_PKG_VERSION"),
        vec![
            Arc::new(MysqlDriver::mysql()),
            Arc::new(MysqlDriver::mariadb()),
        ],
    ))
    .await
}
