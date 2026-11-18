//! A relation's properties, partitions and dependencies (SpelaionDB's SDB-393), from MySQL's
//! and MariaDB's `information_schema`.
//!
//! Dependencies are what the catalog records: foreign keys both ways, the triggers on a table,
//! and the tables and functions a view reads (`view_table_usage` and `view_routine_usage`,
//! MySQL 8.0.13 and later). MariaDB, and servers that speak MySQL's protocol without those
//! tables, do not record what a view reads: views are then among the untracked kinds. What a
//! routine's or a trigger's body touches is never recorded.

use spelaion_driver_api::{
    Dependencies, DependencyInfo, DependencyKind, DriverError, ObjectProperty, PartitionInfo,
    Partitioning, Result, property,
};

use crate::MysqlConnection;

type Rows = Vec<Vec<Option<String>>>;

fn text(row: &[Option<String>], i: usize) -> String {
    row.get(i).cloned().flatten().unwrap_or_default()
}

fn opt(row: &[Option<String>], i: usize) -> Option<String> {
    row.get(i).cloned().flatten().filter(|s| !s.is_empty())
}

/// A catalog table this server does not have (MariaDB's missing `view_table_usage`).
fn missing_table(error: &DriverError, table: &str) -> bool {
    let DriverError::Database { message, .. } = error else {
        return false;
    };
    let message = message.to_ascii_lowercase();
    message.contains(table)
        && (message.contains("unknown table") || message.contains("doesn't exist"))
}

fn missing(schema: &str, name: &str) -> DriverError {
    DriverError::Other(format!("there is no table or view {schema}.{name}"))
}

pub(crate) async fn properties(
    conn: &MysqlConnection,
    schema: &str,
    name: &str,
) -> Result<Vec<ObjectProperty>> {
    let params = || vec![schema.to_string(), name.to_string()];
    let rows = conn
        .rows(
            "select t.table_type, t.engine, t.table_collation, c.character_set_name,
                    t.create_time, t.update_time, t.row_format, t.create_options
             from information_schema.tables t
             left join information_schema.collations c on c.collation_name = t.table_collation
             where t.table_schema = ? and t.table_name = ?",
            params(),
        )
        .await?;
    let row = rows.first().ok_or_else(|| missing(schema, name))?;
    let mut out = Vec::new();
    let mut push = |name: &str, value: Option<String>| {
        if let Some(value) = value {
            out.push(ObjectProperty {
                name: name.into(),
                value,
            });
        }
    };
    if text(row, 0).contains("VIEW") {
        // A view's owner is its definer; how it runs is its own.
        let views = conn
            .rows(
                "select definer, security_type, check_option from information_schema.views
                 where table_schema = ? and table_name = ?",
                params(),
            )
            .await?;
        if let Some(view) = views.first() {
            push(property::OWNER, opt(view, 0));
            push("security_type", opt(view, 1));
            push("check_option", opt(view, 2).filter(|o| o != "NONE"));
        }
        return Ok(out);
    }
    push(property::ENGINE, opt(row, 1));
    push(property::COLLATION, opt(row, 2));
    push(property::CHARSET, opt(row, 3));
    push(property::CREATED, opt(row, 4));
    push(property::UPDATED, opt(row, 5));
    push("row_format", opt(row, 6));
    push(property::OPTIONS, opt(row, 7));
    Ok(out)
}

/// How a partition's rows are told: `VALUES LESS THAN (…)` for a range, `VALUES IN (…)` for a
/// list; hashes and keys have no bound to write.
fn bound(method: &str, description: Option<String>) -> Option<String> {
    let description = description?;
    if method.starts_with("RANGE") {
        Some(format!("VALUES LESS THAN ({description})"))
    } else if method.starts_with("LIST") {
        Some(format!("VALUES IN ({description})"))
    } else {
        None
    }
}

pub(crate) async fn partitions(
    conn: &MysqlConnection,
    schema: &str,
    name: &str,
) -> Result<Option<Partitioning>> {
    let rows = match conn
        .rows(
            "select partition_name, partition_method, partition_expression,
                    partition_description, table_rows, subpartition_name
             from information_schema.partitions
             where table_schema = ? and table_name = ? and partition_name is not null
             order by partition_ordinal_position, subpartition_ordinal_position",
            vec![schema.to_string(), name.to_string()],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) if missing_table(&e, "partitions") => return Ok(None),
        Err(e) => return Err(e),
    };
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let method = text(first, 1);
    let mut partitions: Vec<PartitionInfo> = Vec::new();
    for row in &rows {
        let partition = text(row, 0);
        let estimate: Option<i64> = opt(row, 4).and_then(|n| n.parse().ok());
        match partitions.last_mut() {
            // The next subpartition of the same partition: its rows add up.
            Some(last) if last.name == partition => {
                last.estimated_rows = match (last.estimated_rows, estimate) {
                    (Some(a), Some(b)) => Some(a + b),
                    (a, b) => a.or(b),
                };
            }
            _ => partitions.push(PartitionInfo {
                schema: schema.to_string(),
                name: partition,
                bound: bound(&method, opt(row, 3)),
                estimated_rows: estimate,
                subpartitioned: opt(row, 5).is_some(),
            }),
        }
    }
    Ok(Some(Partitioning {
        method: method.to_ascii_lowercase(),
        key: text(first, 2),
        partitions,
    }))
}

fn kind_order(kind: DependencyKind) -> u8 {
    match kind {
        DependencyKind::Table => 0,
        DependencyKind::View => 1,
        DependencyKind::MaterializedView => 2,
        DependencyKind::ForeignKey => 3,
        DependencyKind::Trigger => 4,
        DependencyKind::Function => 5,
        DependencyKind::Procedure => 6,
        DependencyKind::Sequence => 7,
        DependencyKind::Type => 8,
        DependencyKind::Index => 9,
        DependencyKind::Other => 10,
    }
}

fn sorted(mut list: Vec<DependencyInfo>) -> Vec<DependencyInfo> {
    list.sort_by(|a, b| {
        (kind_order(a.kind), &a.schema, &a.name, &a.detail).cmp(&(
            kind_order(b.kind),
            &b.schema,
            &b.name,
            &b.detail,
        ))
    });
    list.dedup();
    list
}

/// A foreign key being read: the table on the side shown, its name, and its columns on each side.
type Key = (String, String, String, Vec<String>, Vec<String>);

/// Rows of (schema, table, key, column, other table's schema, other table, other column),
/// ordered by key and position, as one dependency per key: `name (cols) → (cols)`.
fn keys(rows: &Rows, kind: DependencyKind, other_side: bool) -> Vec<DependencyInfo> {
    let mut out: Vec<Key> = Vec::new();
    for row in rows {
        let (schema, table) = if other_side {
            (text(row, 4), text(row, 5))
        } else {
            (text(row, 0), text(row, 1))
        };
        let key = text(row, 2);
        match out.last_mut() {
            Some(last) if last.0 == schema && last.1 == table && last.2 == key => {
                last.3.push(text(row, 3));
                last.4.push(text(row, 6));
            }
            _ => out.push((schema, table, key, vec![text(row, 3)], vec![text(row, 6)])),
        }
    }
    out.into_iter()
        .map(|(schema, name, key, from, to)| DependencyInfo {
            schema,
            name,
            kind,
            detail: Some(format!("{key} ({}) → ({})", from.join(", "), to.join(", "))),
        })
        .collect()
}

pub(crate) async fn dependencies(
    conn: &MysqlConnection,
    schema: &str,
    name: &str,
) -> Result<Option<Dependencies>> {
    let params = || vec![schema.to_string(), name.to_string()];
    let found = conn
        .rows(
            "select table_type from information_schema.tables
             where table_schema = ? and table_name = ?",
            params(),
        )
        .await?;
    let is_view = found
        .first()
        .ok_or_else(|| missing(schema, name))
        .map(|row| text(row, 0).contains("VIEW"))?;
    let mut uses = Vec::new();
    let mut used_by = Vec::new();
    let mut untracked = vec![DependencyKind::Function, DependencyKind::Procedure];

    // Foreign keys, both ways.
    let key_columns = "table_schema, table_name, constraint_name, column_name, \
                       referenced_table_schema, referenced_table_name, referenced_column_name";
    let outgoing = conn
        .rows(
            &format!(
                "select {key_columns} from information_schema.key_column_usage
                 where table_schema = ? and table_name = ? and referenced_table_name is not null
                 order by constraint_name, ordinal_position"
            ),
            params(),
        )
        .await?;
    uses.extend(keys(&outgoing, DependencyKind::Table, true));
    let incoming = conn
        .rows(
            &format!(
                "select {key_columns} from information_schema.key_column_usage
                 where referenced_table_schema = ? and referenced_table_name = ?
                 order by table_schema, table_name, constraint_name, ordinal_position"
            ),
            params(),
        )
        .await?;
    used_by.extend(keys(&incoming, DependencyKind::ForeignKey, false));

    // What a view reads, and the views that read this relation.
    let view_tables = if is_view {
        "select table_schema, table_name, null from information_schema.view_table_usage
         where view_schema = ? and view_name = ?"
    } else {
        "select view_schema, view_name, null from information_schema.view_table_usage
         where table_schema = ? and table_name = ?"
    };
    match conn.rows(view_tables, params()).await {
        Ok(rows) => {
            for row in rows {
                let info = DependencyInfo {
                    schema: text(&row, 0),
                    name: text(&row, 1),
                    kind: if is_view {
                        DependencyKind::Table
                    } else {
                        DependencyKind::View
                    },
                    detail: None,
                };
                if is_view {
                    uses.push(info);
                } else {
                    used_by.push(info);
                }
            }
            if is_view {
                let routines = conn
                    .rows(
                        "select specific_schema, specific_name from information_schema.view_routine_usage
                         where table_schema = ? and table_name = ?",
                        params(),
                    )
                    .await;
                match routines {
                    Ok(rows) => uses.extend(rows.iter().map(|row| DependencyInfo {
                        schema: text(row, 0),
                        name: text(row, 1),
                        kind: DependencyKind::Function,
                        detail: None,
                    })),
                    Err(e) if missing_table(&e, "view_routine_usage") => {}
                    Err(e) => return Err(e),
                }
            }
        }
        Err(e) if missing_table(&e, "view_table_usage") => untracked.push(DependencyKind::View),
        Err(e) => return Err(e),
    }
    // A view whose tables are not recorded: which ones it reads is not known either.
    if is_view && untracked.contains(&DependencyKind::View) {
        untracked.push(DependencyKind::Table);
    }

    // The triggers on a table.
    let triggers = conn
        .rows(
            "select trigger_schema, trigger_name, action_timing, event_manipulation
             from information_schema.triggers
             where event_object_schema = ? and event_object_table = ?",
            params(),
        )
        .await?;
    used_by.extend(triggers.iter().map(|row| DependencyInfo {
        schema: text(row, 0),
        name: text(row, 1),
        kind: DependencyKind::Trigger,
        detail: Some(format!("{} {}", text(row, 2), text(row, 3)).to_ascii_lowercase()),
    }));
    Ok(Some(Dependencies {
        uses: sorted(uses),
        used_by: sorted(used_by),
        untracked,
    }))
}

#[cfg(test)]
mod tests {
    use super::bound;

    #[test]
    fn bounds_are_written_as_the_partition_clause() {
        assert_eq!(
            bound("RANGE", Some("2025".into())).as_deref(),
            Some("VALUES LESS THAN (2025)")
        );
        assert_eq!(
            bound("RANGE COLUMNS", Some("MAXVALUE".into())).as_deref(),
            Some("VALUES LESS THAN (MAXVALUE)")
        );
        assert_eq!(
            bound("LIST", Some("1,2".into())).as_deref(),
            Some("VALUES IN (1,2)")
        );
        assert_eq!(bound("HASH", None), None);
    }
}
