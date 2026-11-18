//! An accepted question keeps its expansion, declaration snapshot and clock.
//! Requests persist this recipe rather than cached or decrypted output values.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Question {
    pub text: String,
    template: sinex_model::query_definition::Template,
    now: String,
    zone: String,
    day_start: String,
    declarations: Vec<(sinex_model::KindRef, String)>,
    pub position: i64,
    #[serde(default)]
    population: Option<(sinex_model::OperationId, i64)>,
}

impl Question {
    /// Resolve lifecycle subjects through the compiler's hidden identities.
    /// Projection names and the display page cannot change the write scope.
    /// The original question stays private; public scope names its subjects.
    pub async fn lifecycle_scope(
        &self,
        conn: &mut PgConnection,
    ) -> Result<(sinex_model::lifecycle::Scope, u64)> {
        use sinex_model::{EventId, MaterialId, lifecycle::Scope};
        let mut context = self.context()?;
        let refs: Vec<_> = self
            .declarations
            .iter()
            .map(|(kind, _)| kind.clone())
            .collect();
        let declarations = sinex_schema::kinds::load(conn, &refs).await?;
        if self.declarations.iter().any(|(kind, hash)| {
            declarations
                .get(kind)
                .is_none_or(|declaration| declaration.definition_hash() != *hash)
        }) {
            return Err(Error::Invalid(
                "accepted query declaration snapshot is unavailable".into(),
            ));
        }
        context.catalog = Some(sinex_query::QueryCatalog::new(declarations.into_values()));
        let mut query = sinex_query::template::bind(&self.template, &context)?;
        context
            .catalog
            .as_ref()
            .expect("declaration snapshot")
            .bind(&mut query, &context)?;
        let Source::Table { table, .. } = query.source else {
            return Err(Error::Invalid(
                "archive and restore need event, material or run rows".into(),
            ));
        };
        if !matches!(
            table,
            Table::Events | Table::Archive | Table::Materials | Table::Runs | Table::Sources
        ) || query
            .stages
            .iter()
            .any(|stage| matches!(stage, Stage::Group(_)))
        {
            return Err(Error::Invalid(
                "archive and restore need individual event, material or run rows".into(),
            ));
        }
        initialize_calendar(conn).await?;
        let mut tx = conn.begin().await?;
        read_one_snapshot(&mut tx).await?;
        sqlx::query_scalar!("SELECT set_config('TimeZone',$1,true)", self.zone)
            .fetch_one(&mut *tx)
            .await?;
        if self.population.is_some() {
            query = accepted_subjects(&query);
        }
        let mut compiled = compile_query(&mut tx, &context, self.position, &query).await?;
        if let Some((operation, _)) = self.population {
            compiled = compiled.accepted_population(operation);
        }
        let field = if table.is_events() {
            "_event_id"
        } else {
            "_row_identity"
        };
        let mut selection = SqlFragment::text(format!("SELECT q.\"{field}\"::text FROM ("));
        selection.append(compiled.answer).push(") q");
        let (sql, values) = selection.render();
        let ids: Vec<String> = sql::bind_scalar(sqlx::query_scalar(&sql), values)
            .fetch_all(&mut *tx)
            .await?;
        let count = ids.len() as u64;
        if self
            .population
            .is_some_and(|(_, expected)| expected as u64 != count)
        {
            return Err(Error::NotFound(
                "an accepted subject is no longer available".into(),
            ));
        }
        if table == Table::Sources {
            let scope = if let [source] = ids.as_slice() {
                Scope::Source {
                    source: source.clone(),
                    after: None,
                    before: None,
                }
            } else {
                let events = sqlx::query_scalar!(
                    "SELECT id FROM core.events WHERE source=ANY($1) AND commit_seq<=$2 ORDER BY id",
                    &ids,self.position
                ).fetch_all(&mut *tx).await?;
                Scope::Events {
                    events: events.into_iter().map(EventId).collect(),
                    query: None,
                }
            };
            tx.commit().await?;
            return Ok((scope, count));
        }
        let ids = ids
            .into_iter()
            .map(|id| {
                id.parse::<uuid::Uuid>().map_err(|_| {
                    Error::Invalid(
                        "lifecycle selection contains a row without a subject identity".into(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let scope =
            if table == Table::Materials {
                Scope::Materials {
                    materials: ids.into_iter().map(MaterialId).collect(),
                }
            } else if table == Table::Runs {
                if let [run] = ids.as_slice() {
                    let scope = Scope::Run {
                        run: sinex_model::RunId(*run),
                    };
                    tx.commit().await?;
                    return Ok((scope, count));
                }
                let events = sqlx::query_scalar!(
                "SELECT id FROM core.events WHERE run_id=ANY($1) AND commit_seq<=$2 ORDER BY id",
                &ids,self.position
            ).fetch_all(&mut *tx).await?;
                Scope::Events {
                    events: events.into_iter().map(EventId).collect(),
                    query: None,
                }
            } else {
                Scope::Events {
                    events: ids.into_iter().map(EventId).collect(),
                    query: None,
                }
            };
        tx.commit().await?;
        Ok((scope, count))
    }

    pub async fn accept(conn: &mut PgConnection, text: &str, context: &Context) -> Result<Self> {
        let mut tx = conn.begin().await?;
        read_one_snapshot(&mut tx).await?;
        let mut context = context.clone();
        let catalog = match context.catalog.take() {
            Some(catalog) => catalog,
            None => sinex_query::QueryCatalog::new(
                sinex_schema::kinds::all(&mut tx).await?.into_values(),
            ),
        };
        context.catalog = Some(catalog.clone());
        let template = if let Some(name) = sinex_query::saved::reference_name(text) {
            let definitions = saved_definitions(&mut tx, &name).await?;
            sinex_query::saved::template_saved(text, &context, &definitions)?
        } else {
            sinex_query::template::parse(text, &context)?
        };
        let mut position = current_seq(&mut tx).await?;
        let query = sinex_query::template::bind(&template, &context)?;
        for stage in &query.stages {
            match stage {
                Stage::AsOf(AsOf::Seq(seq)) => position = *seq,
                Stage::AsOf(AsOf::Time(time)) => position = seq_at(&mut tx, *time).await?,
                _ => {}
            }
        }
        tx.commit().await?;
        Ok(Self {
            text: text.into(),
            template,
            now: context.now.to_string(),
            zone: context.zone_name(),
            day_start: context.day_start.to_string(),
            declarations: catalog.declarations,
            position,
            population: None,
        })
    }

    /// Materialize only ordered subject identities for mutable units. This is
    /// one INSERT/SELECT inside the request snapshot transaction, with no client
    /// accumulation and no stored result text.
    pub(crate) async fn accept_population(
        &mut self,
        conn: &mut PgConnection,
        operation: sinex_model::OperationId,
    ) -> Result<()> {
        let mut context = self.context()?;
        let refs: Vec<_> = self
            .declarations
            .iter()
            .map(|(kind, _)| kind.clone())
            .collect();
        context.catalog = Some(sinex_query::QueryCatalog::new(
            sinex_schema::kinds::load(conn, &refs).await?.into_values(),
        ));
        let mut query = sinex_query::template::bind(&self.template, &context)?;
        context
            .catalog
            .as_ref()
            .expect("declaration snapshot")
            .bind(&mut query, &context)?;
        if !matches!(query.source, Source::Table { table,.. } if !table.is_events()) {
            return Ok(());
        }
        initialize_calendar(conn).await?;
        let compiled = compile_query(conn, &context, self.position, &query).await?;
        let order = compiled.order_by();
        let mut insert = SqlFragment::text(
            "WITH accepted AS (INSERT INTO core.operation_rows (operation_id,ordinal,identity) SELECT ",
        );
        insert
            .param(operation.0)
            .push(format!(
                ", row_number() OVER (ORDER BY {order}), q.\"_row_identity\" FROM ("
            ))
            .append(compiled.answer)
            .push(") q RETURNING ordinal) SELECT count(*) FROM accepted");
        let (sql, values) = insert.render();
        let count = sql::bind_scalar(sqlx::query_scalar::<_, i64>(&sql), values)
            .fetch_one(conn)
            .await?;
        self.population = Some((operation, count));
        Ok(())
    }

    pub fn reads_current_metadata(&self) -> bool {
        self.population.is_some()
    }

    pub fn context(&self) -> Result<Context> {
        let invalid = |error: &dyn std::fmt::Display| {
            Error::Invalid(format!("accepted query context: {error}"))
        };
        Ok(Context {
            now: self
                .now
                .parse()
                .map_err(|error: jiff::Error| invalid(&error))?,
            zone: jiff::tz::TimeZone::get(&self.zone).map_err(|error| invalid(&error))?,
            day_start: self
                .day_start
                .parse()
                .map_err(|error: jiff::Error| invalid(&error))?,
            catalog: None,
        })
    }

    pub async fn read(&self, conn: &mut PgConnection, page: &Page) -> Result<QueryRead> {
        let refs: Vec<_> = self
            .declarations
            .iter()
            .map(|(kind, _)| kind.clone())
            .collect();
        let declarations = sinex_schema::kinds::load(conn, &refs).await?;
        if self.declarations.iter().any(|(kind, hash)| {
            declarations
                .get(kind)
                .is_none_or(|declaration| declaration.definition_hash() != *hash)
        }) {
            return Err(Error::Invalid(
                "accepted query declaration snapshot is unavailable".into(),
            ));
        }
        let mut context = self.context()?;
        context.catalog = Some(sinex_query::QueryCatalog::new(declarations.into_values()));
        let selected = run_inner(
            conn,
            &self.text,
            None,
            &context,
            page,
            false,
            Some(self.position),
            Some(&self.template),
            self.population,
        )
        .await?;
        Ok(QueryRead {
            answer: selected.answer,
            event_ids: selected.event_ids,
        })
    }
}
