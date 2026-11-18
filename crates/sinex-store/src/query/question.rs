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
