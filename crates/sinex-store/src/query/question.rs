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
        })
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
        )
        .await?;
        Ok(QueryRead {
            answer: selected.answer,
            event_ids: selected.event_ids,
        })
    }
}
