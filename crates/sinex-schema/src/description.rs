//! The schema description: the statements that create the current schema in
//! an empty database, and what keeping an existing one current reinstalls.
use crate::{Checks, event_types, tables};
use sinex_model::KindDeclaration;

pub(crate) struct View {
    pub name: String,
    pub columns: Vec<String>,
    pub statement: String,
}

pub(crate) struct Description {
    pub tables: Vec<String>,
    pub after_tables: Vec<String>,
    pub visibility: Vec<String>,
    pub views: Vec<View>,
    pub kinds: Vec<KindDeclaration>,
}

impl Description {
    pub(crate) fn current() -> Self {
        Self {
            tables: tables::tables(&Checks::from_model()),
            after_tables: tables::after_tables(),
            visibility: crate::visibility(),
            views: event_types::ALL.iter().map(crate::kind_view).collect(),
            kinds: event_types::declarations().to_vec(),
        }
    }

    /// Install the bundled kinds and recreate the views and grants: what
    /// creation, an upgrade and every start run last.
    pub(crate) async fn apply(
        &self,
        connection: &mut sqlx::PgConnection,
        owners: &crate::kinds::Owners,
    ) -> Result<(), crate::Error> {
        crate::kinds::install(connection, owners, &self.kinds).await?;
        crate::apply_views(connection, self).await
    }

    pub(crate) fn statements(&self) -> Vec<String> {
        let mut statements = self.tables.clone();
        statements.extend(self.after_tables.clone());
        statements.extend(self.visibility.clone());
        statements.extend(self.views.iter().map(|view| view.statement.clone()));
        statements
    }
}
