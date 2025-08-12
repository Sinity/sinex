//! Comparison of Event ID patterns in practice

// ============================================================================
// CURRENT: Option<Id> Pattern
// ============================================================================

mod option_pattern {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Event {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub id: Option<String>,
        pub data: String,
    }

    impl Event {
        pub fn new(data: String) -> Self {
            Self { id: None, data }
        }

        pub fn is_persisted(&self) -> bool {
            self.id.is_some()
        }
    }

    pub async fn insert(mut event: Event) -> Event {
        event.id = Some("generated_id".to_string());
        event
    }

    pub async fn usage() {
        let event = Event::new("data".to_string());
        assert!(!event.is_persisted());

        let persisted = insert(event).await;
        assert!(persisted.is_persisted());

        // Same type throughout
        let _events: Vec<Event> = vec![Event::new("new".to_string()), persisted];
    }
}

// ============================================================================
// ALTERNATIVE: Separate Types Pattern
// ============================================================================

mod separate_types {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct NewEvent {
        pub data: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Event {
        pub id: String,
        pub data: String,
    }

    impl NewEvent {
        pub fn new(data: String) -> Self {
            Self { data }
        }
    }

    impl Event {
        pub fn id(&self) -> &str {
            &self.id
        }
    }

    pub async fn insert(new: NewEvent) -> Event {
        Event {
            id: "generated_id".to_string(),
            data: new.data,
        }
    }

    pub async fn usage() {
        let new_event = NewEvent::new("data".to_string());
        // Compile error: new_event.id doesn't exist ✓

        let persisted = insert(new_event).await;
        let _id = persisted.id(); // Always has ID ✓

        // Different types - need enum or trait object
        enum AnyEvent {
            New(NewEvent),
            Persisted(Event),
        }

        let _events = vec![
            AnyEvent::New(NewEvent::new("new".to_string())),
            AnyEvent::Persisted(persisted),
        ];
    }
}

// ============================================================================
// REAL WORLD EXAMPLES
// ============================================================================

// Diesel's approach (simplified)
mod diesel_style {
    #[derive(Insertable)]
    #[table_name = "events"]
    struct NewEvent<'a> {
        data: &'a str,
    }

    #[derive(Queryable)]
    struct Event {
        id: i32,
        data: String,
    }
}

// SeaORM's approach (simplified)
mod sea_orm_style {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "events")]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub id: Option<i32>, // Option for new entities
        pub data: String,
    }
}

// Our current Sinex approach
mod sinex_style {
    use std::marker::PhantomData;

    pub struct Id<T> {
        ulid: String,
        _phantom: PhantomData<T>,
    }

    pub struct Event<T> {
        pub id: Option<Id<Event<T>>>,
        pub payload: T,
    }

    impl<T> Event<T> {
        pub fn new(payload: T) -> Self {
            Self { id: None, payload }
        }

        pub fn with_id(mut self, id: Id<Event<T>>) -> Self {
            self.id = Some(id);
            self
        }
    }
}
