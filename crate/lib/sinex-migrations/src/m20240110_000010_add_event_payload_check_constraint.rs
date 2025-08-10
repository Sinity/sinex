use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // This functionality was already implemented in m20240102_000002_add_validation_functions
        // with constraint name 'payload_must_be_valid' instead of 'check_payload_valid'
        // Keeping this migration as a no-op for compatibility
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // No-op - constraint was created in m20240102_000002_add_validation_functions
        Ok(())
    }
}
