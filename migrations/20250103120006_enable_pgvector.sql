-- Migration: Enable pgvector extension for vector similarity search
-- Up Migration

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

COMMENT ON EXTENSION vector IS 'pgvector: Open-source vector similarity search for Postgres';