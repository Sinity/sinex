-- Revert the column rename
ALTER TABLE core.blobs 
RENAME COLUMN checksum_blake3 TO checksum_md5;

-- Remove the comment
COMMENT ON COLUMN core.blobs.checksum_md5 IS NULL;