-- Rename checksum_md5 to checksum_blake3 to accurately reflect the hash algorithm used
ALTER TABLE core.blobs 
RENAME COLUMN checksum_md5 TO checksum_blake3;

-- Add comment to clarify the column contains BLAKE3 hashes
COMMENT ON COLUMN core.blobs.checksum_blake3 IS 'BLAKE3 hash of the blob content';