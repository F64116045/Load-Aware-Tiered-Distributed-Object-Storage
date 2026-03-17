ALTER TABLE object_versions
ADD COLUMN IF NOT EXISTS content_type TEXT;
