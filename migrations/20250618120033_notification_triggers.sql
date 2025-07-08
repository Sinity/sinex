-- Enable PostgreSQL LISTEN/NOTIFY system for real-time event processing
-- This migration adds trigger functions and triggers for automatic notifications

-- Function for notifying when events are inserted
CREATE OR REPLACE FUNCTION notify_event_inserted()
RETURNS TRIGGER AS $$
DECLARE
    notification_payload JSON;
    is_chunked BOOLEAN;
    chunk_count INTEGER;
BEGIN
    -- Check if this event is part of a chunked payload
    is_chunked := NEW.payload ? 'chunk_info';
    chunk_count := CASE 
        WHEN is_chunked THEN (NEW.payload->'chunk_info'->>'total_chunks')::INTEGER
        ELSE NULL 
    END;

    notification_payload := json_build_object(
        'event_id', NEW.id::TEXT,
        'source', NEW.source,
        'event_type', NEW.event_type,
        'host', NEW.host,
        'chunked', is_chunked,
        'chunk_count', chunk_count
    );
    
    PERFORM pg_notify('event_inserted', notification_payload::TEXT);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function for notifying when work queue is updated
CREATE OR REPLACE FUNCTION notify_work_queue_updated()
RETURNS TRIGGER AS $$
DECLARE
    notification_payload JSON;
    queue_action TEXT;
BEGIN
    -- Determine the action based on the change
    IF TG_OP = 'INSERT' THEN
        queue_action := 'added';
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.status != NEW.status THEN
            CASE NEW.status
                WHEN 'processing' THEN queue_action := 'claimed';
                WHEN 'succeeded' THEN queue_action := 'completed';
                WHEN 'failed' THEN queue_action := 'failed';
                WHEN 'failed_retryable' THEN queue_action := 'retried';
                ELSE queue_action := 'updated';
            END CASE;
        ELSE
            queue_action := 'updated';
        END IF;
    ELSE
        RETURN NULL;
    END IF;

    notification_payload := json_build_object(
        'queue_id', COALESCE(NEW.queue_id, OLD.queue_id)::TEXT,
        'event_id', COALESCE(NEW.raw_event_id, OLD.raw_event_id)::TEXT,
        'agent_name', COALESCE(NEW.target_agent_name, OLD.target_agent_name),
        'action', queue_action,
        'priority', COALESCE(NEW.priority, OLD.priority)
    );
    
    PERFORM pg_notify('work_queue_updated', notification_payload::TEXT);
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Create triggers
DROP TRIGGER IF EXISTS trigger_notify_event_inserted ON raw.events;
CREATE TRIGGER trigger_notify_event_inserted
    AFTER INSERT ON raw.events
    FOR EACH ROW
    EXECUTE FUNCTION notify_event_inserted();

DROP TRIGGER IF EXISTS trigger_notify_work_queue_updated ON sinex_schemas.work_queue;
CREATE TRIGGER trigger_notify_work_queue_updated
    AFTER INSERT OR UPDATE ON sinex_schemas.work_queue
    FOR EACH ROW
    EXECUTE FUNCTION notify_work_queue_updated();

-- Add comments explaining the notification system
COMMENT ON FUNCTION notify_event_inserted() IS 'Sends PostgreSQL notifications when new events are inserted into raw.events table';
COMMENT ON FUNCTION notify_work_queue_updated() IS 'Sends PostgreSQL notifications when work queue items are added or updated';
COMMENT ON TRIGGER trigger_notify_event_inserted ON raw.events IS 'Triggers event_inserted notifications for real-time event processing';
COMMENT ON TRIGGER trigger_notify_work_queue_updated ON sinex_schemas.work_queue IS 'Triggers work_queue_updated notifications for real-time work management';