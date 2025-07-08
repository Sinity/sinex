-- Fix work queue notification trigger to handle missing priority field
-- The work_queue table does not have a priority column, but the trigger was trying to access it

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
        'priority', NULL  -- work_queue table does not have priority column
    );
    
    PERFORM pg_notify('work_queue_updated', notification_payload::TEXT);
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION notify_work_queue_updated() IS 'Sends PostgreSQL notifications when work queue items are added or updated (fixed priority handling)';
