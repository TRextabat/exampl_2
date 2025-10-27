-- Processed Layer: Sessionized web events
CREATE TABLE IF NOT EXISTS processed.sessions AS
WITH event_sequence AS (
    SELECT 
        event_id,
        COALESCE(user_id, anonymous_id) AS visitor_id,
        user_id,
        anonymous_id,
        event_timestamp,
        event_type,
        page_url,
        referrer_url,
        utm_source,
        utm_medium,
        utm_campaign,
        device_type,
        geo_country,
        -- Calculate time since previous event
        event_timestamp - LAG(event_timestamp) OVER (
            PARTITION BY COALESCE(user_id, anonymous_id)
            ORDER BY event_timestamp
        ) AS time_since_last_event,
        -- Assign session ID based on 30-minute timeout
        SUM(CASE 
            WHEN event_timestamp - LAG(event_timestamp) OVER (
                PARTITION BY COALESCE(user_id, anonymous_id)
                ORDER BY event_timestamp
            ) > INTERVAL '30 minutes' 
            OR LAG(event_timestamp) OVER (
                PARTITION BY COALESCE(user_id, anonymous_id)
                ORDER BY event_timestamp
            ) IS NULL
            THEN 1 
            ELSE 0 
        END) OVER (
            PARTITION BY COALESCE(user_id, anonymous_id)
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_number
    FROM landing.web_events
),
session_aggregates AS (
    SELECT 
        visitor_id,
        session_number,
        MIN(event_timestamp) AS session_start,
        MAX(event_timestamp) AS session_end,
        COUNT(*) AS event_count,
        COUNT(DISTINCT page_url) AS unique_pages_viewed,
        -- First touch attribution fields
        FIRST_VALUE(utm_source) OVER (
            PARTITION BY visitor_id, session_number 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_touch_source,
        FIRST_VALUE(utm_medium) OVER (
            PARTITION BY visitor_id, session_number 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_touch_medium,
        FIRST_VALUE(utm_campaign) OVER (
            PARTITION BY visitor_id, session_number 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_touch_campaign,
        FIRST_VALUE(referrer_url) OVER (
            PARTITION BY visitor_id, session_number 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS landing_page_referrer,
        -- Last touch attribution fields
        LAST_VALUE(utm_source) OVER (
            PARTITION BY visitor_id, session_number 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_touch_source,
        LAST_VALUE(utm_medium) OVER (
            PARTITION BY visitor_id, session_number 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_touch_medium,
        LAST_VALUE(utm_campaign) OVER (
            PARTITION BY visitor_id, session_number 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_touch_campaign,
        MAX(user_id) AS user_id, -- Get user_id if they logged in during session
        FIRST_VALUE(device_type) OVER (
            PARTITION BY visitor_id, session_number 
            ORDER BY event_timestamp
        ) AS device_type,
        FIRST_VALUE(geo_country) OVER (
            PARTITION BY visitor_id, session_number 
            ORDER BY event_timestamp
        ) AS geo_country
    FROM event_sequence
    GROUP BY 
        visitor_id, 
        session_number,
        utm_source,
        utm_medium,
        utm_campaign,
        referrer_url,
        user_id,
        device_type,
        geo_country,
        event_timestamp
)
SELECT DISTINCT
    MD5(visitor_id || '-' || session_number::TEXT) AS session_id,
    visitor_id,
    user_id,
    session_number,
    session_start,
    session_end,
    EXTRACT(EPOCH FROM (session_end - session_start)) AS session_duration_seconds,
    event_count,
    unique_pages_viewed,
    first_touch_source,
    first_touch_medium,
    first_touch_campaign,
    landing_page_referrer,
    last_touch_source,
    last_touch_medium,
    last_touch_campaign,
    device_type,
    geo_country,
    -- Engagement scoring
    CASE 
        WHEN event_count >= 10 AND session_duration_seconds > 300 THEN 'high'
        WHEN event_count >= 5 AND session_duration_seconds > 120 THEN 'medium'
        ELSE 'low'
    END AS engagement_level
FROM session_aggregates;

-- Create indexes
CREATE INDEX idx_processed_sessions_visitor ON processed.sessions(visitor_id);
CREATE INDEX idx_processed_sessions_user ON processed.sessions(user_id);
CREATE INDEX idx_processed_sessions_start ON processed.sessions(session_start);
CREATE INDEX idx_processed_sessions_campaign ON processed.sessions(first_touch_campaign);

