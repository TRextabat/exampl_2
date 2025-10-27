-- Attribution Layer: Customer journey with all touchpoints before conversion
CREATE TABLE IF NOT EXISTS attribution.customer_journeys AS
WITH conversions_with_sessions AS (
    SELECT 
        c.conversion_id,
        c.user_id,
        c.anonymous_id,
        c.conversion_timestamp,
        c.conversion_type,
        c.conversion_value,
        c.order_id,
        s.session_id,
        s.session_start,
        s.first_touch_source,
        s.first_touch_medium,
        s.first_touch_campaign
    FROM landing.conversions c
    LEFT JOIN processed.sessions s 
        ON COALESCE(c.user_id, c.anonymous_id) = s.visitor_id
        AND c.conversion_timestamp BETWEEN s.session_start AND s.session_end + INTERVAL '5 minutes'
),
customer_touchpoints AS (
    SELECT 
        cwc.conversion_id,
        cwc.conversion_timestamp,
        cwc.conversion_value,
        s.session_id,
        s.session_start,
        s.first_touch_source AS touchpoint_source,
        s.first_touch_medium AS touchpoint_medium,
        s.first_touch_campaign AS touchpoint_campaign,
        s.device_type,
        s.engagement_level,
        -- Calculate touchpoint position in journey
        ROW_NUMBER() OVER (
            PARTITION BY cwc.conversion_id 
            ORDER BY s.session_start
        ) AS touchpoint_position,
        -- Total touchpoints in journey
        COUNT(*) OVER (
            PARTITION BY cwc.conversion_id
        ) AS total_touchpoints,
        -- Days before conversion
        EXTRACT(EPOCH FROM (cwc.conversion_timestamp - s.session_start)) / 86400 AS days_before_conversion,
        -- Ad interactions during this touchpoint
        COUNT(ai.impression_id) AS ad_impressions_count,
        SUM(ai.impression_cost) AS ad_cost_at_touchpoint
    FROM conversions_with_sessions cwc
    INNER JOIN processed.sessions s 
        ON cwc.user_id = s.user_id 
        OR cwc.anonymous_id = s.visitor_id
    LEFT JOIN landing.ad_impressions ai 
        ON s.visitor_id = ai.anonymous_id
        AND ai.impression_timestamp BETWEEN s.session_start AND s.session_end
    WHERE 
        s.session_start <= cwc.conversion_timestamp
        AND s.session_start >= cwc.conversion_timestamp - INTERVAL '90 days'
    GROUP BY 
        cwc.conversion_id,
        cwc.conversion_timestamp,
        cwc.conversion_value,
        s.session_id,
        s.session_start,
        s.first_touch_source,
        s.first_touch_medium,
        s.first_touch_campaign,
        s.device_type,
        s.engagement_level
)
SELECT 
    conversion_id,
    conversion_timestamp,
    conversion_value,
    session_id,
    session_start,
    touchpoint_source,
    touchpoint_medium,
    touchpoint_campaign,
    device_type,
    engagement_level,
    touchpoint_position,
    total_touchpoints,
    days_before_conversion,
    ad_impressions_count,
    ad_cost_at_touchpoint,
    -- Multi-touch attribution weights
    -- 1. First-touch: 100% to first touchpoint
    CASE WHEN touchpoint_position = 1 THEN conversion_value ELSE 0 END AS first_touch_value,
    -- 2. Last-touch: 100% to last touchpoint
    CASE WHEN touchpoint_position = total_touchpoints THEN conversion_value ELSE 0 END AS last_touch_value,
    -- 3. Linear: Equal credit to all touchpoints
    conversion_value / total_touchpoints AS linear_attribution_value,
    -- 4. Time-decay: More recent touchpoints get more credit
    conversion_value * EXP(-0.1 * days_before_conversion) / 
        SUM(EXP(-0.1 * days_before_conversion)) OVER (PARTITION BY conversion_id) AS time_decay_value,
    -- 5. Position-based (U-shaped): 40% first, 40% last, 20% middle
    CASE 
        WHEN total_touchpoints = 1 THEN conversion_value
        WHEN touchpoint_position = 1 THEN conversion_value * 0.4
        WHEN touchpoint_position = total_touchpoints THEN conversion_value * 0.4
        ELSE conversion_value * 0.2 / (total_touchpoints - 2)
    END AS position_based_value,
    -- Journey metrics
    CASE 
        WHEN touchpoint_position = 1 THEN 'acquisition'
        WHEN touchpoint_position = total_touchpoints THEN 'conversion'
        ELSE 'nurture'
    END AS touchpoint_role
FROM customer_touchpoints;

-- Create indexes
CREATE INDEX idx_attribution_journeys_conversion ON attribution.customer_journeys(conversion_id);
CREATE INDEX idx_attribution_journeys_campaign ON attribution.customer_journeys(touchpoint_campaign);
CREATE INDEX idx_attribution_journeys_source ON attribution.customer_journeys(touchpoint_source);
CREATE INDEX idx_attribution_journeys_position ON attribution.customer_journeys(touchpoint_position);

