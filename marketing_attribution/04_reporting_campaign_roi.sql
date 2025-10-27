-- Reporting Layer: Campaign performance and ROI metrics
CREATE TABLE IF NOT EXISTS reporting.campaign_roi AS
WITH campaign_touchpoints AS (
    SELECT 
        touchpoint_campaign,
        touchpoint_source,
        touchpoint_medium,
        COUNT(DISTINCT conversion_id) AS total_conversions,
        COUNT(DISTINCT session_id) AS total_touchpoints,
        SUM(ad_cost_at_touchpoint) AS total_ad_spend,
        -- Revenue by attribution model
        SUM(first_touch_value) AS first_touch_revenue,
        SUM(last_touch_value) AS last_touch_revenue,
        SUM(linear_attribution_value) AS linear_attribution_revenue,
        SUM(time_decay_value) AS time_decay_revenue,
        SUM(position_based_value) AS position_based_revenue,
        -- Journey metrics
        AVG(total_touchpoints) AS avg_touchpoints_per_conversion,
        AVG(days_before_conversion) AS avg_days_to_conversion,
        COUNT(*) FILTER (WHERE touchpoint_role = 'acquisition') AS acquisition_touchpoints,
        COUNT(*) FILTER (WHERE touchpoint_role = 'nurture') AS nurture_touchpoints,
        COUNT(*) FILTER (WHERE touchpoint_role = 'conversion') AS conversion_touchpoints
    FROM attribution.customer_journeys
    WHERE 
        touchpoint_campaign IS NOT NULL
        AND touchpoint_campaign != ''
    GROUP BY 
        touchpoint_campaign,
        touchpoint_source,
        touchpoint_medium
),
campaign_sessions AS (
    SELECT 
        first_touch_campaign AS campaign,
        COUNT(*) AS total_sessions,
        COUNT(DISTINCT visitor_id) AS unique_visitors,
        AVG(session_duration_seconds) AS avg_session_duration,
        COUNT(*) FILTER (WHERE engagement_level = 'high') AS high_engagement_sessions
    FROM processed.sessions
    WHERE first_touch_campaign IS NOT NULL
    GROUP BY first_touch_campaign
)
SELECT 
    ct.touchpoint_campaign AS campaign_name,
    ct.touchpoint_source AS source,
    ct.touchpoint_medium AS medium,
    -- Traffic metrics
    cs.total_sessions,
    cs.unique_visitors,
    cs.avg_session_duration,
    cs.high_engagement_sessions,
    -- Conversion metrics
    ct.total_conversions,
    ct.total_touchpoints,
    -- Conversion rates
    CASE 
        WHEN cs.total_sessions > 0 
        THEN (ct.total_conversions::DECIMAL / cs.total_sessions * 100)
        ELSE 0
    END AS conversion_rate_pct,
    -- Cost metrics
    ct.total_ad_spend,
    CASE 
        WHEN ct.total_conversions > 0 
        THEN ct.total_ad_spend / ct.total_conversions
        ELSE NULL
    END AS cost_per_conversion,
    -- Revenue by attribution model
    ct.first_touch_revenue,
    ct.last_touch_revenue,
    ct.linear_attribution_revenue,
    ct.time_decay_revenue,
    ct.position_based_revenue,
    -- ROI calculations (using linear attribution as default)
    CASE 
        WHEN ct.total_ad_spend > 0 
        THEN ((ct.linear_attribution_revenue - ct.total_ad_spend) / ct.total_ad_spend * 100)
        ELSE NULL
    END AS roi_pct_linear,
    CASE 
        WHEN ct.total_ad_spend > 0 
        THEN ((ct.time_decay_revenue - ct.total_ad_spend) / ct.total_ad_spend * 100)
        ELSE NULL
    END AS roi_pct_time_decay,
    -- ROAS (Return on Ad Spend)
    CASE 
        WHEN ct.total_ad_spend > 0 
        THEN ct.linear_attribution_revenue / ct.total_ad_spend
        ELSE NULL
    END AS roas_linear,
    -- Journey insights
    ct.avg_touchpoints_per_conversion,
    ct.avg_days_to_conversion,
    ct.acquisition_touchpoints,
    ct.nurture_touchpoints,
    ct.conversion_touchpoints,
    -- Performance tier
    CASE 
        WHEN ct.total_ad_spend > 0 AND 
             ((ct.linear_attribution_revenue - ct.total_ad_spend) / ct.total_ad_spend * 100) > 200 
            THEN 'excellent'
        WHEN ct.total_ad_spend > 0 AND 
             ((ct.linear_attribution_revenue - ct.total_ad_spend) / ct.total_ad_spend * 100) > 100 
            THEN 'good'
        WHEN ct.total_ad_spend > 0 AND 
             ((ct.linear_attribution_revenue - ct.total_ad_spend) / ct.total_ad_spend * 100) > 0 
            THEN 'profitable'
        ELSE 'needs_optimization'
    END AS performance_tier,
    CURRENT_TIMESTAMP AS report_generated_at
FROM campaign_touchpoints ct
LEFT JOIN campaign_sessions cs ON ct.touchpoint_campaign = cs.campaign
WHERE ct.total_ad_spend > 0  -- Only show campaigns with spend
ORDER BY ct.linear_attribution_revenue DESC;

-- Create indexes
CREATE INDEX idx_reporting_campaign_roi_campaign ON reporting.campaign_roi(campaign_name);
CREATE INDEX idx_reporting_campaign_roi_performance ON reporting.campaign_roi(performance_tier);
CREATE INDEX idx_reporting_campaign_roi_source ON reporting.campaign_roi(source);

