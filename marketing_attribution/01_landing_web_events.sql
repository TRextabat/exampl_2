-- Landing Layer: Raw web tracking events
CREATE TABLE IF NOT EXISTS landing.web_events (
    event_id VARCHAR(50) PRIMARY KEY,
    anonymous_id VARCHAR(50), -- Cookie/device ID
    user_id VARCHAR(50), -- Logged-in user (if available)
    session_id VARCHAR(50),
    event_timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50), -- 'page_view', 'click', 'form_submit', 'video_play', etc.
    page_url TEXT,
    page_title VARCHAR(200),
    referrer_url TEXT,
    utm_source VARCHAR(100),
    utm_medium VARCHAR(100),
    utm_campaign VARCHAR(100),
    utm_term VARCHAR(200),
    utm_content VARCHAR(200),
    device_type VARCHAR(20), -- 'desktop', 'mobile', 'tablet'
    browser VARCHAR(50),
    operating_system VARCHAR(50),
    geo_country VARCHAR(50),
    geo_city VARCHAR(100),
    ip_address VARCHAR(45),
    user_agent TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Landing: Ad impressions and clicks
CREATE TABLE IF NOT EXISTS landing.ad_impressions (
    impression_id VARCHAR(50) PRIMARY KEY,
    anonymous_id VARCHAR(50),
    campaign_id VARCHAR(50) NOT NULL,
    ad_group_id VARCHAR(50),
    ad_id VARCHAR(50),
    impression_timestamp TIMESTAMP NOT NULL,
    ad_platform VARCHAR(30), -- 'google_ads', 'facebook', 'linkedin', 'twitter'
    ad_format VARCHAR(30), -- 'search', 'display', 'video', 'social'
    placement VARCHAR(100),
    impression_cost DECIMAL(10, 4),
    was_clicked BOOLEAN DEFAULT FALSE,
    click_timestamp TIMESTAMP,
    device_type VARCHAR(20),
    geo_country VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Landing: Conversions (purchases, signups, leads)
CREATE TABLE IF NOT EXISTS landing.conversions (
    conversion_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    anonymous_id VARCHAR(50),
    conversion_timestamp TIMESTAMP NOT NULL,
    conversion_type VARCHAR(50), -- 'purchase', 'signup', 'lead', 'trial_start'
    conversion_value DECIMAL(10, 2),
    order_id VARCHAR(50),
    product_ids TEXT[],
    source_session_id VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Load from event streaming (Segment, RudderStack, etc.)
INSERT INTO landing.web_events
SELECT * FROM external_segment.events
WHERE event_timestamp >= CURRENT_DATE - INTERVAL '90 days';

INSERT INTO landing.ad_impressions
SELECT * FROM external_ad_platforms.impressions
WHERE impression_timestamp >= CURRENT_DATE - INTERVAL '90 days';

INSERT INTO landing.conversions
SELECT * FROM external_ecommerce.conversions
WHERE conversion_timestamp >= CURRENT_DATE - INTERVAL '90 days';

