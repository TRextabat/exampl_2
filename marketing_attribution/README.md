# Marketing Attribution Data Warehouse

Complex marketing analytics with multi-touch attribution, campaign performance, and conversion funnel analysis.

## Schema Layers

1. **Landing** (`landing.*`) - Raw event streams from tracking pixels
2. **Processed** (`processed.*`) - Sessionized and enriched events
3. **Attribution** (`attribution.*`) - Attribution models and touchpoint analysis
4. **Reporting** (`reporting.*`) - Campaign performance and ROI metrics

## Tables: 14 tables
## Dependencies: 20+ lineage edges

## Expected Lineage Flow

```
landing.web_events → processed.sessions → attribution.customer_journeys → reporting.campaign_roi
landing.ad_impressions → processed.ad_interactions → attribution.touchpoint_attribution → reporting.channel_performance
landing.conversions → processed.conversion_events → attribution.multi_touch_model → reporting.attribution_report
landing.email_events → processed.email_engagement → attribution.email_attribution → reporting.email_performance
```

## Business Logic

- Multi-touch attribution (first-touch, last-touch, linear, time-decay, position-based)
- Customer journey mapping across channels
- Conversion funnel analysis with drop-off rates
- Marketing ROI calculations by channel
- Cohort analysis by acquisition source
- A/B test performance tracking
- Email engagement scoring
- Retargeting effectiveness measurement

