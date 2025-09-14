**Project Description**

This project leverages Google BigQuery AI and the Google Ads Transparency Center dataset to study online advertising integrity. It is divided into two complementary parts:

**Ads Removal Forecast**

1. We analyze ad lifespans to detect “quick removals” — ads that disappear within 7–30 days of launch, often due to policy violations.

2. Using BigQuery SQL and BigQuery ML (ARIMA), we compute removal rates by topic, ad format, region, and advertiser verification status.

3. We then forecast removal rates 30 days ahead, giving platforms and regulators early warnings about risky campaigns.

**Patterns Across Regions**

1. We focus on political and government-related ads to identify cross-region coordination patterns.

2. Ads are grouped by creative, and we measure how many regions they launched in, whether they launched simultaneously (≤7 days apart), and the advertiser’s verification status.

3. Campaigns with ≥5 regions, simultaneous launches, and unverifiable advertisers are flagged as suspicious.

4. Results are written back into BigQuery for further transparency analysis.

**Key Impact**

1. Provides regulators and platforms with tools to predict harmful ads before they spread widely.

2. Surfaces suspicious multi-region campaigns, improving transparency in political advertising.

3. Demonstrates how BigQuery AI can be used not just for analytics, but for real-world trust and safety applications.
