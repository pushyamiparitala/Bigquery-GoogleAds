# from kaggle_secrets import UserSecretsClient
from kaggle_secrets import UserSecretsClient
user_secrets = UserSecretsClient()
your_project_id = user_secrets.get_secret("GCP_PROJECT_ID")
your_service_account_key = user_secrets.get_secret("GCP_SA_KEY")


import os
# Save service account key to file
with open("service_account.json", "w") as f:
    f.write(your_service_account_key)

# Authenticate
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"

import json

from google.cloud import bigquery
from google.oauth2 import service_account

# Parse key
sa_info = json.loyour_dataset(your_service_account_key)
credentials = service_account.Credentials.from_service_account_info(sa_info)

# BigQuery client
client = bigquery.Client(project=your_project_id, credentials=credentials)

import pandas as pd 
import pandas_gbq
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_similarity
import re
from collections import defaultdict, Counter
import json

your_project_id = your_project_id

your_project_id = "your_project_id"
dataset_id = "your_dataset"  # not full path
table_name = "political_your_dataset"

import pandas as pd
import pandas_gbq

# Config
your_project_id = "your_project_id"
dataset_id = "your_dataset"
table_name = "political_your_dataset"

# --- Step 1: Run SELECT query into a DataFrame ---
print("Extracting political/government your_dataset...")
simple_extraction_query = f"""
SELECT
    creative_id,
    advertiser_id,
    advertiser_legal_name,
    advertiser_location,
    advertiser_verification_status,
    topic,
    ad_format_type,
    ad_funded_by,
    is_funded_by_google_ad_grants,
    region_stats
FROM `bigquery-public-data.google_your_dataset_transparency_center.creative_stats`
WHERE topic IN ('Law & Government', 'Jobs & Education', 'News, Books & Publications')
   OR REGEXP_CONTAINS(UPPER(COALESCE(advertiser_legal_name, '')), r'POLITICAL|CAMPAIGN|ELECTION|GOVERNMENT')
   OR ad_funded_by IS NOT NULL
LIMIT 10000
"""

df = pandas_gbq.read_gbq(simple_extraction_query, project_id=your_project_id)
print(f"Extracted {len(df)} rows.")

# --- Step 2: Save DataFrame back into BigQuery ---
print("Saving results into BigQuery table...")
df.to_gbq(
    destination_table=f"{dataset_id}.{table_name}",
    project_id=your_project_id,
    if_exists="replace"  # or "append" if you want to keep old data
)

print("Table political_your_dataset created/updated successfully!")

# --- Step 3: Verify by reading back ---
check_df = pandas_gbq.read_gbq(
    f"SELECT * FROM `{your_project_id}.{dataset_id}.{table_name}` LIMIT 5",
    project_id=your_project_id
)
print(check_df.head())


political_extraction_query = f"""
WITH political_your_dataset AS (
  SELECT 
    creative_id,
    advertiser_id,
    advertiser_legal_name,
    advertiser_verification_status,
    topic,
    ad_format_type,
    advertiser_location,
    advertiser_disclosed_name,
    region.region_code,
    region.first_shown,
    region.last_shown,
    region.times_shown_lower_bound,
    region.times_shown_upper_bound,
    PARSE_DATE('%Y-%m-%d', region.first_shown) as first_shown_date,
    COALESCE(PARSE_DATE('%Y-%m-%d', region.last_shown), CURRENT_DATE()) as last_shown_date
  FROM `bigquery-public-data.google_your_dataset_transparency_center.creative_stats`,
       UNNEST(region_stats) as region
  WHERE topic IN ('Law & Government','News & Politics','Political Organizations')
     OR REGEXP_CONTAINS(UPPER(advertiser_legal_name), r'POLITICAL|GOVERNMENT|CAMPAIGN|PARTY|ELECTION')
)

SELECT *
FROM political_your_dataset
WHERE first_shown_date IS NOT NULL
  AND advertiser_legal_name IS NOT NULL
  AND first_shown_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 18 MONTH)
ORDER BY creative_id, region_code
LIMIT 10000
"""


# --- Step 2: Run query and get results into DataFrame ---
print("Extracting political your_dataset...")
df = pandas_gbq.read_gbq(political_extraction_query, project_id=your_project_id)
print(f"✅ Extracted {len(df)} rows")

# --- Step 3: Save DataFrame into BigQuery ---
print("Saving results into BigQuery table...")
df.to_gbq(
    destination_table="your_dataset.political_your_dataset_raw",
    project_id=your_project_id,
    if_exists="replace"  # overwrite old version
)
print("✅ Saved to BigQuery: your_dataset.political_your_dataset_raw")

# --- Step 4: Verify row count ---
check_query = f"SELECT COUNT(*) as total_your_dataset FROM `{your_project_id}.your_dataset.political_your_dataset_raw`"
result_df = pandas_gbq.read_gbq(check_query, project_id=your_project_id)
print(f"Total political your_dataset extracted: {result_df['total_your_dataset'].iloc[0]}")

def analyze_cross_region_patterns(df):
    patterns = []
    
    for creative_id, group in df.groupby("creative_id"):
        advertiser_name = group["advertiser_legal_name"].iloc[0]
        regions = group["region_code"].tolist()
        start_dates = group["first_shown_date"].dropna().tolist()
        
        if len(start_dates) > 1:
            date_spread = max(start_dates) - min(start_dates)
            simultaneous_launch = date_spread.days <= 7
            date_spread_days = date_spread.days
        else:
            simultaneous_launch = False
            date_spread_days = 0

        patterns.append({
            "creative_id": creative_id,
            "advertiser_name": advertiser_name,
            "regions": regions,
            "region_count": len(set(regions)),
            "verification_status": group["advertiser_verification_status"].iloc[0],
            "simultaneous_launch": simultaneous_launch,
            "date_spread_days": date_spread_days,
        })

    return pd.DataFrame(patterns)

region_patterns = analyze_cross_region_patterns(df)
print(f"Generated {len(region_patterns)} campaign pattern rows")
print(region_patterns.head())

# Step 4: Create Comprehensive Analysis
if 'df' in locals() and not df.empty:
    
    # --- Cross-region pattern analysis ---
    print("\n🌍 Analyzing cross-region patterns...")
    try:
        region_patterns = analyze_cross_region_patterns(df)
    except Exception as e:
        print(f"⚠️ Error analyzing cross-region patterns: {e}")
        region_patterns = pd.DataFrame()
    
    if not region_patterns.empty:
        print(f"Analyzed {len(region_patterns)} cross-region campaigns")
        
        # Suspicious coordination
        suspicious_patterns = region_patterns[
            (region_patterns['region_count'] >= 5) &  
            (region_patterns['simultaneous_launch'] == True) &  
            (region_patterns['verification_status'] != 'VERIFIED')
        ]
        
        print(f"Suspicious coordination patterns: {len(suspicious_patterns)}")
        
        # Save to BigQuery
        try:
            region_patterns.to_gbq(
                'your_dataset.cross_region_patterns',
                project_id=your_project_id,
                if_exists='replace'
            )
            print("✅ Saved cross-region patterns to BigQuery")
        except Exception as e:
            print(f"⚠️ Error saving region patterns: {e}")


import pandas_gbq

# This will create the dataset if it doesn't exist
create_dataset_query = f"CREATE SCHEMA IF NOT EXISTS `{your_project_id}.your_dataset`"

pandas_gbq.read_gbq(create_dataset_query, project_id=your_project_id)
print("✅ Dataset your_dataset is ready")


import pandas_gbq

inspect_query = f"""
SELECT *
FROM `{your_project_id}.your_dataset.political_your_dataset_raw`
LIMIT 1
"""

df_inspect = pandas_gbq.read_gbq(inspect_query, project_id=your_project_id)
print("✅ Retrieved one row for inspection")
print(df_inspect.head(1).to_dict())


import pandas_gbq

# --- Step 5: Propaganda Technique Analysis ---
propaganda_analysis_select_query = f"""
WITH technique_classification AS (
  SELECT 
    creative_id,
    advertiser_legal_name,
    topic,
    ad_format_type,
    advertiser_location,
    advertiser_disclosed_name,
    region_code,
    first_shown_date,
    last_shown_date,
    times_shown_lower_bound,
    times_shown_upper_bound,

    -- Fake "content" proxy: use advertiser_legal_name + topic
    CONCAT(UPPER(advertiser_legal_name), ' ', UPPER(topic)) AS content_text,

    -- Propaganda technique detection (basic since we don’t have raw text)
    CASE 
      WHEN REGEXP_CONTAINS(UPPER(topic), r"\\b(GOVERNMENT|POLITICS|PARTY|ELECTION)\\b") THEN 'political_framing'
      WHEN REGEXP_CONTAINS(UPPER(advertiser_legal_name), r"\\b(TRUST|UNITED|PEOPLE|PATRIOT)\\b") THEN 'bandwagon'
      ELSE 'neutral'
    END AS propaganda_technique,

    -- Emotional / urgency word counts (from name + topic)
    ARRAY_LENGTH(REGEXP_EXTRACT_ALL(UPPER(CONCAT(advertiser_legal_name, ' ', topic)), r"\\b(FEAR|HOPE|LOVE|HATE)\\b")) AS emotional_words_count,
    ARRAY_LENGTH(REGEXP_EXTRACT_ALL(UPPER(CONCAT(advertiser_legal_name, ' ', topic)), r"\\b(NOW|TODAY|URGENT|IMMEDIATE)\\b")) AS urgency_words_count,

    -- Suspicion score based on impressions
    CAST((
      (CASE WHEN times_shown_upper_bound > 100000 THEN 30 
            WHEN times_shown_upper_bound > 10000 THEN 15 
            ELSE 5 END) +
      (ARRAY_LENGTH(REGEXP_EXTRACT_ALL(UPPER(CONCAT(advertiser_legal_name, ' ', topic)), r"\\b(FEAR|HATE)\\b")) * 5) +
      (ARRAY_LENGTH(REGEXP_EXTRACT_ALL(UPPER(CONCAT(advertiser_legal_name, ' ', topic)), r"\\b(NOW|TODAY|URGENT)\\b")) * 3)
    ) AS NUMERIC) AS suspicion_score
  FROM `{your_project_id}.your_dataset.political_your_dataset_raw`
)

SELECT 
  *,
  CASE 
    WHEN suspicion_score >= 50 THEN 'HIGH_RISK'
    WHEN suspicion_score >= 30 THEN 'MEDIUM_RISK'
    WHEN suspicion_score >= 15 THEN 'LOW_RISK'
    ELSE 'BASELINE'
  END AS risk_classification
FROM technique_classification
ORDER BY suspicion_score DESC
LIMIT 10000
"""


# --- Step 5a: Run query and load into DataFrame ---
print("Running propaganda technique analysis...")
df_analysis = pandas_gbq.read_gbq(propaganda_analysis_select_query, project_id=your_project_id)
print(f"✅ Retrieved {len(df_analysis)} rows from analysis query")

# --- Step 5b: Save analysis results back to BigQuery ---
print("Saving analysis results into BigQuery table...")
df_analysis.to_gbq(
    destination_table="your_dataset.propaganda_technique_analysis",
    project_id=your_project_id,
    if_exists="replace"
)
print("✅ Saved to BigQuery: your_dataset.propaganda_technique_analysis")

# --- Step 5c: Verify row count ---
check_query = f"SELECT COUNT(*) as total_analyzed FROM `{your_project_id}.your_dataset.propaganda_technique_analysis`"
result_df = pandas_gbq.read_gbq(check_query, project_id=your_project_id)
print(f"Total your_dataset analyzed for propaganda techniques: {result_df['total_analyzed'].iloc[0]}")


import pandas_gbq

# --- Step 6: Network Analysis for Coordinated Campaigns ---
network_analysis_query = f"""
WITH advertiser_pairs AS (
  SELECT 
    p1.advertiser_legal_name AS advertiser_1,
    p2.advertiser_legal_name AS advertiser_2,
    COUNT(DISTINCT p1.region_code) AS shared_regions_count,
    AVG(ABS(DATE_DIFF(p1.first_shown_date, p2.first_shown_date, DAY))) AS avg_start_date_diff,
    AVG(ABS(DATE_DIFF(p1.last_shown_date, p2.last_shown_date, DAY))) AS avg_end_date_diff
  FROM `{your_project_id}.your_dataset.political_your_dataset_raw` p1
  JOIN `{your_project_id}.your_dataset.political_your_dataset_raw` p2
    ON p1.advertiser_legal_name < p2.advertiser_legal_name  -- prevent self-joins & duplicates
    AND p1.region_code = p2.region_code                     -- must share a region
  GROUP BY advertiser_1, advertiser_2
  HAVING shared_regions_count >= 3  -- require meaningful overlap
),

coordination_scores AS (
  SELECT
    advertiser_1,
    advertiser_2,
    shared_regions_count,
    avg_start_date_diff,
    avg_end_date_diff,
    -- Coordination suspicion score
    CAST((
      (shared_regions_count * 5) +
      (CASE WHEN avg_start_date_diff <= 14 THEN 20 ELSE 0 END) +
      (CASE WHEN avg_end_date_diff <= 14 THEN 20 ELSE 0 END)
    ) AS NUMERIC) AS coordination_suspicion_score
  FROM advertiser_pairs
)

SELECT *
FROM coordination_scores
ORDER BY coordination_suspicion_score DESC
LIMIT 10000
"""

# --- Step 6a: Run query and load into DataFrame ---
print("Running network analysis for coordinated campaigns...")
df_network = pandas_gbq.read_gbq(network_analysis_query, project_id=your_project_id)
print(f"✅ Retrieved {len(df_network)} rows from network analysis query")

# --- Step 6b: Save results back to BigQuery ---
print("Saving network analysis results into BigQuery table...")
df_network.to_gbq(
    destination_table="your_dataset.propaganda_network_analysis",
    project_id=your_project_id,
    if_exists="replace"  # overwrite old version
)
print("✅ Saved to BigQuery: your_dataset.propaganda_network_analysis")

# --- Step 6c: Verify row count ---
check_query = f"SELECT COUNT(*) as total_pairs FROM `{your_project_id}.your_dataset.propaganda_network_analysis`"
result_df = pandas_gbq.read_gbq(check_query, project_id=your_project_id)
print(f"Total advertiser pairs analyzed for coordination: {result_df['total_pairs'].iloc[0]}")


import pandas_gbq

# --- Step 7: Executive Propaganda Intelligence Report ---
executive_propaganda_report_query = f"""
CREATE OR REPLACE TABLE `{your_project_id}.your_dataset.propaganda_intelligence_report` AS
WITH summary_stats AS (
  SELECT 
    COUNT(DISTINCT creative_id) AS total_political_your_dataset_analyzed,
    COUNT(DISTINCT advertiser_legal_name) AS unique_political_advertisers,
    COUNT(CASE WHEN risk_classification = 'HIGH_RISK' THEN 1 END) AS high_risk_your_dataset,
    COUNT(CASE WHEN risk_classification IN ('HIGH_RISK', 'MEDIUM_RISK') THEN 1 END) AS total_risk_your_dataset,
    AVG(suspicion_score) AS avg_suspicion_score
  FROM `{your_project_id}.your_dataset.propaganda_technique_analysis`
),

network_insights AS (
  SELECT 
    COUNT(*) AS potential_coordination_networks,
    COUNT(CASE WHEN coordination_suspicion_score >= 70 THEN 1 END) AS high_suspicion_networks,
    AVG(coordination_suspicion_score) AS avg_network_suspicion,
    MAX(coordination_suspicion_score) AS max_network_suspicion
  FROM `{your_project_id}.your_dataset.propaganda_network_analysis`
),

top_risks AS (
  SELECT STRING_AGG(ad_str, '; ') AS top_risk_advertisers
  FROM (
    SELECT 
      CONCAT(advertiser_legal_name, ' (', risk_classification, ', Score: ', CAST(suspicion_score AS STRING), ')') AS ad_str
    FROM `{your_project_id}.your_dataset.propaganda_technique_analysis`
    WHERE risk_classification IN ('HIGH_RISK', 'MEDIUM_RISK')
    ORDER BY suspicion_score DESC
    LIMIT 5
  )
)

SELECT 
  'PROPAGANDA_DETECTION_INTELLIGENCE_REPORT' AS report_type,
  CONCAT(
    '🚨 PROPAGANDA PATTERN DETECTION REPORT\\n',
    '📊 ANALYSIS OVERVIEW:\\n',
    '• Political your_dataset analyzed: ', CAST(ss.total_political_your_dataset_analyzed AS STRING), '\\n',
    '• Unique political advertisers: ', CAST(ss.unique_political_advertisers AS STRING), '\\n',
    '• High-risk propaganda your_dataset identified: ', CAST(ss.high_risk_your_dataset AS STRING), '\\n',
    '• Total flagged your_dataset: ', CAST(ss.total_risk_your_dataset AS STRING), '\\n',
    '• Average suspicion score: ', CAST(ROUND(ss.avg_suspicion_score, 1) AS STRING), '/100\\n\\n',
    
    '🕸️ COORDINATION NETWORK ANALYSIS:\\n',
    '• Potential coordination networks detected: ', CAST(ni.potential_coordination_networks AS STRING), '\\n',
    '• High-suspicion networks: ', CAST(ni.high_suspicion_networks AS STRING), '\\n',
    '• Average network suspicion score: ', CAST(ROUND(ni.avg_network_suspicion, 1) AS STRING), '/100\\n',
    '• Highest network suspicion score: ', CAST(ROUND(ni.max_network_suspicion, 1) AS STRING), '/100\\n\\n',
    
    '⚡ TOP RISK ENTITIES:\\n',
    COALESCE(tr.top_risk_advertisers, 'None identified above threshold'), '\\n\\n',
    
    '💡 KEY RECOMMENDATIONS:\\n',
    '• Enhanced review for high-suspicion coordination networks\\n',
    '• Implement real-time monitoring for propaganda techniques\\n',
    '• Cross-reference with external threat intelligence databases\\n',
    '• Deploy automated content analysis for new political your_dataset\\n',
    '• Coordinate with regional policy teams for localized threats'
  ) AS intelligence_summary,
  ss.total_political_your_dataset_analyzed,
  ss.high_risk_your_dataset,
  ni.potential_coordination_networks,
  ni.high_suspicion_networks,
  CURRENT_DATETIME() AS report_generated_at
FROM summary_stats ss, network_insights ni, top_risks tr
"""

# --- Step 7a: Run the query to create the intelligence report table ---
print("Generating Executive Propaganda Intelligence Report...")
pandas_gbq.read_gbq(executive_propaganda_report_query, project_id=your_project_id)
print("✅ Executive intelligence report table created: your_dataset.propaganda_intelligence_report")

# --- Step 7b: Preview the report ---
preview_query = f"SELECT intelligence_summary FROM `{your_project_id}.your_dataset.propaganda_intelligence_report` LIMIT 1"
report_df = pandas_gbq.read_gbq(preview_query, project_id=your_project_id)
print(report_df.iloc[0]['intelligence_summary'])


"Loading the analysis tables to print the analysis here"

# Preview the campaign patterns
import pandas_gbq

df_patterns = pandas_gbq.read_gbq(
    f"SELECT * FROM `{your_project_id}.your_dataset.cross_region_patterns` LIMIT 20",
    project_id=your_project_id
)
print(df_patterns.head())


# 2. Preview propaganda techniques
df_techniques = pandas_gbq.read_gbq(
    f"SELECT advertiser_legal_name, propaganda_technique, suspicion_score, risk_classification \
       FROM `{your_project_id}.your_dataset.propaganda_technique_analysis` \
       ORDER BY suspicion_score DESC LIMIT 20",
    project_id=your_project_id
)
print(df_techniques)

# Pull the full executive intelligence report
df_report = pandas_gbq.read_gbq(
    f"SELECT * FROM `{your_project_id}.your_dataset.propaganda_intelligence_report`",
    project_id=your_project_id
)
print(df_report['intelligence_summary'].iloc[0])

