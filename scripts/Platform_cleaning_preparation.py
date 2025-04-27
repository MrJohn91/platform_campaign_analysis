# Importing libraries
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime, timedelta

# Reading the parquet files
meta_df = pd.read_parquet('../data/meta.parquet')
snapchat_df = pd.read_parquet('../data/snapchat.parquet')
tiktok_df = pd.read_parquet('../data/tiktok.parquet')
youtube_df = pd.read_parquet('../data/youtube.parquet')

# Converting string dates to datetime format
snapchat_df['date'] = pd.to_datetime(snapchat_df['date_start'])
tiktok_df['date'] = pd.to_datetime(tiktok_df['date'])
youtube_df['date'] = pd.to_datetime(youtube_df['date'])

# Renaming YouTube columns for consistency
youtube_df = youtube_df.rename(columns={
    'account_name=advertiser_name': 'advertiser_name',
    'line_item_id': 'campaign_id'
})

# Dropping duplicates
meta_df = meta_df.drop_duplicates()
snapchat_df = snapchat_df.drop_duplicates()
tiktok_df = tiktok_df.drop_duplicates()
youtube_df = youtube_df.drop_duplicates()

# Dropping TikTok's device_type column
tiktok_df = tiktok_df.drop(columns=['device_type (#1)'])

# Renaming Snapchat's video_views_p100 to video_completions
snapchat_df = snapchat_df.rename(columns={'video_views_p100': 'video_completions'})

# Defining columns needed for visualizations
visualization_metrics = ['campaign_id', 'date', 'impressions', 'clicks', 'video_completions', 'device_type', 'advertiser_name']

meta_df['date'] = meta_df['date'].astype('datetime64[ns]')

# Adding the source column for each platform
meta_df['source'] = 'Meta'
snapchat_df['source'] = 'Snapchat'
tiktok_df['source'] = 'TikTok'
youtube_df['source'] = 'YouTube'

# Combining all datasets into one table for platform comparisons
platform_data = pd.concat([
    meta_df[visualization_metrics + ['source']],
    snapchat_df[visualization_metrics + ['source']],
    tiktok_df[visualization_metrics + ['source']],
    youtube_df[visualization_metrics + ['source']]
], ignore_index=True)

# Check if 'data' directory exists, if not, create it
if not os.path.exists('../data'):
    os.makedirs('../data')

# Saving the combined data and cleaned data to CSV
platform_data.to_csv('../data/platform_data.csv', index=False)
meta_df.to_csv('../data/cleaned_meta.csv', index=False)
