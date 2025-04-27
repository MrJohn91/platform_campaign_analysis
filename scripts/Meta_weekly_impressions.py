import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Create the output visualizations directory if it doesn't exist
output_vis_dir = '../outputs/visualizations'
os.makedirs(output_vis_dir, exist_ok=True)

# Load the cleaned Meta dataset with error handling
try:
    meta_df = pd.read_csv('../data/cleaned_meta.csv')
except FileNotFoundError:
    print("Error: cleaned_meta.csv not found in data folder.")
    exit(1)
except Exception as e:
    print(f"Error loading cleaned_meta.csv: {e}")
    exit(1)

# Date columns to datetime with format
meta_df['Startdatum'] = pd.to_datetime(meta_df['Startdatum'], format='mixed')
meta_df['Endedatum'] = pd.to_datetime(meta_df['Endedatum'], format='mixed')
meta_df['date'] = pd.to_datetime(meta_df['date'], format='mixed')

# Grouping by campaign_id
meta_campaigns = meta_df.groupby(['campaign_id']).agg({
    'Startdatum': 'min',  
    'Endedatum': 'max',   
    'impressions': 'sum',  
}).reset_index()

# Renaming columns
meta_campaigns = meta_campaigns.rename(columns={
    'campaign_id': 'Campaign_id',
    'Startdatum': 'start_date',
    'Endedatum': 'end_date',
    'impressions': 'Impression'
})

# Calculating campaign duration in days
meta_campaigns['duration_days'] = (meta_campaigns['end_date'] - meta_campaigns['start_date']).dt.days + 1

# Create weeks for CW0 to CW52
cw0_start = datetime(2016, 12, 26)
weeks = []

for i in range(53):
    start_date = cw0_start + timedelta(days=i*7)
    end_date = start_date + timedelta(days=6)
    weeks.append({'week': f'CW{i}', 'start': start_date, 'end': end_date})

weeks_df = pd.DataFrame(weeks)

weeks_df['start'] = pd.to_datetime(weeks_df['start'])
weeks_df['end'] = pd.to_datetime(weeks_df['end'])

def split_impressions(row, weeks_df):
    start, end = row['start_date'], row['end_date']
    total_impressions = row['Impression']
    
    daily_impressions = total_impressions / max(1, (end - start).days + 1)
    impressions = []
    weeks_count = 0
    
    for _, week in weeks_df.iterrows():
        week_start = week['start'].normalize()
        week_end = week['end'].normalize()
        
        overlap_start = max(start, week_start)
        overlap_end = min(end, week_end)
        
        if overlap_end < overlap_start:
            overlap_days = 0
        else:
            overlap_days = (overlap_end - overlap_start).days + 1
        
        impression_for_week = daily_impressions * overlap_days
        impressions.append(impression_for_week)
        
        if overlap_days > 0:
            weeks_count += 1
    
    int_impressions = [int(imp) for imp in impressions]
    remainder_tuples = sorted([(imp - int(imp), i) for i, imp in enumerate(impressions)], reverse=True)
    
    difference = int(total_impressions - sum(int_impressions))
    for i in range(difference):
        if i < len(remainder_tuples):
            int_impressions[remainder_tuples[i][1]] += 1
    
    return int_impressions + [weeks_count]

# Applying the function to create weekly impressions
weekly_data = meta_campaigns.apply(lambda row: split_impressions(row, weeks_df), axis=1)
week_columns = [f'CW{i}' for i in range(53)] + ['total_weeks']
weekly_df = pd.DataFrame(weekly_data.tolist(), columns=week_columns)

# Combine with the original meta_campaigns
meta_weekly_impressions = pd.concat([meta_campaigns, weekly_df], axis=1)

# Save the result to a CSV file with error handling
try:
    meta_weekly_impressions.to_csv('../outputs/meta_weekly_impressions.csv', index=False)
except Exception as e:
    print(f"Error saving meta_weekly_impressions.csv: {e}")
    exit(1)

# Calculate total impressions by week and plot
weekly_totals = meta_weekly_impressions[[f'CW{i}' for i in range(53)]].sum()
weekly_df = pd.DataFrame({
    'Week': weekly_totals.index,
    'Impressions': weekly_totals.values
})
top_3_df = weekly_df.sort_values(by='Impressions', ascending=False).head(3)
top_3_weeks = top_3_df['Week'].tolist()

# Add a category column for coloring
weekly_df['Category'] = weekly_df['Week'].apply(lambda x: 'Top 3' if x in top_3_weeks else 'Other')

# Create the bar chart with color mapping
fig = px.bar(weekly_df, 
             x='Week', 
             y='Impressions',
             title='Total Impressions by Calendar Week',
             color='Category', 
             color_discrete_map={'Top 3': '#FF6200', 'Other': '#c48a47'},  
             hover_data={'Impressions': ':,.0f'}) 
fig.update_layout(
    xaxis_title='Calendar Week',
    yaxis_title='Impressions',
    width=1000,
    height=600,
    xaxis={'categoryorder': 'array', 'categoryarray': weekly_df['Week'].tolist()},
    yaxis_type='log',
    font=dict(size=12),
    title_font=dict(size=18),
    title={'text': 'Total Impressions by Calendar Week', 'x': 0.5, 'xanchor': 'center'}
)

# Save the plot with error handling
try:
    fig.write_html(os.path.join(output_vis_dir, 'total_impressions_by_week.html'))
except Exception as e:
    print(f"Error saving total_impressions_by_week.html: {e}")
    exit(1)

# Calculating average weekly impressions by campaign duration
meta_weekly_impressions['avg_weekly_impressions'] = meta_weekly_impressions['Impression'] / meta_weekly_impressions['total_weeks']
duration_analysis = meta_weekly_impressions.groupby('total_weeks').agg({
    'avg_weekly_impressions': 'mean',
    'Campaign_id': 'count'
}).reset_index()

# Line chart
fig2 = px.line(duration_analysis, 
               x='total_weeks', 
               y='avg_weekly_impressions',
               labels={'total_weeks': 'Campaign Duration', 'avg_weekly_impressions': 'Average Impression'},
               title='Weekly Impressions by Campaign Duration')

# Save the plot with error handling
try:
    fig2.write_html(os.path.join(output_vis_dir, 'weekly_impressions_by_duration.html'))
except Exception as e:
    print(f"Error saving weekly_impressions_by_duration.html: {e}")
    exit(1)
