import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

def run(data_dir, output_dir):
    # Load platform dataset
    platform_data = pd.read_csv(os.path.join(data_dir, 'platform_data.csv'))
    
    # Converting date column to datetime, handling mixed formats
    platform_data['date'] = pd.to_datetime(platform_data['date'], format='mixed')
    
    # Total Impressions by Platform
    impressions_by_source = platform_data.groupby('source')['impressions'].sum().reset_index()
    impressions_by_source['text'] = impressions_by_source['impressions'].apply(
        lambda x: f'{x/1e9:.1f}B' if x >= 1e9 else f'{x/1e6:.1f}M'
    )
    
    platform_colors = {
        'Meta': '#0866FF',
        'YouTube': '#F26522',
        'TikTok': '#444444',
        'Snapchat': '#c48a47'
    }
    
    fig = px.bar(impressions_by_source.sort_values('impressions', ascending=False),
                 y='source',
                 x='impressions',
                 title='Total Impressions by Platform',
                 labels={'source': 'Platform', 'impressions': 'Impressions'},
                 color='source',
                 text='text',
                 log_x=True,
                 orientation='h',
                 color_discrete_map=platform_colors)
    
    fig.update_traces(textposition='outside')
    fig.update_layout(title={'x': 0.5}, width=1000, height=500)
    fig.write_html(os.path.join(output_dir, 'Total_Impressions_by_Platform.html'))
    
    # CTR by Platform
    ctr_by_source = platform_data.groupby('source').agg({'clicks': 'sum', 'impressions': 'sum'}).reset_index()
    ctr_by_source['CTR (%)'] = (ctr_by_source['clicks'] / ctr_by_source['impressions']) * 100
    ctr_by_source = ctr_by_source.sort_values('CTR (%)', ascending=False)
    
    fig2 = px.bar(ctr_by_source,
                  y='source',
                  x='CTR (%)',
                  title='Click Performance by Platform',
                  labels={'source': 'Platform', 'CTR (%)': ''},
                  color='source',
                  text='CTR (%)',
                  orientation='h',
                  color_discrete_map=platform_colors)
    
    fig2.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
    fig2.update_layout(title={'x': 0.5}, width=1000, height=500)
    fig2.update_xaxes(showticklabels=False)
    fig2.write_html(os.path.join(output_dir, 'CTR_by_Platform.html'))
    
    # Video Completion Metrics by Platform
    platform_metrics = []
    for platform_name in platform_data['source'].unique():
        platform_subset = platform_data[platform_data['source'] == platform_name]
        platform_impressions = platform_subset['impressions'].sum()
        platform_clicks = platform_subset['clicks'].sum()
        platform_completions = platform_subset['video_completions'].sum()
        platform_metrics.append({
            'Platform': platform_name,
            'Completion per impression': (platform_completions / platform_impressions) * 100,
            'Completions per Click': platform_completions / max(1, platform_clicks)
        })
    
    completion_df = pd.DataFrame(platform_metrics)
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Completion per Impression', 'Completions per Click'),
        vertical_spacing=0.1
    )
    
    metrics = ['Completion per impression', 'Completions per Click']
    for idx, metric in enumerate(metrics):
        sorted_df = completion_df.sort_values(by=metric, ascending=False)
        for _, row in sorted_df.iterrows():
            platform = row['Platform']
            value = row[metric]
            text_label = f'{value:.2f}%' if metric == 'Completion per impression' else f'{value:.2f}'
            fig.add_trace(
                go.Bar(
                    x=[platform],
                    y=[value],
                    name=platform if idx == 0 else None,
                    marker_color=platform_colors[platform],
                    text=text_label,
                    textposition='outside',
                    showlegend=(idx == 0)
                ),
                row=1, col=idx+1
            )
    
    fig.update_layout(
        height=600,
        width=1000,
        title_text="Video Completion Metrics by Platform",
        title_x=0.5,
        title_y=0.95,
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
        margin=dict(t=150, b=100)
    )
    fig.update_yaxes(title_text="Rate (%)", row=1, col=1, range=[0, 70])
    fig.update_yaxes(title_text="Ratio", row=1, col=2, range=[0, 150])
    fig.write_html(os.path.join(output_dir, 'Video_Completion_Metrics_by_Platform.html'))
    
    # Video Completions by Device Type and Platform
    video_by_device = platform_data.groupby(['source', 'device_type'])['video_completions'].sum().reset_index()
    platform_totals = video_by_device.groupby('source')['video_completions'].sum().reset_index()
    sorted_platforms = platform_totals.sort_values('video_completions', ascending=False)['source'].tolist()
    video_by_device = video_by_device.merge(platform_totals, on='source', suffixes=('', '_total'))
    video_by_device['Percentage'] = (video_by_device['video_completions'] / video_by_device['video_completions_total']) * 100
    
    device_colors = {
        'Desktop': '#0866FF',
        'SmartTV': '#F26522',
        'Smartphone': '#444444',
        'Tablet': '#c48a47'
    }
    
    fig3 = px.bar(video_by_device,
                  y='source',
                  x='video_completions',
                  color='device_type',
                  title='Video Completions by Device Type and Platform',
                  labels={'source': 'Platform', 'video_completions': 'Video Completions', 'device_type': 'Device Type'},
                  log_x=True,
                  orientation='h',
                  category_orders={'source': sorted_platforms},
                  color_discrete_map=device_colors,
                  text=video_by_device['Percentage'].apply(lambda x: f'{x:.1f}%'))
    
    fig3.update_layout(title={'x': 0.5}, width=1000, height=500)
    fig3.write_html(os.path.join(output_dir, 'Video_Completions_by_Device_Type_and_Platform.html'))
    
    # Impressions Over Time by Platform
    platform_data['month_year'] = platform_data['date'].dt.to_period('M')
    impressions_over_time = platform_data.groupby(['month_year', 'source'])['impressions'].sum().reset_index()
    impressions_over_time['month_year'] = impressions_over_time['month_year'].dt.to_timestamp()
    
    fig4 = px.line(impressions_over_time,
                   x='month_year',
                   y='impressions',
                   color='source',
                   title='Impressions Over Time by Platform',
                   labels={'month_year': 'Date', 'impressions': 'Impressions', 'source': 'Platform'},
                   log_y=True,
                   color_discrete_map=platform_colors)
    
    fig4.update_layout(title={'x': 0.5}, width=1000, height=500)
    fig4.write_html(os.path.join(output_dir, 'Impressions_Over_Time_by_Platform.html'))
    
    print("Platform analysis and visualizations complete!")
    return platform_data

# This allows the script to be run directly or imported
if __name__ == "__main__":
    # Default directories if run directly
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    output_dir = os.path.join(base_dir, 'output')
    
    # Only create output directory here for standalone execution
    os.makedirs(output_dir, exist_ok=True)
    
    run(data_dir, output_dir)
