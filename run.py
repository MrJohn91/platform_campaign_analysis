import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime, timedelta
from pathlib import Path

class AdvertisingAnalyticsPipeline:
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.data_dir = self.project_root / 'data'
        self.output_dir = self.project_root / 'output'
        self._setup_directories()
        
    def _setup_directories(self):
        """Ensure required directories exist"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def clean_and_prepare_data(self):
        """Step 1: Data cleaning and preparation"""
        print("🔧 Cleaning and preparing data...")
        
        # Load raw data
        meta_df = pd.read_parquet(self.data_dir / 'meta.parquet')
        snapchat_df = pd.read_parquet(self.data_dir / 'snapchat.parquet')
        tiktok_df = pd.read_parquet(self.data_dir / 'tiktok.parquet')
        youtube_df = pd.read_parquet(self.data_dir / 'youtube.parquet')

        # Data processing
        snapchat_df['date'] = pd.to_datetime(snapchat_df['date_start'])
        tiktok_df['date'] = pd.to_datetime(tiktok_df['date'])
        youtube_df['date'] = pd.to_datetime(youtube_df['date'])

        youtube_df = youtube_df.rename(columns={
            'account_name=advertiser_name': 'advertiser_name',
            'line_item_id': 'campaign_id'
        })

        # Dropping duplicates and cleaning
        dfs = [meta_df, snapchat_df, tiktok_df, youtube_df]
        dfs = [df.drop_duplicates() for df in dfs]
        meta_df, snapchat_df, tiktok_df, youtube_df = dfs
        
        tiktok_df = tiktok_df.drop(columns=['device_type (#1)'])
        snapchat_df = snapchat_df.rename(columns={'video_views_p100': 'video_completions'})

        # Combine platform data
        visualization_metrics = ['campaign_id', 'date', 'impressions', 'clicks', 
                               'video_completions', 'device_type', 'advertiser_name']
        meta_df['date'] = meta_df['date'].astype('datetime64[ns]')
        
        meta_df['source'] = 'Meta'
        snapchat_df['source'] = 'Snapchat'
        tiktok_df['source'] = 'TikTok'
        youtube_df['source'] = 'YouTube'

        platform_data = pd.concat([
            meta_df[visualization_metrics + ['source']],
            snapchat_df[visualization_metrics + ['source']],
            tiktok_df[visualization_metrics + ['source']],
            youtube_df[visualization_metrics + ['source']]
        ], ignore_index=True)

        # Save processed data
        platform_data.to_csv(self.data_dir / 'platform_data.csv', index=False)
        meta_df.to_csv(self.data_dir / 'cleaned_meta.csv', index=False)
        
        return platform_data, meta_df

    def analyze_platforms(self, platform_data):
        """Step 2: Platform performance analysis"""
        print("📊 Analyzing platform performance...")
        
        # Visualization settings
        platform_colors = {
            'Meta': '#0866FF',
            'YouTube': '#F26522',
            'TikTok': '#444444',
            'Snapchat': '#c48a47'
        }
        
        device_colors = {
            'Desktop': '#0866FF',
            'SmartTV': '#F26522',
            'Smartphone': '#444444',
            'Tablet': '#c48a47'
        }

        # 1. Total Impressions by Platform
        impressions_by_source = platform_data.groupby('source')['impressions'].sum().reset_index()
        impressions_by_source['text'] = impressions_by_source['impressions'].apply(
            lambda x: f'{x/1e9:.1f}B' if x >= 1e9 else f'{x/1e6:.1f}M'
        )

        fig1 = px.bar(impressions_by_source.sort_values('impressions', ascending=False),
                     y='source', x='impressions', title='Total Impressions by Platform',
                     color='source', text='text', log_x=True, orientation='h',
                     color_discrete_map=platform_colors)
        fig1.update_traces(textposition='outside')
        fig1.update_layout(title={'x': 0.5}, width=1000, height=500)
        fig1.write_html(self.output_dir / 'Total_Impressions_by_Platform.html')

        # 2. CTR by Platform
        ctr_by_source = platform_data.groupby('source').agg({'clicks': 'sum', 'impressions': 'sum'}).reset_index()
        ctr_by_source['CTR (%)'] = (ctr_by_source['clicks'] / ctr_by_source['impressions']) * 100
        ctr_by_source = ctr_by_source.sort_values('CTR (%)', ascending=False)

        fig2 = px.bar(ctr_by_source, y='source', x='CTR (%)', title='Click Performance by Platform',
                      color='source', text='CTR (%)', orientation='h', color_discrete_map=platform_colors)
        fig2.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
        fig2.update_layout(title={'x': 0.5}, width=1000, height=500)
        fig2.update_xaxes(showticklabels=False)
        fig2.write_html(self.output_dir / 'CTR_by_Platform.html')

        # 3. Video Completion Metrics
        platform_metrics = []
        for platform_name in platform_data['source'].unique():
            platform_subset = platform_data[platform_data['source'] == platform_name]
            metrics = {
                'Platform': platform_name,
                'Completion per impression': (platform_subset['video_completions'].sum() / platform_subset['impressions'].sum()) * 100,
                'Completions per Click': platform_subset['video_completions'].sum() / max(1, platform_subset['clicks'].sum())
            }
            platform_metrics.append(metrics)

        completion_df = pd.DataFrame(platform_metrics)
        fig3 = make_subplots(rows=1, cols=2, subplot_titles=('Completion per Impression', 'Completions per Click'))
        
        for idx, metric in enumerate(['Completion per impression', 'Completions per Click'], 1):
            sorted_df = completion_df.sort_values(by=metric, ascending=False)
            for _, row in sorted_df.iterrows():
                fig3.add_trace(
                    go.Bar(
                        x=[row['Platform']],
                        y=[row[metric]],
                        name=row['Platform'],
                        marker_color=platform_colors[row['Platform']],
                        text=f"{row[metric]:.2f}{'%' if 'impression' in metric else ''}",
                        textposition='outside'
                    ),
                    row=1, col=idx
                )
        
        fig3.update_layout(
            height=600, width=1000,
            title_text="Video Completion Metrics by Platform",
            showlegend=False
        )
        fig3.write_html(self.output_dir / 'Video_Completion_Metrics_by_Platform.html')

        # 4. Video Completions by Device Type
        video_by_device = platform_data.groupby(['source', 'device_type'])['video_completions'].sum().reset_index()
        platform_totals = video_by_device.groupby('source')['video_completions'].sum().reset_index()
        video_by_device = video_by_device.merge(platform_totals, on='source', suffixes=('', '_total'))
        video_by_device['Percentage'] = (video_by_device['video_completions'] / video_by_device['video_completions_total']) * 100

        fig4 = px.bar(video_by_device,
                     y='source', x='video_completions', color='device_type',
                     title='Video Completions by Device Type and Platform',
                     log_x=True, orientation='h',
                     color_discrete_map=device_colors,
                     text=video_by_device['Percentage'].apply(lambda x: f'{x:.1f}%'))
        fig4.update_layout(title={'x': 0.5}, width=1000, height=500)
        fig4.write_html(self.output_dir / 'Video_Completions_by_Device_Type_and_Platform.html')

        # 5. Impressions Over Time
        platform_data['month_year'] = platform_data['date'].dt.to_period('M')
        impressions_over_time = platform_data.groupby(['month_year', 'source'])['impressions'].sum().reset_index()
        impressions_over_time['month_year'] = impressions_over_time['month_year'].dt.to_timestamp()

        fig5 = px.line(impressions_over_time, x='month_year', y='impressions', color='source',
                      title='Impressions Over Time by Platform', log_y=True, color_discrete_map=platform_colors)
        fig5.update_layout(title={'x': 0.5}, width=1000, height=500)
        fig5.write_html(self.output_dir / 'Impressions_Over_Time_by_Platform.html')

    def calculate_weekly_impressions(self, meta_df):
        """Step 3: Weekly impressions calculation"""
        print("📅 Calculating weekly impressions...")
        
        # Process meta data
        meta_df['Startdatum'] = pd.to_datetime(meta_df['Startdatum'])
        meta_df['Endedatum'] = pd.to_datetime(meta_df['Endedatum'])
        
        meta_campaigns = meta_df.groupby('campaign_id').agg({
            'Startdatum': 'min', 
            'Endedatum': 'max',
            'impressions': 'sum'
        }).reset_index().rename(columns={
            'campaign_id': 'Campaign_id',
            'Startdatum': 'start_date',
            'Endedatum': 'end_date',
            'impressions': 'Impression'
        })
        
        meta_campaigns['duration_days'] = (meta_campaigns['end_date'] - meta_campaigns['start_date']).dt.days + 1
        
        # Weekly calculations
        cw0_start = datetime(2016, 12, 26)
        weeks = [{'week': f'CW{i}', 
                 'start': cw0_start + timedelta(days=i*7),
                 'end': cw0_start + timedelta(days=i*7+6)} 
                for i in range(53)]
        weeks_df = pd.DataFrame(weeks)
        
        def split_impressions(row, weeks_df):
            start, end = row['start_date'], row['end_date']
            total_impressions = row['Impression']
            daily_impressions = total_impressions / max(1, (end - start).days + 1)
            
            impressions = []
            for _, week in weeks_df.iterrows():
                overlap_start = max(start, week['start'])
                overlap_end = min(end, week['end'])
                overlap_days = max(0, (overlap_end - overlap_start).days + 1)
                impressions.append(daily_impressions * overlap_days)
            
            # Convert to integers while preserving total
            int_impressions = [int(imp) for imp in impressions]
            remainder = int(total_impressions - sum(int_impressions))
            
            # Distribute remainder
            if remainder > 0:
                remainder_indices = sorted(
                    range(len(impressions)),
                    key=lambda i: impressions[i] - int_impressions[i],
                    reverse=True
                )[:remainder]
                for i in remainder_indices:
                    int_impressions[i] += 1
            
            return int_impressions + [sum(1 for imp in int_impressions if imp > 0)]

        # Apply the function
        weekly_data = meta_campaigns.apply(lambda row: split_impressions(row, weeks_df), axis=1)
        week_columns = [f'CW{i}' for i in range(53)] + ['total_weeks']
        weekly_df = pd.DataFrame(weekly_data.tolist(), columns=week_columns)
        
        # Combine with original data
        meta_weekly_impressions = pd.concat([meta_campaigns, weekly_df], axis=1)
        meta_weekly_impressions.to_csv(self.output_dir / 'meta_weekly_impressions.csv', index=False)
        
        # Generate visualizations
        weekly_totals = meta_weekly_impressions[[f'CW{i}' for i in range(53)]].sum()
        weekly_df = pd.DataFrame({
            'Week': weekly_totals.index,
            'Impressions': weekly_totals.values
        })
        
        # Top 3 weeks
        top_3_weeks = weekly_df.nlargest(3, 'Impressions')['Week'].tolist()
        weekly_df['Category'] = weekly_df['Week'].apply(lambda x: 'Top 3' if x in top_3_weeks else 'Other')
        
        fig6 = px.bar(weekly_df, x='Week', y='Impressions',
                     title='Total Impressions by Calendar Week',
                     color='Category',
                     color_discrete_map={'Top 3': '#FF6200', 'Other': '#c48a47'})
        fig6.update_layout(
            xaxis_title='Calendar Week',
            yaxis_title='Impressions',
            width=1000,
            height=600,
            xaxis={'categoryorder': 'array', 'categoryarray': weekly_df['Week'].tolist()},
            yaxis_type='log'
        )
        fig6.write_html(self.output_dir / 'total_impressions_by_week.html')
        
        # Weekly impressions by duration
        meta_weekly_impressions['avg_weekly_impressions'] = (
            meta_weekly_impressions['Impression'] / meta_weekly_impressions['total_weeks']
        )
        duration_analysis = meta_weekly_impressions.groupby('total_weeks').agg({
            'avg_weekly_impressions': 'mean',
            'Campaign_id': 'count'
        }).reset_index()
        
        fig7 = px.line(duration_analysis,
                      x='total_weeks', y='avg_weekly_impressions',
                      title='Weekly Impressions by Campaign Duration',
                      labels={'total_weeks': 'Campaign Duration (weeks)',
                              'avg_weekly_impressions': 'Average Weekly Impressions'})
        fig7.write_html(self.output_dir / 'weekly_impressions_by_duration.html')

    def run(self):
        """Execute full pipeline"""
        try:
            # Step 1
            platform_data, meta_df = self.clean_and_prepare_data()
            
            # Step 2
            self.analyze_platforms(platform_data)
            
            # Step 3
            self.calculate_weekly_impressions(meta_df)
            
            print("\nPipeline completed successfully!")
            print(f"Outputs saved to: {self.output_dir}")
        except Exception as e:
            print(f"\nPipeline failed: {str(e)}")
            raise

if __name__ == "__main__":
    pipeline = AdvertisingAnalyticsPipeline()
    pipeline.run()