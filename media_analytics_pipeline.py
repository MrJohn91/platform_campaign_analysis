import pandas as pd
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
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def clean_and_prepare_data(self):
        meta_df = pd.read_parquet(self.data_dir / 'meta.parquet')
        snapchat_df = pd.read_parquet(self.data_dir / 'snapchat.parquet')
        tiktok_df = pd.read_parquet(self.data_dir / 'tiktok.parquet')
        youtube_df = pd.read_parquet(self.data_dir / 'youtube.parquet')

        snapchat_df['date'] = pd.to_datetime(snapchat_df['date_start'])
        tiktok_df['date'] = pd.to_datetime(tiktok_df['date'])
        youtube_df['date'] = pd.to_datetime(youtube_df['date'])

        youtube_df = youtube_df.rename(columns={
            'account_name=advertiser_name': 'advertiser_name',
            'line_item_id': 'campaign_id'
        })

        meta_df, snapchat_df, tiktok_df, youtube_df = [
            df.drop_duplicates() for df in [meta_df, snapchat_df, tiktok_df, youtube_df]
        ]

        tiktok_df = tiktok_df.drop(columns=['device_type (#1)'])
        snapchat_df = snapchat_df.rename(columns={'video_views_p100': 'video_completions'})

        meta_df['date'] = meta_df['date'].astype('datetime64[ns]')
        meta_df['source'] = 'Meta'
        snapchat_df['source'] = 'Snapchat'
        tiktok_df['source'] = 'TikTok'
        youtube_df['source'] = 'YouTube'

        platform_data = pd.concat([
            df[['campaign_id', 'date', 'impressions', 'clicks', 'video_completions', 'device_type', 'advertiser_name', 'source']]
            for df in [meta_df, snapchat_df, tiktok_df, youtube_df]
        ], ignore_index=True)

        platform_data.to_csv(self.data_dir / 'platform_data.csv', index=False)
        meta_df.to_csv(self.data_dir / 'cleaned_meta.csv', index=False)
        return platform_data, meta_df

    def analyze_platforms(self, platform_data):
        platform_colors = {'Meta': '#0866FF', 'YouTube': '#F26522', 'TikTok': '#444444', 'Snapchat': '#c48a47'}
        device_colors = {'Desktop': '#0866FF', 'SmartTV': '#F26522', 'Smartphone': '#444444', 'Tablet': '#c48a47'}

        impressions = platform_data.groupby('source')['impressions'].sum().reset_index()
        impressions['text'] = impressions['impressions'].apply(lambda x: f'{x/1e9:.1f}B' if x >= 1e9 else f'{x/1e6:.1f}M')
        fig1 = px.bar(impressions.sort_values('impressions', ascending=False),
                     y='source', x='impressions', title='Total Impressions by Platform',
                     color='source', text='text', log_x=True, orientation='h',
                     color_discrete_map=platform_colors)
        fig1.update_traces(textposition='outside').update_layout(title={'x': 0.5}, width=1000, height=500)
        fig1.write_html(self.output_dir / 'Total_Impressions_by_Platform.html')

        ctr = platform_data.groupby('source').agg({'clicks': 'sum', 'impressions': 'sum'}).reset_index()
        ctr['CTR (%)'] = (ctr['clicks'] / ctr['impressions']) * 100
        fig2 = px.bar(ctr.sort_values('CTR (%)', ascending=False),
                     y='source', x='CTR (%)', title='Click Performance by Platform',
                     color='source', text='CTR (%)', orientation='h', color_discrete_map=platform_colors)
        fig2.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
        fig2.update_layout(title={'x': 0.5}, width=1000, height=500).update_xaxes(showticklabels=False)
        fig2.write_html(self.output_dir / 'CTR_by_Platform.html')

        platform_metrics = []
        for platform in platform_data['source'].unique():
            subset = platform_data[platform_data['source'] == platform]
            platform_metrics.append({
                'Platform': platform,
                'Completion per impression': (subset['video_completions'].sum() / subset['impressions'].sum()) * 100,
                'Completions per Click': subset['video_completions'].sum() / max(1, subset['clicks'].sum())
            })

        fig3 = make_subplots(rows=1, cols=2, subplot_titles=('Completion per Impression', 'Completions per Click'))
        for idx, metric in enumerate(['Completion per impression', 'Completions per Click'], 1):
            for _, row in pd.DataFrame(platform_metrics).sort_values(metric, ascending=False).iterrows():
                fig3.add_trace(go.Bar(
                    x=[row['Platform']], y=[row[metric]], name=row['Platform'],
                    marker_color=platform_colors[row['Platform']],
                    text=f"{row[metric]:.2f}{'%' if 'impression' in metric else ''}",
                    textposition='outside'), row=1, col=idx)
        fig3.update_layout(height=600, width=1000, title_text="Video Completion Metrics by Platform", showlegend=False)
        fig3.write_html(self.output_dir / 'Video_Completion_Metrics_by_Platform.html')

        video_by_device = platform_data.groupby(['source', 'device_type'])['video_completions'].sum().reset_index()
        video_by_device = video_by_device.merge(
            video_by_device.groupby('source')['video_completions'].sum().reset_index(),
            on='source', suffixes=('', '_total'))
        video_by_device['Percentage'] = (video_by_device['video_completions'] / video_by_device['video_completions_total']) * 100
        fig4 = px.bar(video_by_device, y='source', x='video_completions', color='device_type',
                     title='Video Completions by Device Type and Platform', log_x=True, orientation='h',
                     color_discrete_map=device_colors, text=video_by_device['Percentage'].apply(lambda x: f'{x:.1f}%'))
        fig4.update_layout(title={'x': 0.5}, width=1000, height=500)
        fig4.write_html(self.output_dir / 'Video_Completions_by_Device_Type_and_Platform.html')

        platform_data['month_year'] = platform_data['date'].dt.to_period('M')
        impressions_over_time = platform_data.groupby(['month_year', 'source'])['impressions'].sum().reset_index()
        impressions_over_time['month_year'] = impressions_over_time['month_year'].dt.to_timestamp()
        fig5 = px.line(impressions_over_time, x='month_year', y='impressions', color='source',
                      title='Impressions Over Time by Platform', log_y=True, color_discrete_map=platform_colors)
        fig5.update_layout(title={'x': 0.5}, width=1000, height=500)
        fig5.write_html(self.output_dir / 'Impressions_Over_Time_by_Platform.html')

    def calculate_weekly_impressions(self, meta_df):
        meta_df['Startdatum'] = pd.to_datetime(meta_df['Startdatum'])
        meta_df['Endedatum'] = pd.to_datetime(meta_df['Endedatum'])
        
        campaigns = meta_df.groupby('campaign_id').agg({
            'Startdatum': 'min', 'Endedatum': 'max', 'impressions': 'sum'
        }).reset_index().rename(columns={
            'campaign_id': 'Campaign_id', 'Startdatum': 'start_date', 
            'Endedatum': 'end_date', 'impressions': 'Impression'
        })
        campaigns['duration_days'] = (campaigns['end_date'] - campaigns['start_date']).dt.days + 1

        weeks = [{'week': f'CW{i}', 'start': datetime(2016, 12, 26) + timedelta(days=i*7),
                 'end': datetime(2016, 12, 26) + timedelta(days=i*7+6)} for i in range(53)]
        weeks_df = pd.DataFrame(weeks)

        def split_impressions(row, weeks_df):
            daily_imp = row['Impression'] / max(1, row['duration_days'])
            impressions = []
            for _, week in weeks_df.iterrows():
                overlap_days = (min(row['end_date'], week['end']) - max(row['start_date'], week['start'])).days + 1
                impressions.append(daily_imp * max(0, overlap_days))
            
            int_imp = [int(x) for x in impressions]
            remainder = int(row['Impression'] - sum(int_imp))
            for i in sorted(range(len(impressions)), key=lambda i: impressions[i]-int_imp[i], reverse=True)[:remainder]:
                int_imp[i] += 1
            
            return int_imp + [sum(1 for x in int_imp if x > 0)]

        weekly_data = campaigns.apply(lambda row: split_impressions(row, weeks_df), axis=1)
        weekly_df = pd.DataFrame(weekly_data.tolist(), columns=[f'CW{i}' for i in range(53)] + ['total_weeks'])
        result = pd.concat([campaigns, weekly_df], axis=1)
        result.to_csv(self.output_dir / 'meta_weekly_impressions.csv', index=False)

        weekly_totals = result[[f'CW{i}' for i in range(53)]].sum()
        top_weeks = weekly_totals.nlargest(3).index.tolist()
        weekly_plot = pd.DataFrame({'Week': weekly_totals.index, 'Impressions': weekly_totals.values})
        weekly_plot['Category'] = weekly_plot['Week'].apply(lambda x: 'Top 3' if x in top_weeks else 'Other')
        
        fig6 = px.bar(weekly_plot, x='Week', y='Impressions', color='Category',
                     color_discrete_map={'Top 3': '#FF6200', 'Other': '#c48a47'})
        fig6.update_layout(xaxis_title='Calendar Week', yaxis_title='Impressions', width=1000, height=600,
                          xaxis={'categoryorder': 'array', 'categoryarray': weekly_plot['Week'].tolist()}, yaxis_type='log')
        fig6.write_html(self.output_dir / 'total_impressions_by_week.html')

        result['avg_weekly_impressions'] = result['Impression'] / result['total_weeks']
        duration_analysis = result.groupby('total_weeks').agg({'avg_weekly_impressions': 'mean', 'Campaign_id': 'count'}).reset_index()
        fig7 = px.line(duration_analysis, x='total_weeks', y='avg_weekly_impressions',
                      title='Weekly Impressions by Campaign Duration')
        fig7.write_html(self.output_dir / 'weekly_impressions_by_duration.html')

    def run(self):
        try:
            platform_data, meta_df = self.clean_and_prepare_data()
            self.analyze_platforms(platform_data)
            self.calculate_weekly_impressions(meta_df)
            print("✅ Pipeline completed successfully")
        except Exception as e:
            print(f"❌ Pipeline failed: {e}")
            raise

if __name__ == "__main__":
    AdvertisingAnalyticsPipeline().run()