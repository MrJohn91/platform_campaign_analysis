import os
import sys
import traceback

def main():
    try:
        # Set up paths relative to this script
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        DATA_DIR = os.path.join(BASE_DIR, 'data')
        OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
        
        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Add scripts directory to path
        SCRIPTS_DIR = os.path.join(BASE_DIR, 'scripts')
        if not os.path.exists(SCRIPTS_DIR):
            raise FileNotFoundError(f"Scripts directory not found: {SCRIPTS_DIR}")
        
        sys.path.insert(0, SCRIPTS_DIR)
        
        # Check if required data files exist
        required_files = ['meta.parquet', 'snapchat.parquet', 'tiktok.parquet', 'youtube.parquet']
        missing_files = [f for f in required_files if not os.path.exists(os.path.join(DATA_DIR, f))]
        if missing_files:
            raise FileNotFoundError(f"Missing required data files: {', '.join(missing_files)}")
        
        print("Starting analysis pipeline...")
        
        # Step 1: Data cleaning and preparation
        print("\nStep 1: Data cleaning and preparation")
        try:
            from scripts.platform_cleaning_preparation import run as clean_data
            platform_data, meta_df = clean_data(DATA_DIR, OUTPUT_DIR)
            print("✓ Data cleaning and preparation completed successfully")
        except Exception as e:
            print(f"✗ Error in data cleaning and preparation: {str(e)}")
            raise
        
        # Step 2: Platform analysis and visualization
        print("\nStep 2: Platform analysis and visualization")
        try:
            from scripts.platform_analysis import run as analyze_platforms
            analyze_platforms(DATA_DIR, OUTPUT_DIR)
            print("✓ Platform analysis and visualization completed successfully")
        except Exception as e:
            print(f"✗ Error in platform analysis: {str(e)}")
            raise
        
        # Step 3: Meta weekly impression analysis
        print("\nStep 3: Meta weekly impression analysis")
        try:
            from scripts.meta_weekly_impression import run as analyze_weekly
            meta_weekly_impressions = analyze_weekly(DATA_DIR, OUTPUT_DIR)
            print("✓ Meta weekly impression analysis completed successfully")
        except Exception as e:
            print(f"✗ Error in Meta weekly impression analysis: {str(e)}")
            raise
        
        print("\nAnalysis complete! Results saved to the output directory.")
        print(f"Output directory: {OUTPUT_DIR}")
        print("\nTo view the visualizations, open the following HTML files in your web browser:")
        print(f"- {os.path.join(OUTPUT_DIR, 'Total_Impressions_by_Platform.html')}")
        print(f"- {os.path.join(OUTPUT_DIR, 'CTR_by_Platform.html')}")
        print(f"- {os.path.join(OUTPUT_DIR, 'Video_Completion_Metrics_by_Platform.html')}")
        print(f"- {os.path.join(OUTPUT_DIR, 'Video_Completions_by_Device_Type_and_Platform.html')}")
        print(f"- {os.path.join(OUTPUT_DIR, 'Impressions_Over_Time_by_Platform.html')}")
        print(f"- {os.path.join(OUTPUT_DIR, 'total_impressions_by_week.html')}")
        print(f"- {os.path.join(OUTPUT_DIR, 'weekly_impressions_by_duration.html')}")
        
        return 0
    except FileNotFoundError as e:
        print(f"ERROR: {str(e)}")
        print("Please ensure all required files and directories are present.")
        return 1
    except ImportError as e:
        print(f"ERROR: Could not import required module - {str(e)}")
        print("Please ensure all dependencies are installed by running: pip install -r requirements.txt")
        return 1
    except Exception as e:
        print(f"ERROR: An unexpected error occurred:")
        print(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())