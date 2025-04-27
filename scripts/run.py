import os
import subprocess

def run_script(script_name):
    try:
        subprocess.run(['python', script_name], check=True)
        print(f"✓ {script_name} completed successfully")
    except subprocess.CalledProcessError:
        print(f"✗ {script_name} failed")

def main():
    # Correct paths to reference the Python scripts directly in the 'scripts' directory
    scripts = [
        'scripts/Platform_cleaning_preparation.py',
        'scripts/Platform_analysis.py',
        'scripts/Meta_weekly_impressions.py'
    ]
    
    # Running each script in the list
    for script in scripts:
        run_script(script)
    
    print("\nPipeline complete! Check the output directory for results.")

if __name__ == "__main__":
    main()
