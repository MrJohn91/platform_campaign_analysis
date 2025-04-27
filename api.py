from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from media_analytics_pipeline import AdvertisingAnalyticsPipeline
from pathlib import Path
import uvicorn

app = FastAPI(
    title="Media Analytics API",
    description="API for generating performance reports",
    version="1.0.0"
)

@app.post("/run-pipeline")
async def run_pipeline():
    """Execute the full analytics pipeline"""
    try:
        pipeline = AdvertisingAnalyticsPipeline()
        pipeline.run()
        return JSONResponse(
            content={"status": "success", "message": "Pipeline executed successfully"},
            status_code=200
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Pipeline failed: {str(e)}"
        )

@app.get("/download-report/{filename}")
async def download_report(filename: str):
    """Download generated report files"""
    report_path = Path("/app/output") / filename
    if not report_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Report not found. Generate it first via /run-pipeline"
        )
    return FileResponse(
        report_path,
        media_type='application/octet-stream',
        filename=filename
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)