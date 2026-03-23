import os
import shutil
import asyncio
from pathlib import Path
from tempfile import NamedTemporaryFile

from fastapi import FastAPI, UploadFile, File, BackgroundTasks
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from analyst.config import load_config
from analyst.orchestrator import LLMOrchestrator
from analyst.state import AnalysisState
from analyst.webapp.sse import SSEStreamer

app = FastAPI(title="Multi-Agent Analyst API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store runs in memory for this simple dashboard
RUNS: dict[str, AnalysisState] = {}

# We will mount static files at exactly "/" later, but for API dev we keep it separate
STATIC_DIR = Path(__file__).parent / "static"
if not STATIC_DIR.exists():
    STATIC_DIR.mkdir(parents=True, exist_ok=True)

app.mount("/app", StaticFiles(directory=STATIC_DIR, html=True), name="static")

from fastapi.responses import RedirectResponse
@app.get("/")
def root():
    return RedirectResponse(url="/app/")


@app.post("/api/upload")
async def upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...), mode: str = "full"):
    """Accept a file, save it to temp, and kick off the pipeline."""
    import uuid
    run_id = str(uuid.uuid4())[:8]
    
    # Save the uploaded file securely
    temp_dir = Path("output") / run_id / "data"
    temp_dir.mkdir(parents=True, exist_ok=True)
    file_path = temp_dir / file.filename
    
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        
    def run_pipeline_sync_local():
        from analyst.event_emitter import global_emitter
        from analyst.digest import load_cache, save_cache

        try:
            cached_state = load_cache(file_path)
            
            if cached_state:
                global_emitter.emit("log", {"agent": "system", "message": "♻ Cache hit — loaded digest from disk", "severity": "INFO", "is_done": False})
                cached_state.output_dir = Path("output") / run_id
                state = cached_state
                skip_up_to = "eda"
            else:
                cfg = load_config(None)
                output_dir = Path("output") / run_id
                state = AnalysisState(file_path=file_path, output_dir=output_dir, config=cfg)
                skip_up_to = None

            RUNS[run_id] = state
            
            if mode == "profile":
                if not cached_state:
                    from analyst.agents.ingestion import IngestionAgent
                    from analyst.agents.profiling import ProfilerAgent
                    
                    IngestionAgent().execute(state)
                    ProfilerAgent().execute(state)
                    
                    save_cache(state)
                
                global_emitter.emit("log", {"agent": "system", "message": "Profile complete", "severity": "SUCCESS", "is_done": True})
            else:
                from analyst.orchestrator import DeterministicPipeline
                pipeline = DeterministicPipeline(state=state, skip_up_to=skip_up_to)
                pipeline.run()
                
                global_emitter.emit("log", {"agent": "system", "message": "Pipeline complete", "severity": "SUCCESS", "is_done": True})
            
        except Exception as e:
            global_emitter.emit("log", {"agent": "system", "message": f"Error: {e}", "severity": "ERROR", "is_done": True})

    # Start the analyst pipeline in the background
    background_tasks.add_task(run_pipeline_sync_local)
    
    return {"run_id": run_id, "filename": file.filename, "message": "Pipeline started", "mode": mode}



@app.get("/api/stream")
async def stream_progress():
    """Stream live events from the LLM agents to the browser."""
    streamer = SSEStreamer()
    return StreamingResponse(streamer.sse_generator(), media_type="text/event-stream")


@app.get("/api/results/{run_id}")
async def get_results(run_id: str):
    """Poll for final results including profile, eda, charts, report."""
    state = RUNS.get(run_id)
    if not state:
        return {"error": "Run not found or not started yet"}
        
    charts = [p.name for p in state.visualizations]
    report = state.report_path.read_text() if state.report_path and state.report_path.exists() else None
    
    return {
        "profile": [{"name": p.name, "type": p.dtype, "nulls": p.null_count, "unique": p.unique_count} for p in state.profile],
        "eda_results": state.eda_results,
        "charts": charts,
        "report": report
    }


class QueryRequest(BaseModel):
    question: str

@app.post("/api/query/{run_id}")
async def ask_question(run_id: str, req: QueryRequest):
    """Ask a natural language question about the analysis."""
    state = RUNS.get(run_id)
    if not state:
        return {"error": "Run not found"}
        
    from analyst.query_agent import ask_question as run_query
    
    # Run the query using the existing state (avoids re-running previous agents)
    answer = run_query(state, req.question)
    return {"answer": answer}
