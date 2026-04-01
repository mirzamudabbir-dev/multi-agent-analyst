import os
import shutil
import asyncio
from pathlib import Path
from tempfile import NamedTemporaryFile

from fastapi import FastAPI, UploadFile, File, BackgroundTasks, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from analyst.config import load_config
from analyst.orchestrator import LLMOrchestrator
from analyst.state import AnalysisState
from analyst.webapp.sse import SSEStreamer
from analyst.webapp.auth import auth_manager

security = HTTPBearer()

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    token = credentials.credentials
    user = auth_manager.decode_token(token)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user

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
app.mount("/output", StaticFiles(directory=Path("output")), name="output")

from fastapi.responses import RedirectResponse
@app.get("/")
def root():
    return RedirectResponse(url="/app/")


class AuthRequest(BaseModel):
    username: str
    password: str

class KeyRequest(BaseModel):
    api_key: str

@app.post("/api/register")
async def register(req: AuthRequest):
    if auth_manager.register_user(req.username, req.password):
        return {"message": "User registered successfully"}
    raise HTTPException(status_code=400, detail="Username already exists")

@app.post("/api/login")
async def login(req: AuthRequest):
    token = auth_manager.authenticate_user(req.username, req.password)
    if token:
        # Enforce fresh API key on every login
        auth_manager.set_user_api_key(req.username, None)
        return {"token": token}
    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.post("/api/logout")
async def logout(user: str = Depends(get_current_user)):
    """Clear session data and destroy the user's API key."""
    auth_manager.set_user_api_key(user, None)
    return {"message": "Logged out successfully"}

@app.post("/api/set-key")
async def set_key(req: KeyRequest, user: str = Depends(get_current_user)):
    auth_manager.set_user_api_key(user, req.api_key)
    return {"message": "API Key saved and encrypted"}

@app.post("/api/upload")
async def upload_file(
    background_tasks: BackgroundTasks, 
    file: UploadFile = File(...), 
    mode: str = "full",
    user: str = Depends(get_current_user)
):
    """Accept a file, save it to temp, and kick off the pipeline."""
    # Get user's API key
    api_key = auth_manager.get_user_api_key(user)
    if not api_key:
        raise HTTPException(status_code=400, detail="No API Key configured. Please set your Gemini key first.")

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
                state = AnalysisState(file_path=file_path, output_dir=output_dir, config=cfg, api_key=api_key)
                skip_up_to = None

            RUNS[run_id] = state
            
            if mode == "profile":
                if not cached_state:
                    from analyst.agents.ingestion import IngestionAgent
                    from analyst.agents.profiling import ProfilingAgent
                    
                    IngestionAgent().execute(state)
                    ProfilingAgent().execute(state)
                    
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
async def stream_progress(token: str = None):
    """Stream live events from the LLM agents to the browser."""
    if not token or not auth_manager.decode_token(token):
        raise HTTPException(status_code=401, detail="Invalid token")
    
    streamer = SSEStreamer()
    return StreamingResponse(streamer.sse_generator(), media_type="text/event-stream")


@app.get("/api/results/{run_id}")
async def get_results(run_id: str, user: str = Depends(get_current_user)):
    """Poll for final results including profile, eda, charts, report."""
    state = RUNS.get(run_id)
    if not state:
        return {"error": "Run not found or not started yet"}
        
    charts = [p.name for p in state.visualizations]
    report = state.report_path.read_text() if state.report_path and state.report_path.exists() else None
    
    return {
        "profile": [
            {
                "name": p.name,
                "dtype": str(p.dtype),
                "null_count": p.null_count,
                "null_pct": p.null_pct,
                "unique_count": p.unique_count,
                "mean": p.mean,
                "median": p.median,
                "std": p.std,
                "min": p.min,
                "max": p.max,
                "top_values": p.top_values,
            }
            for p in state.profile
        ],
        "eda_results": state.eda_results,
        "charts": charts,
        "report": report
    }


class QueryRequest(BaseModel):
    question: str

# Store Q&A history for PDF export
QA_HISTORY: dict[str, list[dict]] = {}

@app.post("/api/query/{run_id}")
async def ask_question(run_id: str, req: QueryRequest, user: str = Depends(get_current_user)):
    """Ask a natural language question about the analysis."""
    state = RUNS.get(run_id)
    if not state:
        return {"error": "Run not found"}
        
    from analyst.query_agent import ask_question as run_query
    
    # Run the query using the existing state (avoids re-running previous agents)
    answer = run_query(state, req.question)

    # Store Q&A for PDF export
    if run_id not in QA_HISTORY:
        QA_HISTORY[run_id] = []
    QA_HISTORY[run_id].append({"question": req.question, "answer": answer})

    return {"answer": answer}


@app.get("/api/download-pdf/{run_id}")
async def download_pdf(run_id: str, user: str = Depends(get_current_user)):
    """Generate and download a PDF report of the full analysis."""
    state = RUNS.get(run_id)
    if not state:
        return {"error": "Run not found"}

    from analyst.webapp.pdf_export import generate_pdf

    qa_history = QA_HISTORY.get(run_id, [])
    pdf_bytes = generate_pdf(state, qa_history=qa_history)

    filename = state.file_path.stem if state.file_path else "analysis"

    return StreamingResponse(
        iter([bytes(pdf_bytes)]),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}_report.pdf"'},
    )

