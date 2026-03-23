"""CLI entrypoint for Multi-Agent Analyst."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
import questionary
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
from rich.theme import Theme

from analyst.config import load_config
from analyst.orchestrator import LLMOrchestrator
from analyst.state import AnalysisState

# Import agents so they register themselves
import analyst.agents.ingestion  # noqa: F401
import analyst.agents.profiling  # noqa: F401
import analyst.agents.cleaning   # noqa: F401
import analyst.agents.eda        # noqa: F401
import analyst.agents.visualization  # noqa: F401
import analyst.agents.reporting  # noqa: F401
import analyst.agents.tableau_export  # noqa: F401
import analyst.reasoning.consolidated  # noqa: F401

custom_theme = Theme({
    "info": "#ff3399",         # Pink
    "warning": "#fbbc04",      # Yellow
    "error": "bold #ea4335",   # Red
    "success": "bold #00e5ff", # Cyan
    "highlight": "bold #8b3dff", # Purple
    "primary": "#1b66ec",      # Blue
    "secondary": "#e8eaed"     # Light Gray
})

console = Console(theme=custom_theme)

app = typer.Typer(
    name="analyst",
    help="[secondary]An elegant, AI-powered system to profile, clean, analyze, and visualize your datasets automatically.[/]",
    add_completion=False,
    rich_markup_mode="rich",
)

BANNER_TEXT = """\
██╗      ███╗   ███╗██╗   ██╗██╗  ████████╗██╗       █████╗  ██████╗ ███████╗███╗   ██╗████████╗
╚██╗     ████╗ ████║██║   ██║██║  ╚══██╔══╝██║      ██╔══██╗██╔════╝ ██╔════╝████╗  ██║╚══██╔══╝
 ╚██╗    ██╔████╔██║██║   ██║██║     ██║   ██║█████╗███████║██║  ███╗█████╗  ██╔██╗ ██║   ██║   
 ██╔╝    ██║╚██╔╝██║██║   ██║██║     ██║   ██║╚════╝██╔══██║██║   ██║██╔══╝  ██║╚██╗██║   ██║   
██╔╝     ██║ ╚═╝ ██║╚██████╔╝███████╗██║   ██║      ██║  ██║╚██████╔╝███████╗██║ ╚████║   ██║   
╚═╝      ╚═╝     ╚═╝ ╚═════╝ ╚══════╝╚═╝   ╚═╝      ╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═══╝   ╚═╝   
                                                                                                
         █████╗ ███╗   ██╗ █████╗ ██╗  ██╗   ██╗███████╗████████╗
        ██╔══██╗████╗  ██║██╔══██╗██║  ╚██╗ ██╔╝██╔════╝╚══██╔══╝
        ███████║██╔██╗ ██║███████║██║   ╚████╔╝ ███████╗   ██║   
        ██╔══██║██║╚██╗██║██╔══██║██║    ╚██╔╝  ╚════██║   ██║   
        ██║  ██║██║ ╚████║██║  ██║███████╗██║   ███████║   ██║   
        ╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝╚═╝   ╚══════╝   ╚═╝\
"""

def get_gradient_color(pct: float) -> str:
    """Returns a color hex between blue, purple, and pink based on percentage."""
    colors = [
        (0.0, (27, 102, 236)),    # Blue
        (0.5, (139, 61, 255)),    # Purple
        (0.9, (255, 51, 153)),    # Pink
        (1.0, (255, 102, 102)),   # Light Red
    ]
    for i in range(len(colors) - 1):
        if colors[i][0] <= pct <= colors[i+1][0]:
            c1 = colors[i]
            c2 = colors[i+1]
            local_pct = (pct - c1[0]) / (c2[0] - c1[0])
            r = int(c1[1][0] + (c2[1][0] - c1[1][0]) * local_pct)
            g = int(c1[1][1] + (c2[1][1] - c1[1][1]) * local_pct)
            b = int(c1[1][2] + (c2[1][2] - c1[1][2]) * local_pct)
            return f"#{r:02x}{g:02x}{b:02x}"
    return "#ff6666"

@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    """Main entry point. If no command is provided, display a welcome screen with features."""
    if ctx.invoked_subcommand is None:
        lines = BANNER_TEXT.splitlines()
        max_len = max(len(line) for line in lines) if lines else 1
        
        banner_text = Text()
        for line in lines:
            if not line.strip():
                banner_text.append("\n")
                continue
            for i, char in enumerate(line):
                # Only colorize block characters, not empty spaces to avoid background artifacts,
                # actually it's fine to stylize space, but rich may ignore it.
                color = get_gradient_color(i / max_len)
                banner_text.append(char, style=f"bold {color}")
            banner_text.append("\n")
            
        console.print(banner_text)
        
        tips = Text()
        tips.append("Tips for getting started:\n", style="secondary")
        tips.append("1. Ask questions, edit files, or run commands.\n", style="secondary")
        tips.append("2. Be specific for the best results.\n", style="secondary")
        tips.append("3. ", style="secondary")
        tips.append("analyst --help", style="info")
        tips.append(" for more information.\n", style="secondary")
        
        console.print(tips)
        interactive_loop()

def interactive_loop() -> None:
    """Provides a persistent terminal REPL for interactive data analysis."""
    style = questionary.Style([
        ('qmark', 'fg:#ff3399 bold'),       # Pink
        ('question', 'bold'),
        ('answer', 'fg:#00e5ff bold'),      # Cyan
        ('pointer', 'fg:#8b3dff bold'),     # Purple
        ('highlighted', 'fg:#8b3dff bold'), # Purple
        ('selected', 'fg:#00e5ff'),         # Cyan
        ('separator', 'fg:#e8eaed'),
        ('instruction', 'fg:#e8eaed dim'),
    ])

    console.print("\n[secondary]Type [bold highlight]/[/] to open the commands menu, or [bold cyan]exit[/] to quit.[/]\n")

    while True:
        try:
            user_input = questionary.text("> ", style=style).ask()
            
            if user_input is None:
                break
                
            user_input = user_input.strip()
            
            if user_input == "/":
                command = questionary.select(
                    "Select a command to execute:",
                    choices=[
                        questionary.Choice("Run Analysis Pipeline", value="run"),
                        questionary.Choice("Profile Dataset", value="profile"),
                        questionary.Choice("Natural Language Query", value="query"),
                        questionary.Choice("List Agents", value="agents"),
                        questionary.Choice("Exit REPL", value="exit")
                    ],
                    style=style,
                    instruction="(Use arrow keys)"
                ).ask()
                
                if command == "exit" or command is None:
                    break
                elif command == "run":
                    file_path = questionary.text("Enter dataset file path: ", style=style).ask()
                    if file_path:
                        run(file=Path(file_path), output=Path("output"), config=None, skip=None, only=None)
                elif command == "profile":
                    file_path = questionary.text("Enter dataset file path: ", style=style).ask()
                    if file_path:
                        profile(file=Path(file_path), output=Path("output"))
                elif command == "query":
                    file_path = questionary.text("Enter dataset file path: ", style=style).ask()
                    question = questionary.text("Enter your query: ", style=style).ask()
                    if file_path and question:
                        query(file=Path(file_path), question=question, output=Path("output"))
                elif command == "agents":
                    agents()
                    
            elif user_input.lower() in ["exit", "quit"]:
                break
            elif user_input:
                console.print("[warning]Please use the [bold highlight]/[/] menu to execute commands.[/warning]")
                
        except KeyboardInterrupt:
            console.print("\n[secondary]Exiting...[/secondary]")
            break
        except Exception as e:
            console.print(f"[error]Error:[/] {e}")

def print_banner(title: str, subtitle: str = "") -> None:
    text = Text()
    text.append("✦  ", style="info")
    text.append(f"{title}", style="bold white")
    text.append("  ✦", style="info")
    if subtitle:
        text.append(f"\n\n{subtitle}", style="secondary")
    console.print(Panel(text, box=box.ROUNDED, border_style="primary", padding=(1, 2), expand=False))

@app.command()
def run(
    file: Path = typer.Argument(
        ...,
        help="Path to the data file (CSV, XLSX, JSON, Parquet)",
        exists=True,
        readable=True,
    ),
    output: Path = typer.Option(
        Path("output"),
        "--output", "-o",
        help="Output directory for reports and charts",
    ),
    config: Optional[Path] = typer.Option(
        None,
        "--config", "-c",
        help="Path to a TOML config file",
    ),
    smart: bool = typer.Option(
        False,
        "--smart",
        help="Use the LLM Orchestrator for adaptive re-planning (uses more tokens)",
    ),
) -> None:
    """
    [bold cyan]Run the full analysis pipeline on a data file.[/bold cyan]

    Default: deterministic pipeline (zero routing tokens).
    Use --smart for LLM-driven adaptive planning.
    """
    from analyst.digest import load_cache

    print_banner("Multi-Agent Analysis Pipeline", "Initializing agents and preparing for run...")

    # Check cache first
    cached_state = load_cache(file)
    if cached_state:
        console.print("[dim]♻ Cache hit — skipping ingestion/profiling/eda/cleaning[/dim]")
        cached_state.output_dir = output
        state = cached_state
        skip_up_to = "eda"  # skip Phase 1 agents
    else:
        cfg = load_config(config)
        state = AnalysisState(file_path=file, output_dir=output, config=cfg)
        skip_up_to = None

    try:
        if smart:
            goal = "Analyze this dataset fully."
            orchestrator = LLMOrchestrator(state=state, goal=goal)
            orchestrator.run()
        else:
            from analyst.orchestrator import DeterministicPipeline
            pipeline = DeterministicPipeline(state=state, skip_up_to=skip_up_to)
            pipeline.run()
        console.print(Panel("[success]Pipeline completed successfully! ✨[/success]", box=box.ROUNDED, border_style="info"))
    except Exception as e:
        console.print(Panel(f"[error]Pipeline Error:[/] {e}", box=box.HEAVY, border_style="red"))


@app.command()
def agents() -> None:
    """[bold info]List all available agents and their descriptions.[/bold info]"""
    from analyst.registry import get_all_agents
    
    print_banner("Agent Registry", "Listing all available specialized AI agents.")

    table = Table(
        title="[bold]Available Agents[/bold]", 
        title_style="highlight",
        box=box.ROUNDED,
        border_style="primary",
        header_style="highlight",
        show_lines=True
    )
    table.add_column("Agent Name", style="bold cyan", no_wrap=True)
    table.add_column("Description", style="info")

    for name, cls in get_all_agents().items():
        table.add_row(f"🤖 {name}", cls.description)

    console.print(table)


@app.command()
def profile(
    file: Path = typer.Argument(..., help="Path to the data file", exists=True),
    output: Path = typer.Option(Path("output"), "--output", "-o"),
) -> None:
    """[bold info]Quick profile[/bold info] — runs only ingestion + profiling (0 LLM tokens)."""
    from analyst.digest import load_cache, save_cache
    from analyst.agents.ingestion import IngestionAgent
    from analyst.agents.profiling import ProfilerAgent

    print_banner("Quick Data Profiling", "Running ingestion and automated data profiling...")

    # Check cache first
    cached_state = load_cache(file)
    if cached_state:
        console.print("[dim]♻ Cache hit — loaded profile from disk[/dim]")
        state = cached_state
    else:
        cfg = load_config(None)
        state = AnalysisState(file_path=file, output_dir=output, config=cfg)
        try:
            state, _ = IngestionAgent().execute(state)
            state, _ = ProfilerAgent().execute(state)
            save_cache(state)
        except Exception as e:
            console.print(Panel(f"[error]Pipeline Error:[/] {e}", box=box.HEAVY, border_style="red"))
            return

    if state.profile:
        console.print("\n[success]Profiling complete! Here are the results:[/success]\n")
        table = Table(
            title="[bold]Column Profiles[/bold]", 
            title_style="highlight",
            box=box.ROUNDED, 
            border_style="primary",
            header_style="highlight"
        )
        table.add_column("Column", style="bold white")
        table.add_column("Type", style="cyan")
        table.add_column("Nulls", justify="right", style="yellow")
        table.add_column("Unique", justify="right", style="cyan")
        table.add_column("Mean", justify="right", style="magenta")
        table.add_column("Min", justify="right", style="magenta")
        table.add_column("Max", justify="right", style="magenta")

        for p in state.profile:
            table.add_row(
                p.name,
                p.dtype,
                str(p.null_count),
                str(p.unique_count),
                str(p.mean) if p.mean is not None else "—",
                str(p.min) if p.min is not None else "—",
                str(p.max) if p.max is not None else "—",
            )
        console.print(table)


@app.command()
def query(
    file: Path = typer.Argument(..., help="Path to the data file", exists=True),
    question: str = typer.Argument(..., help="Your question in natural language"),
    output: Path = typer.Option(Path("output"), "--output", "-o"),
) -> None:
    """[bold info]Ask a natural language question about the data (auto-loads cache).[/bold info]"""
    from analyst.query_agent import ask_question
    from analyst.digest import load_cache, save_cache

    print_banner("Data Query Interface", "Preparing to answer your question...")

    # Auto-load cache — skip all agents, go straight to LLM query
    cached_state = load_cache(file)
    if cached_state:
        console.print("[dim]♻ Cache hit — skipping all agents, going straight to LLM[/dim]")
        state = cached_state
    else:
        # No cache — run local Python agents only (ingestion + profiling + eda)
        from analyst.agents.ingestion import IngestionAgent
        from analyst.agents.profiling import ProfilerAgent
        from analyst.agents.eda import EDAAgent

        console.print("[dim]No cache found — running quick profile first...[/dim]")
        cfg = load_config(None)
        state = AnalysisState(file_path=file, output_dir=output, config=cfg)
        try:
            state, _ = IngestionAgent().execute(state)
            state, _ = ProfilerAgent().execute(state)
            state, _ = EDAAgent().execute(state)
            save_cache(state)
        except Exception as e:
            console.print(Panel(f"[error]Pipeline Error:[/] {e}", box=box.HEAVY, border_style="red"))
            return

    with console.status("[bold info]🤔 Analyzing data to answer your question...[/bold info]", spinner="dots", spinner_style="highlight"):
        answer = ask_question(state, question)

    result_panel = Panel(
        f"[bold cyan]Question:[/bold cyan] {question}\n\n[bold highlight]Answer:[/bold highlight]\n{answer}",
        title="[bold]Query Result[/bold]",
        title_align="left",
        border_style="highlight",
        box=box.ROUNDED,
        padding=(1, 2)
    )
    console.print("\n")
    console.print(result_panel)


@app.command()
def serve(
    port: int = typer.Option(8501, "--port", "-p", help="Port to run the web dashboard on"),
) -> None:
    """[bold info]Launch the Web Dashboard[/bold info] — open the UI in your browser."""
    print_banner("Web Dashboard", f"Starting server on http://localhost:{port}")
    import uvicorn
    uvicorn.run("analyst.webapp.api:app", host="0.0.0.0", port=port, reload=False)


if __name__ == "__main__":
    app()
