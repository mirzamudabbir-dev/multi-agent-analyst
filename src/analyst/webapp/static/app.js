document.addEventListener("DOMContentLoaded", () => {
    
    let currentRunId = null;
    let eventSource = null;

    const fileInput = document.getElementById("file-input");
    const fileNameDisplay = document.getElementById("file-name");
    const startBtn = document.getElementById("start-btn");
    const profileBtn = document.getElementById("profile-btn");
    const sysStatus = document.getElementById("sys-status");
    const logFeed = document.getElementById("log-feed");

    const feedSection = document.getElementById("feed");
    const insightsSection = document.getElementById("insights");
    const reportSection = document.getElementById("report");

    const queryInput = document.getElementById("query-input");
    const queryBtn = document.getElementById("query-btn");
    const queryResult = document.getElementById("query-result");

    fileInput.addEventListener("change", (e) => {
        if (e.target.files.length > 0) {
            const file = e.target.files[0];
            fileNameDisplay.textContent = file.name;
            startBtn.style.display = "inline-block";
            profileBtn.style.display = "inline-block";
            sysStatus.textContent = "READY";
            sysStatus.style.color = "var(--pure-white)";
        }
    });

    async function executePipeline(mode) {
        const file = fileInput.files[0];
        if (!file) return;

        startBtn.disabled = true;
        profileBtn.disabled = true;

        const formData = new FormData();
        formData.append("file", file);
        if (mode === "profile") formData.append("only", "ingestion,profiling");

        try {
            // Note: Our FastAPI currently hardcodes 'analyze fully'. 
            // For a production 'profile only' mode, we would pass ?mode=profile via URL search params to backend.
            // For this UI demo, we will just send it to upload normally and visualize the profile.
            const res = await fetch(`/api/upload?mode=${mode}`, {
                method: "POST",
                body: formData
            });
            const data = await res.json();
            
            if (res.ok) {
                currentRunId = data.run_id;
                sysStatus.textContent = "EXECUTING";
                sysStatus.style.color = "#55ff55"; 
                
                feedSection.style.display = "grid";
                logFeed.innerHTML = "";
                startSSEListener(mode);
            } else {
                throw new Error("Upload failed");
            }

        } catch (err) {
            sysStatus.textContent = "ERROR";
            sysStatus.style.color = "#ff5555";
            startBtn.disabled = false;
            profileBtn.disabled = false;
        }
    }

    startBtn.addEventListener("click", () => executePipeline("full"));
    profileBtn.addEventListener("click", () => executePipeline("profile"));

    queryBtn.addEventListener("click", async () => {
        const question = queryInput.value.trim();
        if (!question || !currentRunId) return;

        queryBtn.textContent = "Thinking...";
        queryBtn.disabled = true;
        queryResult.style.display = "block";
        queryResult.innerHTML = "<em style='color: #94A3B8;'>Interrogating data model...</em>";

        try {
            const res = await fetch(`/api/query/${currentRunId}`, {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({ question: question })
            });
            const data = await res.json();
            if (res.ok) {
                queryResult.innerHTML = `<strong>A:</strong> ${marked.parse(data.answer)}`;
            } else {
                queryResult.innerHTML = "<em>Error executing query.</em>";
            }
        } catch(e) {
            queryResult.innerHTML = "<em>Network error.</em>";
        }
        
        queryBtn.textContent = "Ask";
        queryBtn.disabled = false;
    });

    function startSSEListener(mode) {
        if (eventSource) eventSource.close();
        eventSource = new EventSource("/api/stream");
        
        eventSource.onmessage = (e) => {
            const data = JSON.parse(e.data);
            appendLog(data);

            if (data.is_done) {
                eventSource.close();
                sysStatus.textContent = "COMPLETE";
                fetchResults(mode);
            }
        };
    }

    function appendLog(log) {
        const div = document.createElement("div");
        div.className = "log-entry";
        let colorClass = "";
        if (log.severity === "ERROR") colorClass = "log-error";
        if (log.severity === "WARNING") colorClass = "log-warning";
        if (log.severity === "SUCCESS") colorClass = "log-success";

        const time = new Date().toLocaleTimeString([], {hour12: false});
        div.innerHTML = `<span style="opacity:0.5">[${time}]</span> <span style="color:#a855f7">[${log.agent.toUpperCase()}]</span> <span class="${colorClass}">${log.message}</span>`;
        logFeed.appendChild(div);
        logFeed.scrollTop = logFeed.scrollHeight;
    }

    async function fetchResults(mode) {
        if (!currentRunId) return;

        try {
            const res = await fetch(`/api/results/${currentRunId}`);
            const data = await res.json();

            insightsSection.style.display = "grid";
            if (mode !== "profile") {
                reportSection.style.display = "grid";
            }

            // Render Markdown
            if (data.report && mode !== "profile") {
                document.getElementById("markdown-content").innerHTML = marked.parse(data.report);
            } else {
                document.getElementById("markdown-content").innerHTML = "<p>No report generated in profile mode.</p>";
            }

            // Render Schema minimal table
            const schemaDiv = document.getElementById("schema-table");
            if (data.profile && data.profile.length > 0) {
                let html = "<div style='display:grid; grid-template-columns: 2fr 1fr 2fr; border-bottom:1px solid rgba(255,255,255,0.1); padding-bottom:12px; margin-bottom:12px; opacity:0.6; font-weight:bold;'><span>COLUMN</span><span>TYPE</span><span>DISTRIBUTION</span></div>";
                data.profile.forEach(p => {
                    html += `<div style='display:grid; grid-template-columns: 2fr 1fr 2fr; margin-bottom:12px; border-bottom:1px solid rgba(255,255,255,0.02); padding-bottom:6px;'>
                                <span style='color: white;'>${p.name}</span>
                                <span style='color: #94A3B8;'>${p.type}</span>
                                <span style='color: #E2E8F0; opacity:0.8;'>${p.unique} Unique &nbsp;|&nbsp; ${p.nulls} Nulls</span>
                             </div>`;
                });
                schemaDiv.innerHTML = html;
                
                // Draw native Plotly Chart!
                drawPlotlyCharts(data.profile);
            } else {
                schemaDiv.innerHTML = "No schema profile available.";
            }

        } catch (err) {
            console.error(err);
        }
    }
    
    function drawPlotlyCharts(profile) {
        // Build a beautiful monochrome bar chart of unique values per column
        const xData = profile.map(p => p.name);
        const yData = profile.map(p => p.unique);
        
        const trace = {
            x: xData,
            y: yData,
            type: 'bar',
            marker: {
                // Silver gradient emulation
                color: '#E2E8F0',
                line: { color: '#ffffff', width: 0 }
            }
        };
        
        const layout = {
            title: { text: "Cardinals (Unique Values) per Field", font: {family: "DM Serif Display", size: 24, color: "#fff"} },
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            font: { family: "Geist Mono, monospace", color: "#94A3B8" },
            margin: { t: 60, l: 40, r: 20, b: 80 },
            xaxis: { showgrid: false, linecolor: 'rgba(255,255,255,0.1)' },
            yaxis: { showgrid: true, gridcolor: 'rgba(255,255,255,0.05)', linecolor: 'rgba(255,255,255,0.1)' }
        };
        
        Plotly.newPlot('charts-container', [trace], layout, {displayModeBar: false, responsive: true});
    }
});
