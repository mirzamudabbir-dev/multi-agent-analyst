document.addEventListener("DOMContentLoaded", () => {
    
    let currentRunId = null;
    let eventSource = null;
    let authToken = localStorage.getItem("analyst_token");

    if (!authToken) {
        window.location.href = "./login.html";
        return;
    }

    const getHeaders = () => ({
        "Authorization": `Bearer ${authToken}`
    });

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
    const downloadPdfBtn = document.getElementById("download-pdf-btn");

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
            const res = await fetch(`https://multi-agent-analyst.onrender.com/api/upload?mode=${mode}`, {
                method: "POST",
                headers: getHeaders(),
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
                if (res.status === 400 && data.detail.includes("API Key")) {
                    window.location.href = "./keygen.html";
                }
                throw new Error(data.detail || "Upload failed");
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
        queryResult.innerHTML = "<em style='color: #94A3B8; font-size: 12px; font-family: var(--font-mono);'>Interrogating data model...</em>";

        try {
            const res = await fetch(`https://multi-agent-analyst.onrender.com/api/query/${currentRunId}`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    ...getHeaders()
                },
                body: JSON.stringify({ question: question })
            });
            const data = await res.json();
            if (res.ok) {
                queryResult.style.display = "block";
                queryResult.style.padding = "2rem";
                queryResult.style.marginTop = "2rem";
                queryResult.style.borderRadius = "12px";
                queryResult.style.border = "1px solid rgba(255,255,255,0.05)";
                queryResult.style.borderLeft = "4px solid rgba(139, 61, 255, 0.8)";
                queryResult.style.background = "rgba(0,0,0,0.4)";
                
                queryResult.innerHTML = `
                  <div style="color: rgba(139, 61, 255, 0.8); font-size: 11px; letter-spacing: 2px; text-transform: uppercase; font-weight: bold; margin-bottom: 1rem;">
                    System Response
                  </div>
                  <div style="color: var(--silver-text); font-size: 15px; line-height: 1.8; font-family: 'Inter', sans-serif;">
                    ${marked.parse(data.answer)}
                  </div>
                `;
            } else {
                queryResult.innerHTML = "<em>Error executing query.</em>";
            }
        } catch(e) {
            queryResult.innerHTML = "<em>Network error.</em>";
        }
        
        queryBtn.textContent = "Ask";
        queryBtn.disabled = false;
    });

    downloadPdfBtn.addEventListener("click", async () => {
        if (!currentRunId) return;
        downloadPdfBtn.textContent = "Generating...";
        downloadPdfBtn.disabled = true;

        try {
            const res = await fetch(`https://multi-agent-analyst.onrender.com/api/download-pdf/${currentRunId}`, {
                headers: getHeaders()
            });
            if (res.ok) {
                const blob = await res.blob();
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url;
                a.download = `${currentRunId}_report.pdf`;
                document.body.appendChild(a);
                a.click();
                window.URL.revokeObjectURL(url);
            }
        } catch(e) {
            console.error("PDF Fail", e);
        }

        setTimeout(() => {
            downloadPdfBtn.textContent = "\u2193 Download PDF";
            downloadPdfBtn.disabled = false;
        }, 2000);
    });

    function startSSEListener(mode) {
        if (eventSource) eventSource.close();
        // SSE doesn't support headers natively, so we pass token in query param
        eventSource = new EventSource(`https://multi-agent-analyst.onrender.com/api/stream?token=${authToken}`);
        
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
        if (logFeed) {
            logFeed.appendChild(div);
            logFeed.scrollTop = logFeed.scrollHeight;
        }
    }

    async function fetchResults(mode) {
        if (!currentRunId) return;

        try {
            const res = await fetch(`https://multi-agent-analyst.onrender.com/api/results/${currentRunId}`, {
                headers: getHeaders()
            });
            const data = await res.json();

            insightsSection.style.display = "grid";
            if (mode !== "profile") {
                reportSection.style.display = "grid";
                downloadPdfBtn.style.display = "inline-block";
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
                let html = "<div class='data-grid-row data-grid-header'><span>COLUMN</span><span>TYPE</span><span>SPECIFICS</span></div>";
                data.profile.forEach(p => {
                    html += `<div class='data-grid-row'>
                                <span style='color: white;'>${p.name}</span>
                                <span style='color: #94A3B8;'>${p.dtype}</span>
                                <span style='color: #E2E8F0; opacity:0.7; font-size:12px;'>${p.unique_count} Unique &nbsp;|&nbsp; ${p.null_count} Nulls</span>
                             </div>`;
                });
                schemaDiv.innerHTML = html;
                
                // Draw native Plotly Chart (Basic Cardinality)
                drawPlotlyCharts(data.profile);

                // Add Static Matplotlib Charts if they exist
                if (data.charts && data.charts.length > 0) {
                    const staticWrapper = document.getElementById("static-charts-wrapper");
                    const staticContainer = document.getElementById("static-charts-container");
                    
                    staticWrapper.style.display = "block";
                    staticContainer.innerHTML = ""; // Clear old charts
                    
                    data.charts.forEach(chartName => {
                        const img = document.createElement("img");
                        img.src = `https://multi-agent-analyst.onrender.com/output/${currentRunId}/charts/${chartName}`;
                        img.style.width = "100%";
                        img.style.height = "auto";
                        img.style.borderRadius = "8px";
                        img.style.border = "1px solid rgba(255,255,255,0.1)";
                        img.style.objectFit = "contain";
                        staticContainer.appendChild(img);
                    });
                }
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
        const yData = profile.map(p => p.unique_count);
        
        const trace = {
            x: xData,
            y: yData,
            type: 'bar',
            marker: {
                color: '#8b3dff',
                line: { color: '#ffffff', width: 0.5 }
            }
        };
        
        const layout = {
            title: { text: "Cardinality (Unique Values) per Field", font: {family: "DM Serif Display", size: 24, color: "#fff"} },
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            font: { family: "Geist Mono, monospace", color: "#94A3B8" },
            margin: { t: 60, l: 40, r: 20, b: 80 },
            xaxis: { showgrid: false, linecolor: 'rgba(255,255,255,0.1)' },
            yaxis: { showgrid: true, gridcolor: 'rgba(255,255,255,0.05)', linecolor: 'rgba(255,255,255,0.1)' }
        };
        
        Plotly.newPlot('charts-container', [trace], layout, {displayModeBar: false, responsive: true});
    }

    // Terminate Session Event
    const logoutBtn = document.getElementById("logout-btn");
    if (logoutBtn) {
        logoutBtn.addEventListener("click", async (e) => {
            e.preventDefault();
            try {
                await fetch("/api/logout", {
                    method: "POST",
                    headers: getHeaders()
                });
            } catch (err) {
                console.error("Logout error", err);
            }
            localStorage.removeItem("analyst_token");
            window.location.href = "./login.html";
        });
    }

});
