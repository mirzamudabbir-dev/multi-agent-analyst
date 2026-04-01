document.addEventListener("DOMContentLoaded", () => {
    let authToken = localStorage.getItem("analyst_token");

    // Utilities
    const checkAuthAndRedirect = () => {
        if (authToken && !window.location.pathname.includes("keygen.html")) {
            window.location.href = "./dashboard.html";
        }
    };

    if (window.location.pathname.includes("login.html") || window.location.pathname.includes("register.html") || window.location.pathname === "/") {
        checkAuthAndRedirect();
    }

    // Login Event
    const loginBtn = document.getElementById("login-btn");
    if (loginBtn) {
        loginBtn.onclick = async () => {
            const user = document.getElementById("login-user").value;
            const pass = document.getElementById("login-pass").value;
            const errorDiv = document.getElementById("login-error");

            try {
                loginBtn.textContent = "Authenticating...";
                const res = await fetch("https://multi-agent-analyst.onrender.com/api/login", {
                    method: "POST",
                    headers: {"Content-Type": "application/json"},
                    body: JSON.stringify({username: user, password: pass})
                });
                const data = await res.json();
                if (res.ok) {
                    authToken = data.token;
                    localStorage.setItem("analyst_token", authToken);
                    window.location.href = "./keygen.html"; // Enforce keygen on every login
                } else {
                    errorDiv.textContent = data.detail || "Login failed";
                    errorDiv.style.display = "block";
                    loginBtn.textContent = "Initialize Session";
                }
            } catch (e) {
                errorDiv.textContent = "Network error";
                errorDiv.style.display = "block";
                loginBtn.textContent = "Initialize Session";
            }
        };
    }

    // Register Event
    const registerBtn = document.getElementById("register-btn");
    if (registerBtn) {
        registerBtn.onclick = async () => {
            const user = document.getElementById("reg-user").value;
            const pass = document.getElementById("reg-pass").value;
            const errorDiv = document.getElementById("register-error");

            try {
                registerBtn.textContent = "Processing...";
                const res = await fetch("https://multi-agent-analyst.onrender.com/api/register", {
                    method: "POST",
                    headers: {"Content-Type": "application/json"},
                    body: JSON.stringify({username: user, password: pass})
                });
                const data = await res.json();
                if (res.ok) {
                    // Force them to login now
                    window.location.href = "./login.html?registered=true";
                } else {
                    errorDiv.textContent = data.detail || "Registration failed";
                    errorDiv.style.display = "block";
                    registerBtn.textContent = "Create Account";
                }
            } catch (e) {
                errorDiv.textContent = "Network error";
                errorDiv.style.display = "block";
                registerBtn.textContent = "Create Account";
            }
        };
    }

    // Registration Success Message on Login Page
    if (window.location.pathname.includes("login.html") && window.location.search.includes("registered=true")) {
        const loginError = document.getElementById("login-error");
        if (loginError) {
            loginError.textContent = "Account created successfully. Please log in.";
            loginError.style.display = "block";
            loginError.style.background = "rgba(85,255,85,0.1)";
            loginError.style.color = "#55ff55";
            loginError.style.borderColor = "rgba(85,255,85,0.3)";
        }
    }

    // Save API Key Event
    const saveKeyBtn = document.getElementById("save-key-btn");
    if (saveKeyBtn) {
        saveKeyBtn.onclick = async () => {
            const key = document.getElementById("api-key-input").value;
            const errorDiv = document.getElementById("keygen-error");

            try {
                saveKeyBtn.textContent = "Encrypting...";
                const res = await fetch("https://multi-agent-analyst.onrender.com/api/set-key", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        "Authorization": `Bearer ${authToken}`
                    },
                    body: JSON.stringify({api_key: key})
                });
                if (res.ok) {
                    window.location.href = "./dashboard.html";
                } else {
                    const data = await res.json();
                    errorDiv.textContent = data.detail || "Failed to save key";
                    errorDiv.style.display = "block";
                    saveKeyBtn.textContent = "Authorize System";
                }
            } catch (e) {
                errorDiv.textContent = "Network error";
                errorDiv.style.display = "block";
                saveKeyBtn.textContent = "Authorize System";
            }
        };
    }
});
