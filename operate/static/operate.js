(function() {
    const POLL_MS = 5000;
    const HISTORY_WINDOW_MS = 10 * 60 * 1000;
    const state = {
        lastStatus: null,
        lastCpu: null,
        statusOk: false,
        cpuOk: false,
        history: [],
        chart: null,
    };

    function getPathValue(source, path) {
        return path.split(".").reduce(function(value, key) {
            return value && Object.prototype.hasOwnProperty.call(value, key) ? value[key] : undefined;
        }, source);
    }

    function isAvailable(value) {
        return value !== undefined && value !== null && value !== "";
    }

    function numberValue(value) {
        if (value === undefined || value === null || value === "") return null;
        const numeric = Number(value);
        return Number.isFinite(numeric) ? numeric : null;
    }

    function formatValue(value, format) {
        if (!isAvailable(value)) return "Not available";
        const numeric = numberValue(value);
        if (format === "percent") {
            return numeric === null ? "Not available" : (numeric * 100).toFixed(2) + "%";
        }
        if (format === "fraction-percent") {
            return numeric === null ? "Not available" : (numeric * 100).toFixed(0) + "%";
        }
        if (format === "percent-value") {
            return numeric === null ? "Not available" : numeric.toFixed(2) + "%";
        }
        if (format === "energy") {
            return numeric === null ? "Not available" : numeric.toFixed(6) + " kWh";
        }
        if (format === "carbon") {
            return numeric === null ? "Not available" : numeric.toFixed(2) + " gCO2";
        }
        if (format === "boolean") {
            return value === true || value === "true" ? "Yes" : "No";
        }
        if (format === "list") {
            return Array.isArray(value) && value.length ? value.join(", ") : "Not available";
        }
        if (format === "action") {
            return String(value).replace(/_/g, " ").toUpperCase();
        }
        return String(value);
    }

    function updateFields(data) {
        document.querySelectorAll("[data-field]").forEach(function(element) {
            const value = getPathValue(data, element.dataset.field);
            element.textContent = formatValue(value, element.dataset.format);
        });
        const reason = getPathValue(data, "jobs.reason");
        document.getElementById("jobReason").textContent = isAvailable(reason) ? reason : "Waiting for decision reason";
    }

    function setEmptyState(id, visible) {
        const element = document.getElementById(id);
        if (element) element.classList.toggle("visible", visible);
    }

    function updateEmptyStates(data) {
        setEmptyState("predictionEmpty", !isAvailable(getPathValue(data, "latest_prediction.cpu_percent")));
        setEmptyState("jobsEmpty", !isAvailable(getPathValue(data, "jobs.total_jobs")));
        setEmptyState("carbonEmpty", !isAvailable(getPathValue(data, "carbon.estimated_carbon_gco2")));
        setEmptyState("kubernetesEmpty", !isAvailable(getPathValue(data, "kubernetes.status")));
    }

    function setConnectionState() {
        const dot = document.getElementById("connectionDot");
        const text = document.getElementById("connectionText");
        dot.classList.remove("ok", "warning", "error");
        if (state.statusOk && state.cpuOk) {
            dot.classList.add("ok");
            text.textContent = "Live";
        } else if (state.statusOk || state.cpuOk) {
            dot.classList.add("warning");
            text.textContent = "Partial connection";
        } else {
            dot.classList.add("error");
            text.textContent = "Disconnected";
        }
    }

    function updateLastUpdated() {
        const stamp = document.getElementById("lastUpdated");
        if (state.lastStatus || state.lastCpu) {
            stamp.textContent = "Last updated: " + new Date().toLocaleTimeString();
        }
    }

    function setupTheme() {
        document.querySelectorAll("[data-theme-choice]").forEach(function(button) {
            button.addEventListener("click", function() {
                const theme = button.dataset.themeChoice === "dark" ? "dark" : "light";
                document.documentElement.dataset.theme = theme;
                try { localStorage.setItem("green-devops-theme", theme); } catch (error) {}
                updateChartTheme();
            });
        });
    }

    function chartColors() {
        const dark = document.documentElement.dataset.theme === "dark";
        return {
            grid: dark ? "rgba(226,232,240,.08)" : "rgba(15,23,42,.08)",
            text: dark ? "#b7c4d4" : "#475569",
            actual: "#0284c7",
            predicted: "#059669",
        };
    }

    function setupChart() {
        const colors = chartColors();
        const ctx = document.getElementById("cpuPredictionChart");
        state.chart = new Chart(ctx, {
            type: "line",
            data: {
                labels: [],
                datasets: [
                    {
                        label: "Current / Actual CPU %",
                        data: [],
                        borderColor: colors.actual,
                        backgroundColor: colors.actual + "18",
                        tension: .32,
                        spanGaps: true,
                    },
                    {
                        label: "Predicted CPU %",
                        data: [],
                        borderColor: colors.predicted,
                        backgroundColor: colors.predicted + "18",
                        tension: .32,
                        spanGaps: true,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: { intersect: false, mode: "index" },
                plugins: {
                    legend: { labels: { color: colors.text, font: { weight: "800" } } },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const value = context.parsed.y;
                                return context.dataset.label + ": " + (value === null ? "Not available" : Number(value).toFixed(2) + "%");
                            },
                        },
                    },
                },
                scales: {
                    x: { ticks: { color: colors.text, maxTicksLimit: 8 }, grid: { color: colors.grid } },
                    y: { beginAtZero: true, ticks: { color: colors.text, callback: function(value) { return value + "%"; } }, grid: { color: colors.grid } },
                },
            },
        });
    }

    function updateChartTheme() {
        if (!state.chart) return;
        const colors = chartColors();
        state.chart.data.datasets[0].borderColor = colors.actual;
        state.chart.data.datasets[0].backgroundColor = colors.actual + "18";
        state.chart.data.datasets[1].borderColor = colors.predicted;
        state.chart.data.datasets[1].backgroundColor = colors.predicted + "18";
        state.chart.options.plugins.legend.labels.color = colors.text;
        state.chart.options.scales.x.ticks.color = colors.text;
        state.chart.options.scales.y.ticks.color = colors.text;
        state.chart.options.scales.x.grid.color = colors.grid;
        state.chart.options.scales.y.grid.color = colors.grid;
        state.chart.update("none");
    }

    function addHistoryPoint() {
        const predicted = state.lastStatus ? numberValue(getPathValue(state.lastStatus, "latest_prediction.cpu_percent")) : null;
        const actual = state.lastCpu && state.lastCpu.available ? numberValue(state.lastCpu.cpu_percent) : null;
        if (predicted === null && actual === null) return;
        const now = Date.now();
        state.history.push({ time: now, label: new Date(now).toLocaleTimeString(), actual: actual, predicted: predicted });
        state.history = state.history.filter(function(point) { return now - point.time <= HISTORY_WINDOW_MS; });
        state.chart.data.labels = state.history.map(function(point) { return point.label; });
        state.chart.data.datasets[0].data = state.history.map(function(point) { return point.actual; });
        state.chart.data.datasets[1].data = state.history.map(function(point) { return point.predicted; });
        state.chart.update("none");
    }

    function updateCpuAvailability() {
        const element = document.getElementById("cpuAvailability");
        if (state.lastCpu && state.lastCpu.available) {
            element.textContent = "Current CPU: " + formatValue(state.lastCpu.cpu_percent, "percent-value");
        } else {
            element.textContent = "Current CPU unavailable";
        }
    }

    async function pollStatus() {
        try {
            const response = await fetch("/api/operate/status", { cache: "no-store" });
            const payload = await response.json();
            if (!response.ok || !payload.available || !payload.data) throw new Error(payload.error || "Operation API unavailable");
            state.lastStatus = payload.data;
            state.statusOk = true;
            updateFields(state.lastStatus);
            updateEmptyStates(state.lastStatus);
        } catch (error) {
            state.statusOk = false;
        }
    }

    async function pollCpu() {
        try {
            const response = await fetch("/api/operate/cpu", { cache: "no-store" });
            const payload = await response.json();
            if (!response.ok) throw new Error(payload.error || "Prometheus unavailable");
            state.lastCpu = payload;
            state.cpuOk = Boolean(payload.available);
        } catch (error) {
            state.cpuOk = false;
        }
        updateCpuAvailability();
    }

    async function pollAll() {
        await Promise.all([pollStatus(), pollCpu()]);
        addHistoryPoint();
        setConnectionState();
        updateLastUpdated();
        if (window.lucide) lucide.createIcons();
    }

    document.addEventListener("DOMContentLoaded", function() {
        if (window.lucide) lucide.createIcons();
        setupTheme();
        setupChart();
        pollAll();
        setInterval(pollAll, POLL_MS);
    });
})();
