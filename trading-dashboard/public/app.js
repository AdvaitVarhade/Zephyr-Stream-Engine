const ws = new WebSocket(`ws://${window.location.host}`);

const MAX_DATA_POINTS = 60;
const labels = Array.from({length: MAX_DATA_POINTS}, (_, i) => '');

// Common chart options
const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    scales: {
        x: { display: false },
        y: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' } }
    },
    plugins: {
        legend: { labels: { color: '#c9d1d9' } }
    }
};

// Throughput Chart
const ctxThroughput = document.getElementById('throughputChart').getContext('2d');
const throughputChart = new Chart(ctxThroughput, {
    type: 'line',
    data: {
        labels: [...labels],
        datasets: [
            {
                label: 'Legacy JSON',
                data: Array(MAX_DATA_POINTS).fill(0),
                borderColor: '#f85149',
                borderWidth: 2,
                tension: 0.1,
                pointRadius: 0
            },
            {
                label: 'Antigravity (Zero-Copy)',
                data: Array(MAX_DATA_POINTS).fill(0),
                borderColor: '#3fb950',
                borderWidth: 2,
                tension: 0.1,
                pointRadius: 0
            }
        ]
    },
    options: chartOptions
});

// Latency Chart
const ctxLatency = document.getElementById('latencyChart').getContext('2d');
const latencyChart = new Chart(ctxLatency, {
    type: 'line',
    data: {
        labels: [...labels],
        datasets: [
            {
                label: 'Legacy JSON p99 (µs)',
                data: Array(MAX_DATA_POINTS).fill(0),
                borderColor: '#f85149',
                borderWidth: 2,
                tension: 0.1,
                pointRadius: 0
            },
            {
                label: 'Antigravity p99 (µs)',
                data: Array(MAX_DATA_POINTS).fill(0),
                borderColor: '#3fb950',
                borderWidth: 2,
                tension: 0.1,
                pointRadius: 0
            }
        ]
    },
    options: chartOptions
});

// DOM Elements
const btnStart = document.getElementById('btnStart');
const btnStop = document.getElementById('btnStop');
const btnBurst = document.getElementById('btnBurst');
const statusLabel = document.getElementById('statusLabel');

const valThroughputBase = document.getElementById('valThroughputBase');
const valThroughputAnti = document.getElementById('valThroughputAnti');
const valLatencyBase = document.getElementById('valLatencyBase');
const valLatencyAnti = document.getElementById('valLatencyAnti');

// WebSocket Handlers
ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    
    if (data.type === 'status') {
        const isRunning = data.isRunning;
        btnStart.style.display = isRunning ? 'none' : 'inline-block';
        btnStop.style.display = isRunning ? 'inline-block' : 'none';
        btnBurst.disabled = !isRunning;
        
        statusLabel.textContent = isRunning ? 'RUNNING' : 'OFFLINE';
        statusLabel.style.color = isRunning ? '#3fb950' : '#8b949e';
    } 
    else if (data.type === 'telemetry') {
        // Update Chart Data
        throughputChart.data.datasets[0].data.shift();
        throughputChart.data.datasets[0].data.push(data.baseline.throughput);
        throughputChart.data.datasets[1].data.shift();
        throughputChart.data.datasets[1].data.push(data.antigravity.throughput);
        throughputChart.update();

        latencyChart.data.datasets[0].data.shift();
        latencyChart.data.datasets[0].data.push(data.baseline.p99);
        latencyChart.data.datasets[1].data.shift();
        latencyChart.data.datasets[1].data.push(data.antigravity.p99);
        latencyChart.update();

        // Update DOM stats
        valThroughputBase.textContent = data.baseline.throughput.toLocaleString();
        valThroughputAnti.textContent = data.antigravity.throughput.toLocaleString();
        valLatencyBase.textContent = data.baseline.p99.toFixed(2);
        valLatencyAnti.textContent = data.antigravity.p99.toFixed(2);
    }
};

// UI Listeners
btnStart.addEventListener('click', () => {
    ws.send(JSON.stringify({ action: 'START' }));
});

btnStop.addEventListener('click', () => {
    ws.send(JSON.stringify({ action: 'STOP' }));
});

btnBurst.addEventListener('click', () => {
    ws.send(JSON.stringify({ action: 'BURST' }));
});
