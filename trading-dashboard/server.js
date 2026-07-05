const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const path = require('path');
const flatbuffers = require('flatbuffers');
const { Event } = require('./event.js');

// Import our custom N-API wrapper (Antigravity Pipeline)
const { NapiPipeline } = require('./node-ffi/index.js');

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

let isRunning = false;
let antigravityPipeline = new NapiPipeline();
let baselineWindow = [];

const TARGET_PORT = 3000;

// Telemetry state
let telemetry = {
    baseline: { latencySum: 0n, processed: 0, p50: 0, p99: 0 },
    antigravity: { latencySum: 0n, processed: 0, p50: 0, p99: 0 },
};
let broadcastInterval = null;

wss.on('connection', (ws) => {
    ws.send(JSON.stringify({ type: 'status', isRunning }));
    
    ws.on('message', (msg) => {
        const cmd = JSON.parse(msg);
        if (cmd.action === 'START') {
            isRunning = true;
            ws.send(JSON.stringify({ type: 'status', isRunning }));
            startGenerator();
        } else if (cmd.action === 'STOP') {
            isRunning = false;
            ws.send(JSON.stringify({ type: 'status', isRunning }));
        } else if (cmd.action === 'BURST') {
            injectBurst();
        }
    });
});

// Resets telemetry window
function resetTelemetry() {
    telemetry.baseline = { latencySum: 0n, processed: 0, p50: 0, p99: 0, latencies: [] };
    telemetry.antigravity = { latencySum: 0n, processed: 0, p50: 0, p99: 0, latencies: [] };
}

resetTelemetry();

// Start broadcast loop
setInterval(() => {
    if (!isRunning && telemetry.baseline.processed === 0) return;
    
    // Poll native Rust telemetry for the Antigravity path
    const nativeTel = antigravityPipeline.pollTelemetry();
    telemetry.antigravity.processed += nativeTel.processed;
    for (let i = 0; i < nativeTel.latencies.length; i++) {
        telemetry.antigravity.latencies.push(nativeTel.latencies[i] / 1000); // ns to us
    }

    const calculatePercentileJS = (arr, p) => {
        if (arr.length === 0) return 0;
        arr.sort((a, b) => Number(a - b));
        const index = Math.floor((p / 100) * arr.length);
        return Number(arr[index]) / 1000; // Return microseconds
    };

    const calculatePercentileNative = (arr, p) => {
        if (arr.length === 0) return 0;
        arr.sort((a, b) => a - b);
        const index = Math.floor((p / 100) * arr.length);
        return arr[index]; // Already microseconds
    };

    const baselineP50 = calculatePercentileJS(telemetry.baseline.latencies, 50);
    const baselineP99 = calculatePercentileJS(telemetry.baseline.latencies, 99);
    const antiP50 = calculatePercentileNative(telemetry.antigravity.latencies, 50);
    const antiP99 = calculatePercentileNative(telemetry.antigravity.latencies, 99);

    const payload = {
        type: 'telemetry',
        baseline: {
            throughput: telemetry.baseline.processed,
            p50: baselineP50,
            p99: baselineP99
        },
        antigravity: {
            throughput: telemetry.antigravity.processed,
            p50: antiP50,
            p99: antiP99
        }
    };
    
    wss.clients.forEach(c => c.send(JSON.stringify(payload)));
    resetTelemetry();
}, 1000);

// --- PIPELINE IMPLEMENTATIONS ---

// 1. Legacy Baseline (JSON Strings, Object Allocation, Array shift)
function processLegacy(jsonString) {
    const start = process.hrtime.bigint();
    
    // Deliberate heavy CPU path: parse JSON
    const obj = JSON.parse(jsonString);
    
    // Push to sliding window
    baselineWindow.push(obj);
    if (baselineWindow.length > 10) {
        baselineWindow.shift();
    }
    
    // Evaluate rule
    let matchCount = 0;
    for (let i = 0; i < baselineWindow.length; i++) {
        const item = baselineWindow[i];
        if (item.symbol === 'AAPL' && item.price > 150) {
            matchCount++;
        }
    }
    
    if (matchCount >= 5) {
        // Alert threshold met
    }

    const end = process.hrtime.bigint();
    telemetry.baseline.processed++;
    telemetry.baseline.latencies.push(end - start);
}

// 2. Antigravity Pipeline (Zero-Copy FlatBuffers via Rust NAPI)
function processAntigravity(flatBufferBytes) {
    const ingested = antigravityPipeline.ingest(flatBufferBytes);
    
    if (ingested) {
        // Synchronously poll the result out of the SPMC ring to drain it
        antigravityPipeline.pollNext((externalBuffer) => {
            // Buffer is a zero-copy view of the Rust ring buffer slot
            // Telemetry is now handled natively inside the Rust engine!
        });
    }
}

// --- TRAFFIC GENERATOR ---
function createMockEvent(seq) {
    return {
        timestamp_ns: Date.now() * 1000000,
        symbol: 'AAPL',
        price: 155.5 + Math.random(),
        volume: 1000,
        sequence_id: seq
    };
}

let seqId = 0;
function generateEvent() {
    const data = createMockEvent(seqId++);
    
    // Legacy String Payload
    const jsonString = JSON.stringify(data);
    
    // Antigravity FlatBuffer Payload
    const builder = new flatbuffers.Builder(256);
    const sym = builder.createString(data.symbol);
    Event.startEvent(builder);
    Event.addTimestampNs(builder, 0n); // mock long
    Event.addSymbol(builder, sym);
    Event.addPrice(builder, data.price);
    Event.addVolume(builder, BigInt(data.volume));
    Event.addSequenceId(builder, BigInt(data.sequence_id));
    const evt = Event.endEvent(builder);
    builder.finish(evt);
    const fbbBytes = Buffer.from(builder.asUint8Array());

    return { jsonString, fbbBytes };
}

function startGenerator() {
    function tick() {
        if (!isRunning) return;
        for (let i = 0; i < 50; i++) { // Batch 50 per tick
            const { jsonString, fbbBytes } = generateEvent();
            processLegacy(jsonString);
            processAntigravity(fbbBytes);
        }
        setImmediate(tick);
    }
    tick();
}

function injectBurst() {
    console.log("INJECTING BURST TRAFFIC (10,000 events)");
    for (let i = 0; i < 10000; i++) {
        const { jsonString, fbbBytes } = generateEvent();
        processLegacy(jsonString);
        processAntigravity(fbbBytes);
    }
}

server.listen(TARGET_PORT, () => {
    console.log(`Trading Dashboard listening on port ${TARGET_PORT}`);
});
