const net = require('net');

const MSG_COUNT = 1000000;
let latencies = new Float64Array(MSG_COUNT);
let received = 0;
let baselineWindow = [];
let startTime = null;

const server = net.createServer((socket) => {
    let buffer = '';
    
    socket.on('data', (data) => {
        buffer += data.toString();
        let boundary = buffer.indexOf('\n');
        
        while (boundary !== -1) {
            const chunk = buffer.slice(0, boundary);
            buffer = buffer.slice(boundary + 1);
            boundary = buffer.indexOf('\n');
            
            if (chunk.length === 0) continue;
            
            try {
                const event = JSON.parse(chunk);
                
                // End-to-End Latency calculation (in nanoseconds, then to float)
                const now = Number(process.hrtime.bigint());
                const sent = Number(event.timestamp_ns);
                const latencyNs = now - sent;
                
                latencies[received] = latencyNs;
                
                // Sliding window rule evaluation
                baselineWindow.push(event);
                if (baselineWindow.length > 10) {
                    baselineWindow.shift();
                }
                
                let matchCount = 0;
                for (let i = 0; i < baselineWindow.length; i++) {
                    const item = baselineWindow[i];
                    if (item.symbol === 'AAPL' && item.price > 150) {
                        matchCount++;
                    }
                }
                
                received++;
                if (received === 1) {
                    startTime = process.hrtime.bigint();
                }
                
                if (received === MSG_COUNT) {
                    printResults();
                    process.exit(0);
                }
            } catch (err) {
                console.error("Parse error:", err);
            }
        }
    });
});

function printResults() {
    const endTime = process.hrtime.bigint();
    const durationSec = Number(endTime - startTime) / 1e9;
    const throughput = MSG_COUNT / durationSec;
    
    // Sort array
    latencies.sort();
    
    const p50 = latencies[Math.floor(MSG_COUNT * 0.50)];
    const p99 = latencies[Math.floor(MSG_COUNT * 0.99)];
    const p99_9 = latencies[Math.floor(MSG_COUNT * 0.999)];
    const min = latencies[0];
    const max = latencies[MSG_COUNT - 1];

    console.log(`\n=== Legacy Node.js Benchmark Results ===`);
    console.log(`Messages Transmitted : ${MSG_COUNT}`);
    console.log(`Elapsed Time         : ${durationSec.toFixed(2)}s`);
    console.log(`Throughput           : ${throughput.toFixed(2)} msgs/sec`);
    console.log(`\n=== End-to-End Latency ===`);
    console.log(`Min                  : ${min} ns`);
    console.log(`Median (p50)         : ${p50} ns`);
    console.log(`p99                  : ${p99} ns`);
    console.log(`p99.9                : ${p99_9} ns`);
    console.log(`Max                  : ${max} ns`);
}

server.listen(4000, () => {
    console.log('Legacy server listening on port 4000');
});
