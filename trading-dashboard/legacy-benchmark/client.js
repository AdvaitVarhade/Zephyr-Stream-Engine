const net = require('net');

const MSG_COUNT = 1000000;
const BATCH_SIZE = 10000;

const client = new net.Socket();

client.connect(4000, '127.0.0.1', () => {
    console.log('Connected to Legacy Server, starting blast...');
    
    let seq = 0;
    
    function sendBatch() {
        let payload = '';
        const limit = Math.min(seq + BATCH_SIZE, MSG_COUNT);
        
        for (; seq < limit; seq++) {
            // Reconstruct exact same JSON payload the mock generator uses
            const obj = {
                timestamp_ns: process.hrtime.bigint().toString(), // send string to avoid JS BigInt serialization loss
                symbol: 'AAPL',
                price: 150.25,
                volume: 100,
                sequence_id: seq
            };
            
            payload += JSON.stringify(obj) + '\n';
        }
        
        // Write batch
        const canContinue = client.write(payload);
        
        if (seq < MSG_COUNT) {
            if (canContinue) {
                setImmediate(sendBatch);
            } else {
                client.once('drain', sendBatch);
            }
        }
    }
    
    // Start loop
    setTimeout(sendBatch, 500); // Give server a moment
});
