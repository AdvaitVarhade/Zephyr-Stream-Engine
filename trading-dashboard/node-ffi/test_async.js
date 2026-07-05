const ffi = require('./index.js');

console.log("Initializing module...");
ffi.init();

console.log("Creating NapiConsumer...");
const consumer = new ffi.NapiConsumer();

let pollCount = 0;
const interval = setInterval(() => {
    pollCount++;
    const hasData = consumer.pollNext((buffer) => {
        console.log(`[Poll ${pollCount}] Received buffer of length:`, buffer.length);
        console.log(`[Poll ${pollCount}] Buffer contents:`, buffer.toString());
    });

    if (!hasData) {
        console.log(`[Poll ${pollCount}] No more data. Polling complete.`);
        clearInterval(interval);
    }
}, 10);
