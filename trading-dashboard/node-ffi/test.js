const ffi = require('./index.js');

console.log("Initializing module...");
ffi.init();

console.log("Creating NodeConsumer...");
const consumer = new ffi.NodeConsumer();

console.log("Calling consume_next...");
consumer.consumeNext((buffer) => {
    console.log("Received buffer of length:", buffer.length);
    console.log("Buffer contents:", buffer.toString());
});

console.log("Test completed successfully!");
