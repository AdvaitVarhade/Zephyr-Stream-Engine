#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi_derive::napi;
use napi::Env;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use core_ringbuffer::mpsc;
use core_ringbuffer::spmc;
use core_ringbuffer::wait::{BusySpin, WaitStrategy};
use stream_analytics::window::SlidingWindowTracker;
use stream_analytics::{Rule, Criterion, FieldSelector, CompareOp, Value, evaluate_rule};

#[napi(object)]
pub struct TelemetryData {
    pub processed: u32,
    pub latencies: Vec<f64>, // nanoseconds
}

#[derive(Default)]
struct InternalTelemetry {
    processed: u32,
    latencies: Vec<f64>,
}

#[napi]
pub fn init() -> napi::Result<()> {
    Ok(())
}

#[napi]
pub struct NapiPipeline {
    mpsc_producer: Arc<Mutex<mpsc::Producer>>,
    spmc_consumer: Arc<Mutex<spmc::Consumer<BusySpin>>>,
    telemetry: Arc<Mutex<InternalTelemetry>>,
}

#[napi]
impl NapiPipeline {
    #[napi(constructor)]
    pub fn new() -> Self {
        let (mut mpsc_producers, mut mpsc_consumer) = mpsc::mpsc(8192, 1);
        let mpsc_producer = mpsc_producers.remove(0);

        let (mut spmc_producer, mut spmc_consumers) = spmc::spmc(8192, 1);
        let spmc_consumer = spmc_consumers.remove(0);
        
        let telemetry = Arc::new(Mutex::new(InternalTelemetry::default()));
        let telemetry_clone = telemetry.clone();

        thread::spawn(move || {
            let mut tracker = SlidingWindowTracker::with_capacity(10, 5, 32); 
            tracker.register_key("AAPL");

            let rule = Rule::new("high_value_aapl", vec![
                Criterion::new(FieldSelector::Symbol, CompareOp::Eq, Value::Str("AAPL")),
                Criterion::new(FieldSelector::Price, CompareOp::Gt, Value::Float64(150.0)),
            ]);

            loop {
                mpsc_consumer.try_consume(|raw_bytes| {
                    let start = Instant::now();
                    
                    // Zero-copy evaluation
                    if let Ok(event) = flatbuffers::root::<core_ringbuffer::event_generated::event::Event>(raw_bytes) {
                        if stream_analytics::evaluator::evaluate_rule(&event, &rule) {
                            if let Some(symbol) = event.symbol() {
                                tracker.record(symbol, true);
                            }
                        }
                    }
                    
                    while spmc_producer.try_publish(raw_bytes).is_err() {
                        std::hint::spin_loop();
                    }
                    
                    let elapsed = start.elapsed().as_nanos() as f64;
                    
                    // Only lock telemetry occasionally to avoid bottlenecking the fast path
                    // But for this benchmark accuracy, we'll record directly (can batch if needed)
                    // We'll record every event to ensure accurate p99 computation.
                    let mut tel = telemetry_clone.lock().unwrap();
                    tel.processed += 1;
                    if tel.latencies.len() < 10000 { // prevent unbounded growth
                        tel.latencies.push(elapsed);
                    }
                });
                std::hint::spin_loop();
            }
        });

        Self {
            mpsc_producer: Arc::new(Mutex::new(mpsc_producer)),
            spmc_consumer: Arc::new(Mutex::new(spmc_consumer)),
            telemetry,
        }
    }

    #[napi]
    pub fn ingest(&self, buffer: Buffer) -> napi::Result<bool> {
        let mut producer = self.mpsc_producer.lock().unwrap();
        match producer.try_publish(buffer.as_ref()) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    #[napi]
    pub fn poll_telemetry(&self) -> napi::Result<TelemetryData> {
        let mut tel = self.telemetry.lock().unwrap();
        let data = TelemetryData {
            processed: tel.processed,
            latencies: std::mem::take(&mut tel.latencies),
        };
        tel.processed = 0;
        Ok(data)
    }

    #[napi]
    pub unsafe fn poll_next(&mut self, env: Env, callback: napi::JsFunction) -> napi::Result<bool> {
        let mut consumer = self.spmc_consumer.lock().unwrap();
        let mut processed = false;
        
        consumer.try_consume(|raw_slice| {
            processed = true;
            let mut js_buffer_val = std::ptr::null_mut();
            let status = unsafe {
                napi::sys::napi_create_external_buffer(
                    env.raw(),
                    raw_slice.len(),
                    raw_slice.as_ptr() as *mut _,
                    None,
                    std::ptr::null_mut(),
                    &mut js_buffer_val,
                )
            };
            if status == napi::sys::Status::napi_ok {
                let js_buffer = unsafe { <napi::JsBuffer as napi::NapiValue>::from_raw_unchecked(env.raw(), js_buffer_val) };
                let _ = callback.call(None, &[js_buffer]);
            }
        });
        Ok(processed)
    }
}
