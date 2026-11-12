use gst::prelude::*;
use gstreamer as gst;
use std::env;
use std::sync::{Arc, Mutex};

// Packet grid (ptime); 1 ms == 48 samples at 48 kHz.
const QUANTUM: gst::ClockTime = gst::ClockTime::from_mseconds(1);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    gst::init()?;

    let desc = env::args().skip(1).collect::<Vec<_>>().join(" ");
    if desc.is_empty() {
        eprintln!("usage: gst-launch-custom <pipeline description>");
        std::process::exit(1);
    }

    let pipe = gst::parse::launch(&desc)?
        .downcast::<gst::Pipeline>()
        .map_err(|_| "description did not produce a pipeline")?;

    // System clock disciplined by ptp4l/phc2sys. TAI matches PTP/AES67
    // timescale (needs the kernel TAI offset to be set, see CLOCK_TAI).
    let clock: gst::Clock = glib::Object::builder::<gst::SystemClock>()
        .property("clock-type", gst::ClockType::Tai)
        .build()
        .upcast();
    pipe.use_clock(Some(&clock));

    // Quantize the stream's start to the packet grid. The pipeline latency
    // (applied by the sink's sync) is what puts the RTP timestamp behind the
    // send time, so no manual offset here.
    if let Some(pay) = pipe.by_name("pay") {
        let pad = pay.static_pad("sink").expect("pay has no sink pad");
        let next: Arc<Mutex<Option<gst::ClockTime>>> = Arc::new(Mutex::new(None));
        pad.add_probe(gst::PadProbeType::BUFFER, move |_pad, info| {
            if let Some(gst::PadProbeData::Buffer(ref mut buf)) = info.data {
                let buf = buf.make_mut();
                let mut n = next.lock().unwrap();
                // Re-anchor on the first buffer and after discontinuities
                // (e.g. SRT reconnect).
                if buf.flags().contains(gst::BufferFlags::DISCONT) {
                    *n = None;
                }
                let pts = match *n {
                    Some(p) => p,
                    None => {
                        let orig = buf.pts().unwrap_or(gst::ClockTime::ZERO);
                        let q = QUANTUM.nseconds();
                        gst::ClockTime::from_nseconds(orig.nseconds() / q * q)
                    }
                };
                buf.set_pts(pts);
                *n = Some(pts + buf.duration().unwrap_or(QUANTUM));
            }
            gst::PadProbeReturn::Ok
        });
    } else {
        eprintln!("warning: no element named 'pay' found, buffers not re-stamped");
    }

    let bus = pipe.bus().expect("pipeline has no bus");

    pipe.set_state(gst::State::Paused)?;
    let (res, cur, _) = pipe.state(gst::ClockTime::from_seconds(10));
    println!("preroll: {:?}, state {:?}", res, cur);

    pipe.set_start_time(gst::ClockTime::NONE);
    pipe.set_base_time(gst::ClockTime::ZERO);
    pipe.set_state(gst::State::Playing)?;

    println!("start_time   = {:?}", pipe.start_time());
    println!("base_time    = {:?}", pipe.base_time());
    println!("clock time   = {:?}", clock.time());
    println!("running time = {:?}", pipe.current_running_time());
    print_latency(&pipe);

    for msg in bus.iter_timed(gst::ClockTime::NONE) {
        use gst::MessageView;
        match msg.view() {
            MessageView::Eos(_) => break,
            MessageView::Error(err) => {
                eprintln!(
                    "Error from {:?}: {} ({:?})",
                    err.src().map(|s| s.path_string()),
                    err.error(),
                    err.debug()
                );
                break;
            }
            // Same as gst-launch: re-distribute latency when it changes
            // (e.g. srtsrc after connect).
            MessageView::Latency(_) => {
                if let Err(e) = pipe.recalculate_latency() {
                    eprintln!("recalculate_latency failed: {e}");
                }
                print_latency(&pipe);
            }
            _ => {}
        }
    }

    pipe.set_state(gst::State::Null)?;
    Ok(())
}

fn print_latency(pipe: &gst::Pipeline) {
    let mut q = gst::query::Latency::new();
    if pipe.query(&mut q) {
        let (live, min, max) = q.result();
        println!(
            "pipeline latency: live={live} min={min} max={max:?} (configured: {:?})",
            pipe.latency()
        );
    }
}