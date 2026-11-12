use gst::prelude::*;
use gstreamer as gst;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    gst::init()?;

    // Everything after the program name is the pipeline description,
    // e.g.: gst-launch-custom videotestsrc ! autovideosink
    let desc = env::args().skip(1).collect::<Vec<_>>().join(" ");
    if desc.is_empty() {
        eprintln!("usage: gst-launch-custom <pipeline description>");
        std::process::exit(1);
    }

    let pipe = gst::parse::launch(&desc)?;

    // Don't let the pipeline manage base-time itself, then pin it to 0
    // so running time == absolute clock time.
    pipe.set_start_time(gst::ClockTime::NONE);
    pipe.set_base_time(gst::ClockTime::ZERO);

    let bus = pipe.bus().expect("pipeline has no bus");
    pipe.set_state(gst::State::Playing)?;

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
            _ => {}
        }
    }

    pipe.set_state(gst::State::Null)?;
    Ok(())
}