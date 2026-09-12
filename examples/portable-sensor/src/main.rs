use portable_sensor::{REPLAY, process_adc, sensor_pipeline};

fn main() {
    let mut pipeline = sensor_pipeline(100);
    println!("raw,level,delta,high");
    let mut emit = |raw| {
        let observation = process_adc(&mut pipeline, raw);
        println!(
            "{raw},{},{},{}",
            observation.features.level, observation.features.delta, observation.high
        );
    };
    let mut args = std::env::args().skip(1).peekable();
    if args.peek().is_none() {
        for raw in REPLAY {
            emit(raw);
        }
    } else {
        for arg in args {
            let raw = arg.parse::<u16>().unwrap_or_else(|_| {
                eprintln!("Invalid ADC sample {arg:?}: expected an integer from 0 to 65535");
                std::process::exit(2);
            });
            emit(raw);
        }
    }
}
