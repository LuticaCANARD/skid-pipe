#![no_std]
#![forbid(unsafe_code)]

//! One sensor computation shared by native replay, Wasm replay, and firmware.
//! Values are integer ADC counts; samples must arrive in acquisition order at
//! a fixed interval. This sample-based filter does not model elapsed time.

use skid_pipe::{Chain, Pipe};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Features {
    pub level: u16,
    pub delta: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Observation {
    pub features: Features,
    pub high: bool,
}

/// Decode little-endian samples, subtract the zero offset, clamp to 12 bits,
/// and smooth with (previous + current) / 2, rounding down.
pub fn preprocessing(zero_offset: u16) -> impl Chain<[u8; 2], Output = u16> {
    let mut previous = None;
    Pipe::new(u16::from_le_bytes)
        .then(move |raw: u16| raw.saturating_sub(zero_offset).min(4095))
        .then(move |value: u16| {
            let filtered = previous.map_or(value, |old: u16| (old + value) / 2);
            previous = Some(filtered);
            filtered
        })
}

/// The first sample has zero delta; subsequent deltas use the filtered value.
pub fn feature_extractor() -> impl Chain<u16, Output = Features> {
    let mut previous = None;
    Pipe::new(move |level: u16| {
        let delta = previous.map_or(0, |old: u16| i32::from(level) - i32::from(old));
        previous = Some(level);
        Features { level, delta }
    })
}

/// Enter High at 3000 counts, leave at 2000, retain the decision in between.
pub fn classifier() -> impl Chain<Features, Output = Observation> {
    let mut high = false;
    Pipe::new(move |features: Features| {
        if features.level >= 3000 {
            high = true;
        } else if features.level <= 2000 {
            high = false;
        }
        Observation { features, high }
    })
}

/// Build once per sensor stream. Rebuilding explicitly resets all state.
pub fn sensor_pipeline(zero_offset: u16) -> impl Chain<[u8; 2], Output = Observation> {
    Pipe::from_chain(preprocessing(zero_offset))
        .then_chain(feature_extractor())
        .then_chain(classifier())
}

/// Firmware boundary: pass ADC samples to the same retained computation.
/// Board setup, sampling cadence, and output delivery belong to the caller.
pub fn process_adc(
    pipeline: &mut impl Chain<[u8; 2], Output = Observation>,
    raw: u16,
) -> Observation {
    pipeline.run(raw.to_le_bytes())
}

/// Constructed replay data, not a physical sensor recording.
pub const REPLAY: [u16; 7] = [100, 1100, 3100, 4100, 4100, 1100, 100];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_checks_filter_delta_and_hysteresis() {
        let mut pipeline = sensor_pipeline(100);
        let expected = [
            (0, 0, false),
            (500, 500, false),
            (1750, 1250, false),
            (2875, 1125, false),
            (3437, 562, true),
            (2218, -1219, true),
            (1109, -1109, false),
        ];
        for (raw, (level, delta, high)) in REPLAY.into_iter().zip(expected) {
            assert_eq!(
                process_adc(&mut pipeline, raw),
                Observation {
                    features: Features { level, delta },
                    high
                }
            );
        }
    }

    #[test]
    fn calibration_saturates_and_new_streams_reset_state() {
        let mut low = sensor_pipeline(u16::MAX);
        assert_eq!(process_adc(&mut low, 0).features.level, 0);
        let mut high = sensor_pipeline(0);
        assert_eq!(process_adc(&mut high, u16::MAX).features.level, 4095);
        assert!(process_adc(&mut high, u16::MAX).high);
        assert_eq!(process_adc(&mut high, 0).features.delta, -2048);
        let fresh = process_adc(&mut sensor_pipeline(0), 0);
        assert_eq!(fresh.features, Features { level: 0, delta: 0 });
        assert!(!fresh.high);
    }

    #[test]
    fn hysteresis_includes_both_boundaries() {
        let mut pipeline = classifier();
        for (level, expected) in [(2999, false), (3000, true), (2001, true), (2000, false)] {
            assert_eq!(pipeline.run(Features { level, delta: 0 }).high, expected);
        }
    }
}
