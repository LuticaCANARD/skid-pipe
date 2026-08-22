use skid_pipe::Pipe;

enum Input {
    Sensor(u16),
    Command(&'static str),
}

#[derive(Debug, PartialEq)]
enum Routed {
    Sensor { value: u16, branch_runs: u32 },
    Command { accepted: bool, branch_runs: u32 },
}

fn main() {
    let mut sensor_runs = 0_u32;
    let mut command_runs = 0_u32;

    let mut pipeline = Pipe::new(|input: Input| input).then(move |input| match input {
        Input::Sensor(value) => {
            sensor_runs += 1;
            Routed::Sensor {
                value: value.saturating_mul(2),
                branch_runs: sensor_runs,
            }
        }
        Input::Command(command) => {
            command_runs += 1;
            Routed::Command {
                accepted: command == "start",
                branch_runs: command_runs,
            }
        }
    });

    assert_eq!(
        pipeline.run(Input::Sensor(10)),
        Routed::Sensor {
            value: 20,
            branch_runs: 1,
        },
    );
    assert_eq!(
        pipeline.run(Input::Command("start")),
        Routed::Command {
            accepted: true,
            branch_runs: 1,
        },
    );
    assert_eq!(
        pipeline.run(Input::Sensor(20)),
        Routed::Sensor {
            value: 40,
            branch_runs: 2,
        },
    );
}
