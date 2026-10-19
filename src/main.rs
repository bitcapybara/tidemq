use std::io::{self, Write};

fn main() -> io::Result<()> {
    writeln!(
        io::stdout().lock(),
        "TideMQ is in development. The message queue runtime is not implemented yet. See README.md."
    )
}
