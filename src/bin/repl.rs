#[macro_use]
extern crate papyrus;
use papyrus::prelude::*;

use sled;
use bincode;
use bincode::config::Options;

fn main() {
  let mut repl = repl!();

  let d = &mut ();

  repl.run(papyrus::run::RunCallbacks::new(d));
}