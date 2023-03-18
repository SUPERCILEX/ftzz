use rand::{distributions::Distribution, Rng};
use rand_distr::Normal;
pub use scheduler::{run, GeneratorStats};
pub use tasks::{DynamicGenerator, GeneratorBytes, StaticGenerator};

mod file_contents;
mod files;
mod scheduler;
mod tasks;

pub fn truncatable_normal(mean: f64) -> Normal<f64> {
    let mean = mean + 0.5;
    Normal::new(mean, mean / 3.).unwrap()
}

// TODO https://github.com/rust-random/rand/issues/1189
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
fn sample_truncated<R: Rng + ?Sized>(normal: &Normal<f64>, rng: &mut R) -> u64 {
    let max = normal.mean() * 2.;
    for _ in 0..5 {
        let x = normal.sample(rng);
        if 0. <= x && x < max {
            return x as u64;
        }
    }
    normal.mean() as u64
}
