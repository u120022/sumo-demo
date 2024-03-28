use rayon::iter::{IntoParallelRefMutIterator, ParallelIterator};

struct Agent {
    x: f64,
    y: f64,
}

#[tokio::main]
async fn main() {
    #[rustfmt::skip]
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect("postgres://postgres:0@localhost/postgres")
        .await
        .expect("failed to connect postgresql");

    #[rustfmt::skip]
    let nodes: Vec<(i32, f64, f64)> = sqlx::query_as("SELECT id, ST_X(geom), ST_Y(geom) FROM node")
        .fetch_all(&pool)
        .await
        .unwrap();

    let bytes = std::fs::read("path.bin").unwrap();
    let paths: Vec<Vec<i32>> = postcard::from_bytes(&bytes).unwrap();
    drop(bytes);

    let mut agents = paths
        .iter()
        .map(|path| Agent {
            x: nodes[path[0] as usize].1,
            y: nodes[path[0] as usize].2,
        })
        .collect::<Vec<_>>();

    println!("[agent stats] agent: {}", agents.len());

    let pb = indicatif::ProgressBar::new(60 * 60 * 24 * 7);

    for _ in 0..(60 * 60 * 24 * 7) {
        agents.par_iter_mut().for_each(|agent| {
            agent.x += 0.01;
            agent.y += 0.01;
        });

        pb.inc(1);
    }
}
