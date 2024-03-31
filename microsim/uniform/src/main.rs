use rand::{distributions::Distribution, seq::IteratorRandom, SeedableRng};

const SEED: u64 = 42;
const VELOCITY: f64 = 10.0;
const MAX_STEP_COUNT: usize = 60 * 60 * 1;
const MAX_AGENT_COUNT: usize = 10000;

#[derive(Debug, Clone, Default)]
struct Agent {
    xy: (f64, f64),
    seq: usize,
    shift: usize,
    work: bool,
}

#[tokio::main]
async fn main() {
    type Data = (
        petgraph::Graph<(f64, f64), (f64, u32), petgraph::Undirected>,
        Vec<Vec<u32>>,
    );

    let bytes = std::fs::read("path.bin").unwrap();
    let (graph, paths): Data = postcard::from_bytes(&bytes).unwrap();
    drop(bytes);

    let indicator = indicatif::ProgressBar::new(MAX_STEP_COUNT as u64);

    let mut rng = rand::rngs::StdRng::seed_from_u64(SEED);
    let paths = paths.into_iter().choose_multiple(&mut rng, MAX_AGENT_COUNT);

    let mut agents = vec![Agent::default(); paths.len()];
    for i in 0..agents.len() {
        agents[i].xy = *graph
            .node_weight(petgraph::graph::NodeIndex::new(paths[i][0] as usize))
            .unwrap();

        agents[i].shift = rand::distributions::Uniform::new(0, MAX_STEP_COUNT).sample(&mut rng);
    }

    for t in 0..MAX_STEP_COUNT {
        for i in 0..agents.len() {
            if t < agents[i].shift {
                agents[i].work = false;
                continue;
            }

            if agents[i].seq >= paths[i].len() {
                agents[i].work = false;
                continue;
            }

            let n1 = {
                let index = paths[i][agents[i].seq as usize] as usize;
                petgraph::graph::NodeIndex::new(index)
            };

            let (x0, y0) = agents[i].xy;
            let (x1, y1) = *graph.node_weight(n1).unwrap();

            let p0 = geo::Point::new(x0, y0);
            let p1 = geo::Point::new(x1, y1);

            let dist = geo::HaversineDistance::haversine_distance(&p0, &p1);

            if dist <= VELOCITY {
                agents[i].xy = p1.x_y();
                agents[i].seq += 1;
            } else {
                let bear = geo::HaversineBearing::haversine_bearing(&p0, p1);
                let dest = geo::HaversineDestination::haversine_destination(&p0, bear, VELOCITY);
                agents[i].xy = dest.x_y();
            }

            agents[i].work = true;
        }

        indicator.inc(1);
    }

    indicator.finish();
    println!("{}", indicator.elapsed().as_secs_f64());

    #[rustfmt::skip]
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect("postgres://postgres:0@localhost/postgres")
        .await
        .expect("failed to connect postgresql");

    #[rustfmt::skip]
    sqlx::query("DROP TABLE IF EXISTS agent")
        .execute(&pool)
        .await
        .unwrap();

    #[rustfmt::skip]
    sqlx::query("CREATE TABLE IF NOT EXISTS agent (id Serial, geom Geometry(Point, 6668))")
        .execute(&pool)
        .await
        .unwrap();

    let agents = agents.iter().filter(|agent| agent.work).collect::<Vec<_>>();

    let x = agents.iter().map(|a| a.xy.0).collect::<Vec<_>>();
    let y = agents.iter().map(|a| a.xy.1).collect::<Vec<_>>();

    #[rustfmt::skip]
    sqlx::query("INSERT INTO agent (geom) SELECT ST_Point(x, y) FROM unnest($1, $2) as _(x, y)")
        .bind(&x)
        .bind(&y)
        .execute(&pool)
        .await
        .unwrap();
}
