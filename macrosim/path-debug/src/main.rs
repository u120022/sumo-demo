const SAMPLE_COUNT: usize = 20_000;

#[tokio::main]
async fn main() {
    type Data = (
        petgraph::Graph<(f64, f64), (f64, u32), petgraph::Undirected>,
        Vec<Vec<u32>>,
    );

    let bytes = std::fs::read("path.bin").unwrap();
    let (graph, paths): Data = postcard::from_bytes(&bytes).unwrap();
    drop(bytes);

    #[rustfmt::skip]
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect("postgres://postgres:0@localhost/postgres")
        .await
        .expect("failed to connect postgresql");

    #[rustfmt::skip]
    sqlx::query("DROP TABLE IF EXISTS path")
        .execute(&pool)
        .await
        .unwrap();

    #[rustfmt::skip]
    sqlx::query("CREATE TABLE IF NOT EXISTS path (id Serial, geom Geometry(LineString, 6668))")
        .execute(&pool)
        .await
        .unwrap();

    let indicator = indicatif::ProgressBar::new(SAMPLE_COUNT as u64);

    let mut rng = rand::thread_rng();
    let paths = rand::seq::SliceRandom::choose_multiple(paths.as_slice(), &mut rng, SAMPLE_COUNT);
    for path in paths {
        let mut xs = vec![];
        let mut ys = vec![];

        for n in path {
            let (x, y) = graph
                .node_weight(petgraph::graph::NodeIndex::new(*n as usize))
                .unwrap()
                .clone();

            xs.push(x);
            ys.push(y);
        }

        #[rustfmt::skip]
        sqlx::query("INSERT INTO path (geom) SELECT ST_MakeLine(geoms) FROM (SELECT array_agg(ST_Point(x, y)) AS geoms FROM unnest($1, $2) AS _(x, y))")
            .bind(xs)
            .bind(ys)
            .execute(&pool)
            .await
            .unwrap();

        indicator.inc(1);
    }

    indicator.finish();
}
