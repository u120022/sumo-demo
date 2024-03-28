#[tokio::main]
async fn main() {
    let bytes = std::fs::read("path.bin").unwrap();
    let paths: Vec<Vec<i32>> = postcard::from_bytes(&bytes).unwrap();
    drop(bytes);

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

    let pb = indicatif::ProgressBar::new(paths.len() as u64);

    for path in paths {
        let mut xs = vec![];
        let mut ys = vec![];

        for n in path {
            xs.push(nodes[n as usize].1);
            ys.push(nodes[n as usize].2);
        }

        #[rustfmt::skip]
        sqlx::query("INSERT INTO path (geom) SELECT ST_MakeLine(geoms) FROM (SELECT array_agg(ST_Point(x, y)) AS geoms FROM unnest($1, $2) AS _(x, y))")
            .bind(xs)
            .bind(ys)
            .execute(&pool)
            .await
            .unwrap();

        pb.inc(1);
    }

    pb.finish();
}
