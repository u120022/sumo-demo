use geo::GeodesicDestination;
use rand::prelude::*;

// https://www.airia.or.jp/publish/statistics/trend.html
const CAR_RATIO: f64 = 0.876;

// interpolation
const INTERPOLATION: f64 = 0.1;

// reference https://www.nite.go.jp/chem/risk/expofactor_index.html
const TIME_MEAN: f64 = 1.6125;
const TIME_SD: f64 = 2.282465;

// reference https://www.mlit.go.jp/road/census/r3/index.html
const SPEED: f64 = 33_800.0;

#[tokio::main]
async fn main() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect("postgres://postgres:0@localhost/postgres")
        .await
        .expect("failed to connect postgresql");

    #[rustfmt::skip]
    let meshes: Vec<(f64, f64, f64, f64, f64)> = sqlx::query_as("SELECT \"PTN_2020\", ST_XMin(geom), ST_YMin(geom), ST_XMax(geom), ST_YMax(geom) FROM population")
        .fetch_all(&pool)
        .await
        .unwrap();

    println!("[mesh stats] mesh: {}", meshes.len());

    let mut pairs = vec![];

    let mut rng = rand::thread_rng();
    let angle_distr = rand::distributions::Uniform::new(0.0, 360.0);
    let distance_distr = rand_distr::Normal::new(TIME_MEAN, TIME_SD).unwrap();
    for mesh in meshes {
        let pop = (mesh.0 * CAR_RATIO * INTERPOLATION) as usize;

        let x_distr = rand::distributions::Uniform::new(mesh.1, mesh.3);
        let y_distr = rand::distributions::Uniform::new(mesh.2, mesh.4);

        for _ in 0..pop {
            let x = rng.sample(x_distr);
            let y = rng.sample(y_distr);
            let angle = rng.sample(angle_distr);
            let distance = rng.sample(distance_distr).max(0.1) * SPEED;

            let point = geo::Point::new(x, y).geodesic_destination(angle, distance);
            let u = point.x();
            let v = point.y();

            pairs.push((x, y, u, v));
        }
    }

    println!("{}", pairs.len());

    let xs = pairs.iter().map(|p| p.0).collect::<Vec<_>>();
    let ys = pairs.iter().map(|p| p.1).collect::<Vec<_>>();
    let us = pairs.iter().map(|p| p.2).collect::<Vec<_>>();
    let vs = pairs.iter().map(|p| p.3).collect::<Vec<_>>();

    #[rustfmt::skip]
    sqlx::query("DROP TABLE IF EXISTS pair")
        .execute(&pool)
        .await
        .unwrap();

    #[rustfmt::skip]
    sqlx::query("CREATE TABLE IF NOT EXISTS pair (id Serial PRIMARY KEY, geom Geometry(LineString, 6668))")
        .execute(&pool)
        .await
        .unwrap();

    #[rustfmt::skip]
    sqlx::query("INSERT INTO pair (geom) SELECT ST_MakeLine(ST_Point(x, y), ST_Point(u, v)) FROM unnest($1, $2, $3, $4) AS _(x, y, u, v)")
        .bind(&xs)
        .bind(&ys)
        .bind(&us)
        .bind(&vs)
        .execute(&pool)
        .await
        .unwrap();
}
