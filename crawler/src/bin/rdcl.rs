use futures::StreamExt;
use std::str::FromStr;

fn to_tile_coord(x: f64, y: f64, z: i32) -> (i64, i64) {
    let pi = std::f64::consts::PI;

    let z_exp2 = 2.0f64.powi(z);

    let y_rad = y.to_radians();

    let x_tile = ((x + 180.0) / 360.0 * z_exp2) as i64;
    let y_tile = ((1.0 - (y_rad.tan() + (1.0 / y_rad.cos())).ln() / pi) / 2.0 * z_exp2) as i64;

    (x_tile, y_tile)
}

#[tokio::main]
async fn main() {
    let (x0, y0) = to_tile_coord(137.011029079, 36.646053135, 16);
    let (x1, y1) = to_tile_coord(137.180130220, 36.793910577, 16);

    let mut tiles: Vec<(i64, i64)> = vec![];
    for y in i64::min(y0, y1)..i64::max(y0, y1) {
        for x in i64::min(x0, x1)..i64::max(x0, x1) {
            tiles.push((x, y));
        }
    }

    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect("postgres://postgres:0@localhost/postgres")
        .await
        .expect("failed to connect postgresql");

    sqlx::query("DROP TABLE IF EXISTS rdcl")
        .execute(&pool)
        .await
        .expect("failed to drop table");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rdcl (id Serial PRIMARY KEY, geom Geometry(LineString, 6668))",
    )
    .execute(&pool)
    .await
    .expect("faile to create table");

    let client = reqwest::Client::new();
    let n_progress = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let n_total = tiles.len();

    futures::stream::iter(tiles)
        .map(|(xtile, ytile)| {
            let pool = pool.clone();

            let client = client.clone();
            let n_progress = n_progress.clone();

            let url = format!(
                "https://cyberjapandata.gsi.go.jp/xyz/experimental_rdcl/16/{}/{}.geojson",
                xtile, ytile
            );

            async move {
                let response = match client.get(url).send().await {
                    Ok(inner) => inner,
                    Err(err) => {
                        println!("invalid response: {}", err);
                        return;
                    }
                };

                if response.status() != reqwest::StatusCode::OK {
                    println!("invalid status: {}", response.status());
                    return;
                }

                let text = match response.text().await {
                    Ok(inner) => inner,
                    Err(err) => {
                        println!("invalid content: {}", err);
                        return;
                    }
                };

                let geojson = match geojson::GeoJson::from_str(&text) {
                    Ok(inner) => inner,
                    Err(err) => {
                        println!("invalid format: {}", err);
                        return;
                    }
                };

                let feats = match geojson::FeatureCollection::try_from(geojson) {
                    Ok(inner) => inner,
                    Err(err) => {
                        println!("invalid format: {}", err);
                        return;
                    }
                };

                for feat in feats {
                    let geojson::Feature { geometry, .. } = feat;

                    let geometry = match geometry {
                        Some(inner) => inner,
                        None => {
                            println!("no geometry");
                            continue;
                        }
                    };

                    if geometry.value.type_name() != "LineString" {
                        println!("no LineString geometry");
                        continue;
                    }

                    let status = sqlx::query(
                        "INSERT INTO rdcl (geom) VALUES (ST_SetSRID(ST_GeomFromGeoJSON($1), 6668))",
                    )
                    .bind(geometry.to_string())
                    .execute(&pool)
                    .await;

                    if let Err(err) = status {
                        println!("{}", err);
                    }
                }

                let n_progress = n_progress.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                println!("PROGRESS: {}/{}", n_progress, n_total);
            }
        })
        .buffer_unordered(512)
        .collect::<Vec<_>>()
        .await;
}
