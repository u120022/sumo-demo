use futures::StreamExt;

#[tokio::main]
async fn main() {
    match simple_logging::log_to_file("crawler.log", log::LevelFilter::Info) {
        Ok(_) => (),
        Err(err) => {
            log::error!("failed to open log file ({})", err);
            panic!("failed to open log file ({})", err);
        }
    }

    let (x0, y0) = slippy_map_tiles::lat_lon_to_tile(36.646053135, 137.011029079, 18);
    let (x1, y1) = slippy_map_tiles::lat_lon_to_tile(36.793910577, 137.180130220, 18);

    let mut tiles = vec![];
    for y in u32::min(y0, y1)..u32::max(y0, y1) {
        for x in u32::min(x0, x1)..u32::max(x0, x1) {
            tiles.push((x, y));
        }
    }

    #[rustfmt::skip]
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .connect("postgres://postgres:0@localhost/postgres")
        .await
    {
        Ok(inner) => inner,
        Err(err) => {
            log::error!("failed to connect postgresql ({})", err);
            panic!("failed to connect postgresql ({})", err);
        }
    };
    let pool = std::sync::Arc::new(pool);

    #[rustfmt::skip]
    match sqlx::query("DROP TABLE IF EXISTS fgd")
        .execute(&*pool)
        .await
    {
        Ok(_) => (),
        Err(err) => {
            log::error!("failed to drop table ({})", err);
            panic!("failed to drop table ({})", err);
        }
    };

    #[rustfmt::skip]
    match sqlx::query("CREATE TABLE IF NOT EXISTS fgd (id Serial PRIMARY KEY, type Text, geom Geometry(LineString, 6668))")
        .execute(&*pool)
        .await
    {
        Ok(_) => (),
        Err(err) => {
            log::error!("failed to create table ({})", err);
            panic!("failed to create table ({})", err);
        }
    };

    let client = std::sync::Arc::new(reqwest::Client::new());
    let pb = std::sync::Arc::new(indicatif::ProgressBar::new(tiles.len() as u64));

    futures::stream::iter(tiles)
        .map(|(xtile, ytile)| {
            let pool = pool.clone();
            let client = client.clone();
            let pb = pb.clone();

            #[rustfmt::skip]
            let url = format!("https://cyberjapandata.gsi.go.jp/xyz/experimental_fgd/18/{}/{}.geojson", xtile, ytile);

            async move {
                pb.inc(1);

                let response = match client.get(url).send().await {
                    Ok(inner) => inner,
                    Err(err) => {
                        log::warn!("({}, {}): failed to request on http ({})", xtile, ytile, err);
                        return;
                    },
                };

                if response.status() != reqwest::StatusCode::OK {
                    log::warn!("({}, {}): invalid http status ({})", xtile, ytile, response.status());
                    return;
                }

                let text = match response.text().await {
                    Ok(inner) => inner,
                    Err(err) => {
                        log::warn!("({}, {}): failed to read http body ({})", xtile, ytile, err);
                        return;
                    }
                };

                let geojson = match text.parse::<geojson::GeoJson>() {
                    Ok(inner) => inner,
                    Err(err) => {
                        log::warn!("({}, {}): failed to parse as geojson ({})", xtile, ytile, err);
                        return;
                    }
                };

                let features = match geojson::FeatureCollection::try_from(geojson) {
                    Ok(inner) => inner,
                    Err(err) => {
                        log::warn!("({}, {}): failed to get feature collection ({})", xtile, ytile, err);
                        return;
                    }
                };

                let mut r#types = vec![];
                let mut geometries = vec![];

                for feature in features {
                    let geojson::Feature { geometry, properties, ..} = feature;

                    let geometry = match geometry {
                        Some(inner) => inner,
                        None => {
                            log::warn!("({}, {}): feature has no geometry", xtile, ytile);
                            continue;
                        }
                    };

                    if geometry.value.type_name() != "LineString" {
                        log::warn!("({}, {}): feature has no LineString geometry", xtile, ytile);
                        continue;
                    }

                    let properties = match properties {
                        Some(inner) => inner,
                        None => {
                            log::warn!("({}, {}): feature has no properties", xtile, ytile);
                            continue;
                        }
                    };

                    let r#type = match properties.get("type") {
                        Some(inner) => inner,
                        None => {
                            log::warn!("({}, {}): feature has no type property", xtile, ytile);
                            continue;
                        }
                    };

                    r#types.push(r#type.to_string());
                    geometries.push(geometry.to_string());
                }

                match sqlx::query("INSERT INTO fgd (type, geom) SELECT type, ST_SetSRID(ST_GeomFromGeoJSON(geom), 6668) FROM unnest($1, $2) AS _(type, geom)")
                    .bind(r#types)
                    .bind(geometries)
                    .execute(&*pool)
                    .await
                {
                    Ok(_) => (),
                    Err(err) => {
                        log::warn!("({}, {}): failed to insert ({})", xtile, ytile, err);
                        return;
                    }
                }
            }
        })
        .buffer_unordered(512)
        .collect::<Vec<_>>()
        .await;

    pb.finish();
}
