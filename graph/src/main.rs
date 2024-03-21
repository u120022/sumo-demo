#[tokio::main]
async fn main() {
    #[rustfmt::skip]
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect("postgres://postgres:0@localhost/postgres")
        .await
        .expect("fauled to connect postgresql");

    #[rustfmt::skip]
    let nodes: Vec<(i32, f64, f64)> = sqlx::query_as("SELECT id, ST_X(geom), ST_Y(geom) FROM node")
        .fetch_all(&pool)
        .await
        .unwrap();

    #[rustfmt::skip]
    let edges: Vec<(i32, i32, i32, f32, Option<f32>)> = sqlx::query_as("SELECT e.id, e.n1, e.n2, e.distance, w.width FROM edge e JOIN width w ON e.id = w.id")
        .fetch_all(&pool)
        .await
        .unwrap();

    let mut graph = petgraph::Graph::<(), f32>::new();

    for _ in nodes {
        graph.add_node(());
    }

    for edge in edges {
        let n1 = edge.1 as u32 - 1;
        let n2 = edge.2 as u32 - 1;
        let w = edge.3 / edge.4.unwrap_or(15.0).clamp(2.0, 15.0);
        graph.add_edge(n1.into(), n2.into(), w);
    }

    println!(
        "[graph stats] node: {}, edges: {}",
        graph.node_count(),
        graph.edge_count()
    );
    
    #[rustfmt::skip]
    let pairs: Vec<(i32, f64, f64, f64, f64)> = sqlx::query_as("SELECT id, ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)), ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom)) FROM pair")
        .fetch_all(&pool)
        .await
        .unwrap();

    println!("[pairs stats] pair: {}", pairs.len());
}
