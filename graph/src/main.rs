use rand::Rng;

#[tokio::main]
async fn main() {
    let mut rng = rand::thread_rng();

    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect("postgres://postgres:0@localhost/postgres")
        .await
        .expect("fauled to connect postgresql");

    let nodes: Vec<(i32, f64, f64)> = sqlx::query_as("SELECT id, ST_X(geom), ST_Y(geom) FROM node")
        .fetch_all(&pool)
        .await
        .unwrap();

    let edges: Vec<(i32, i32, i32, f32)> = sqlx::query_as("SELECT id, n1, n2, weight FROM edge")
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
        graph.add_edge(n1.into(), n2.into(), edge.3);
    }

    println!(
        "[graph stats] node: {}, edges: {}",
        graph.node_count(),
        graph.edge_count()
    );

    for i in 0..1_000_000 {
        let n1 = rng.gen_range(0..graph.node_count() - 1) as u32;
        let n2 = rng.gen_range(0..graph.node_count() - 1) as u32;
        let path =
            petgraph::algo::dijkstra(&graph, n1.into(), Some(n2.into()), |edge| *edge.weight());
        println!(
            "[graph dijkstra {}] path: {} -> {}, edge: {}",
            i,
            n1,
            n2,
            path.len()
        );
    }
}
