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
    let edges: Vec<(i32, i32, i32, f64, Option<f64>)> = sqlx::query_as("SELECT e.id, e.n1, e.n2, e.distance, w.width FROM edge e JOIN width w ON e.id = w.id")
        .fetch_all(&pool)
        .await
        .unwrap();

    let mut tree = rstar::RTree::new();

    for node in &nodes {
        tree.insert(rstar::primitives::GeomWithData::new(
            [node.1, node.2],
            node.0 as usize,
        ));
    }

    println!("[tree stats] node: {}", tree.size());

    let mut graph = petgraph::Graph::<(f64, f64), f64, petgraph::Undirected>::new_undirected();

    for node in &nodes {
        graph.add_node((node.1, node.2));
    }

    for edge in &edges {
        let n1 = petgraph::graph::NodeIndex::new(edge.1 as usize - 1);
        let n2 = petgraph::graph::NodeIndex::new(edge.2 as usize - 1);
        let w = edge.3 / edge.4.unwrap_or(15.0).clamp(2.0, 15.0);
        graph.add_edge(n1, n2, w);
    }

    println!(
        "[graph stats] nodes: {}, edges: {}",
        graph.node_count(),
        graph.edge_count()
    );

    #[rustfmt::skip]
    let pairs: Vec<(i32, f64, f64, f64, f64)> = sqlx::query_as("SELECT id, ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)), ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom)) FROM pair")
        .fetch_all(&pool)
        .await
        .unwrap();

    println!("[pairs stats] pairs: {}", pairs.len());

    let mut plans = vec![];
    for pair in &pairs {
        let n1 = tree.nearest_neighbor(&[pair.1, pair.2]).unwrap().data;
        let n2 = tree.nearest_neighbor(&[pair.3, pair.4]).unwrap().data;
        plans.push((n1, n2));
    }

    println!("[plans stats] plans: {}", plans.len());

    let indicator = indicatif::ProgressBar::new(plans.len() as u64);

    let mut paths = vec![];
    for plan in &plans {
        let n1 = petgraph::graph::NodeIndex::new(plan.0);
        let n2 = petgraph::graph::NodeIndex::new(plan.1);
        let path = petgraph::algo::dijkstra(&graph, n1, Some(n2), |e| *e.weight());

        let path = path
            .keys()
            .map(|node| node.index() as i32)
            .collect::<Vec<_>>();

        paths.push(path);

        indicator.inc(1);
    }

    indicator.finish();

    println!("[path stats] paths: {}", paths.len());

    let bytes = postcard::to_extend(&paths, vec![]).unwrap();
    std::fs::write("path.bin", &bytes).unwrap();
}
