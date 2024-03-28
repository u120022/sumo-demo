const THREAD_COUNT: usize = 8;

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

    let mut graph = petgraph::Graph::<(f64, f64), f64, petgraph::Undirected>::new_undirected();

    for node in nodes {
        graph.add_node((node.1, node.2));
    }

    for edge in edges {
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

    // maximum size graph only
    let candidates = petgraph::algo::kosaraju_scc(&graph)
        .into_iter()
        .max_by_key(|nodes| nodes.len())
        .unwrap();

    let mut tree = rstar::RTree::new();
    for candidate in candidates {
        let node = graph.node_weight(candidate).unwrap();
        tree.insert(rstar::primitives::GeomWithData::new(
            [node.0, node.1],
            candidate,
        ));
    }

    #[rustfmt::skip]
    let pairs: Vec<(i32, f64, f64, f64, f64)> = sqlx::query_as("SELECT id, ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)), ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom)) FROM pair")
        .fetch_all(&pool)
        .await
        .unwrap();

    let mut plans = vec![];
    for pair in pairs {
        let n1 = tree.nearest_neighbor(&[pair.1, pair.2]).unwrap().data;
        let n2 = tree.nearest_neighbor(&[pair.3, pair.4]).unwrap().data;
        plans.push((n1, n2));
    }

    println!("[plans stats] plans: {}", plans.len());

    let indicator = std::sync::Arc::new(indicatif::ProgressBar::new(plans.len() as u64));
    let graph = std::sync::Arc::new(graph);
    let split_len = plans.len().div_ceil(THREAD_COUNT);
    let mut threads = vec![];

    for thread in 0..THREAD_COUNT {
        let indicator = indicator.clone();
        let graph = graph.clone();
        let plans = plans
            .iter()
            .skip(split_len * thread)
            .take(split_len)
            .cloned()
            .collect::<Vec<_>>();

        let thread = std::thread::spawn(move || {
            let mut paths = vec![];

            for plan in plans {
                let path = petgraph::algo::astar(
                    graph.as_ref(),
                    plan.0,
                    |n| n == plan.1,
                    |e| *e.weight(),
                    |_| 0.0,
                );

                if let Some((_, path)) = path {
                    let path = path
                        .into_iter()
                        .map(|n| n.index() as i32)
                        .collect::<Vec<_>>();

                    paths.push(path);
                }

                indicator.inc(1);
            }

            paths
        });
        threads.push(thread);
    }

    let mut paths = vec![];
    for thread in threads {
        let mut split_paths = thread.join().unwrap();
        paths.append(&mut split_paths);
    }

    indicator.finish();

    println!("[path stats] paths: {}", paths.len());

    let bytes = postcard::to_extend(&paths, vec![]).unwrap();
    std::fs::write("path.bin", &bytes).unwrap();
}
