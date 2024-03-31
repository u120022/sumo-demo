const THREAD_COUNT: usize = 8;

#[tokio::main]
async fn main() {
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
    let edges: Vec<(i32, i32, i32, f64, Option<f64>)> = sqlx::query_as("SELECT e.id, e.n1, e.n2, e.distance, w.width FROM edge e JOIN width w ON e.id = w.id")
        .fetch_all(&pool)
        .await
        .unwrap();

    let mut graph =
        petgraph::Graph::<(f64, f64), (f64, u32), petgraph::Undirected>::new_undirected();

    for node in nodes {
        graph.add_node((node.1, node.2));
    }

    for edge in edges {
        let n1 = petgraph::graph::NodeIndex::new(edge.1 as usize - 1);
        let n2 = petgraph::graph::NodeIndex::new(edge.2 as usize - 1);
        let distance = edge.3;
        let lane = ((edge.4.unwrap_or(f64::MAX).clamp(3.0, 18.0) / 3.0).ceil() as u32).div_ceil(2);
        graph.add_edge(n1, n2, (distance, lane));
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
    let share_graph = std::sync::Arc::new(graph.clone());
    let split_len = plans.len().div_ceil(THREAD_COUNT);
    let mut threads = vec![];

    for thread in 0..THREAD_COUNT {
        let indicator = indicator.clone();
        let graph = share_graph.clone();
        let plans = plans
            .iter()
            .skip(split_len * thread)
            .take(split_len)
            .cloned()
            .collect::<Vec<_>>();

        let thread = std::thread::spawn(move || {
            plans
                .into_iter()
                .filter_map(|plan| {
                    indicator.inc(1);

                    petgraph::algo::astar(
                        graph.as_ref(),
                        plan.0,
                        |n| n == plan.1,
                        |e| e.weight().0 / e.weight().1 as f64,
                        |_| 0.0,
                    )
                })
                .map(|path| {
                    path.1
                        .into_iter()
                        .map(|n| n.index() as u32)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        });
        threads.push(thread);
    }

    let paths = threads
        .into_iter()
        .flat_map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();

    indicator.finish();
    println!("[path stats] paths: {}", paths.len());

    let bytes = postcard::to_extend(&(graph, paths), vec![]).unwrap();
    std::fs::write("path.bin", bytes).unwrap();
}
