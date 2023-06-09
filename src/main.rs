use graph::{
    AnotherNodeType, AnotherNodeTypeId, Edge, Graph, Node, NodeId, SomeEdgeType,
    SomeEdgeTypeConnection, SomeNodeType, SomeNodeTypeId,
};
use rayon::prelude::*;

fn main() {
    let graph = Graph::new("/var/lib/rocksdb/dev").unwrap();

    let start = std::time::Instant::now();

    (0..1000000).into_par_iter().for_each(|_| {
        let node1_initial = SomeNodeType::new(
            None,
            "some data".to_string(),
            "some mutable data".to_string(),
        );

        let node2_initial = AnotherNodeType::new(None, "some data".to_string(), false);

        let connection = SomeEdgeTypeConnections::FirstConnectionType(
            SomeNodeTypeId::new(Some(node1_initial.id().to_string())),
            AnotherNodeTypeId::new(Some(node2_initial.id().to_string())),
        );

        let edge = SomeEdgeType::new(None, connection, 0.5);

        let node_1 = graph.add_node(node1_initial).unwrap();
        let node_2 = graph.add_node(node2_initial).unwrap();

        graph.add_edge(edge, node_1, node_2).unwrap();
    });

    println!("time to add 2m nodes and 1m edges: {:?}", start.elapsed());

    // let nodes_num = graph.count_nodes().unwrap();
    // println!("nodes: {}", nodes_num);
}
