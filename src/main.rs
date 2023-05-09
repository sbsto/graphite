use icy::{create_node_struct, create_edge_struct, Graph, IceNode, Serialize, Deserialize, xid, IceEdge};
use rayon::prelude::*;

fn main() {
	let graph = Graph::new("/var/lib/rocksdb/dev").unwrap();

	create_node_struct!(graph, 
		SomeNodeType {
			data: String,
			mutable_data: String,
		}
	);

	create_node_struct!(graph,
		AnotherNodeType {
			data: String,
		}
	);

	create_edge_struct!(graph,
		SomeEdgeType {
			weight: u16,
		}
	);

	// create 100k of each node type and add them to the graph and time it

	
	let start = std::time::Instant::now();
	
	graph.destroy_everything().unwrap();

	// let _ = (0..1_000_000).into_par_iter()
  //   .for_each(|_| {
	// 		let node1_initial = SomeNodeType::new(None, "some data".to_string(), "some mutable data".to_string());
	// 		let node2_initial = AnotherNodeType::new(None, "some data".to_string());

	// 		let edge = SomeEdgeType::new_between(None, node1_initial.id(), node2_initial.id(), 1);

	// 		let node_1 = graph.add_node(node1_initial).unwrap();
	// 		let node_2 = graph.add_node(node2_initial).unwrap();

	// 		graph.add_edge(edge, node_1, node_2).unwrap();
	// });

	println!("time to add 2m nodes and 1m edges: {:?}", start.elapsed());

	let nodes_num = graph.count_nodes().unwrap();
	println!("nodes: {}", nodes_num);

	graph.display().unwrap();

	// graph.display().unwrap();
}
