use icy::{create_node_struct, Graph, IceNode, Serialize, Deserialize, xid};
use rayon::prelude::*;

fn main() {
	let graph = Graph::new("/var/lib/rocksdb/dev").unwrap();

	create_node_struct! {
		SomeNodeType {
			data: String,
			mutable_data: String,
		}
	} 

	create_node_struct! {
		AnotherNodeType {
			data: String,
		}
	} 

	// create 100k of each node type and add them to the graph and time it
	let start = std::time::Instant::now();

	// graph.destroy_everything().unwrap();

	let _ = (0..100_000).into_par_iter()
    .for_each(|_| {
			let node1 = SomeNodeType::new(None, "some data".to_string(), "some mutable data".to_string());
			let node2 = AnotherNodeType::new(None, "some data".to_string());
			graph.add_node(node1).unwrap();
			graph.add_node(node2).unwrap();
    });

	println!("time to add 200k nodes: {:?}", start.elapsed());

	let nodes_num = graph.count_nodes().unwrap();
	println!("nodes number: {}", nodes_num);
	
	println!("time to add 200k nodes and count them: {:?}", start.elapsed());

	// graph.display().unwrap();
}
