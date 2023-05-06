use icy::{create_node_struct, Graph, IceNode, Serialize, Deserialize, xid};


fn main() {
	let mut graph = Graph::new("/var/lib/rocksdb/dev").unwrap();

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

	graph.display().unwrap();
}
