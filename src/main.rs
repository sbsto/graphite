use std::any::type_name;
use rocksdb::{TransactionDB, IteratorMode, Error as RocksError};
use serde::{Serialize, Deserialize};
use serde_json::{Error as SerdeError};
use xid;

pub trait IceNode {
	fn id(&self) -> &str;
  fn nbs(&self) -> &Vec<String>;
}

macro_rules! create_node_type {
	($struct_name:ident { $($field_name:ident: $field_type:ty),* $(,)? }) => {
			#[derive(Debug, Serialize, Deserialize)]
			pub struct $struct_name {
					id: String,
					nbs: Vec<String>,
					$($field_name: $field_type),*
			}

			impl $struct_name {
					pub fn new(id: Option<String>, $($field_name: $field_type,)*) -> Self {
							Self {
									id: format!(concat!(stringify!($struct_name), ":{}"), id.unwrap_or_else(|| xid::new().to_string())),
									nbs: Vec::new(),
									$($field_name),*
							}
					}
			}

			impl std::str::FromStr for $struct_name {
					type Err = serde_json::Error;

					fn from_str(s: &str) -> Result<Self, Self::Err> {
							serde_json::from_str::<Self>(s)
					}
			}

			impl IceNode for $struct_name {
				fn id(&self) -> &str {
					&self.id
				}

				fn nbs(&self) -> &Vec<String> {
					&self.nbs
				}
			}
	};
}


pub struct Graph {
  db: TransactionDB,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NodePayload {
	a: Vec<String>,
}

pub enum GraphError {
	SerdeError(SerdeError),
	RocksError
}

impl From<SerdeError> for GraphError {
	fn from(error: SerdeError) -> Self {
			GraphError::SerdeError(error)
	}
}

impl From<RocksError> for GraphError {
	fn from(error: RocksError) -> Self {
			GraphError::RocksError
	}
}

impl std::fmt::Display for GraphError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			match self {
					GraphError::SerdeError(error) => write!(f, "Serde error: {}", error),
					GraphError::RocksError => write!(f, "RocksDB error"),
			}
	}
}

impl Graph {
	pub fn new(path: &str) -> Graph {
    let db = TransactionDB::open_default( path).unwrap();
    Graph { db }
	}

	pub fn add_node<T>(&self, node: T) -> Result<T, SerdeError> where T: IceNode + Serialize {
		let txn: rocksdb::Transaction<TransactionDB> = self.db.transaction();
		txn.put(stringify!(node.id), serde_json::to_string(&node)?).unwrap();
		txn.commit().unwrap();
		Ok(node)
	}

	pub fn get_node<'a, T>(&self, node_id: &'a str) -> Result<T, GraphError> where T: IceNode + Deserialize<'a> {
    let value = self.db.get(&node_id)?;

    match value {
			Some(value) => {
				let node_payload = serde_json::from_slice::<'a, T>(&value)?;
				Ok(node_payload)
			}
			None => Err(GraphError::RocksError)
    }
	}

	pub fn add_edge(&self, from_node_id: &str, to_node_id: &str) {
		let key = format!("node:{}", from_node_id);
		let node_payload = match self.db.get(&key) {
				Ok(Some(value)) => {
					let mut node_payload = serde_json::from_slice::<NodePayload>(&value).unwrap();
					node_payload.a.push(to_node_id.to_string());
					node_payload
				}
				Ok(None) => {
					// let mut node_payload: NodePayload = self.add_node(from_node_id);
					// node_payload.a.push(to_node_id.to_string());
					// node_payload
					panic!("Error");
				}
				Err(e) => panic!("Error reading adjacency list: {}", e),
		};

		let txn = self.db.transaction();
		txn.put(&key, serde_json::to_vec(&node_payload).unwrap()).unwrap();
		txn.commit().unwrap();
	}

	pub fn remove_edge(&self, from_node_id: &str, to_node_id: &str) {
		let key = format!("node:{}", from_node_id);
		let node_payload: NodePayload = match self.db.get(&key) {
				Ok(Some(value)) => {
					let mut node_payload = serde_json::from_slice::<NodePayload>(&value).unwrap();
					let index = node_payload.a.iter().position(|x| *x == to_node_id.to_string()).unwrap();
					node_payload.a.remove(index);
					node_payload
				}
				Ok(None) => {
					panic!("Node not found");
				}
				Err(e) => panic!("Error reading adjacency list: {}", e),
		};

		let txn = self.db.transaction();
		txn.put(&key, serde_json::to_vec(&node_payload).unwrap()).unwrap();
		txn.commit().unwrap();
	}

	pub fn get_adjacents(&self, node_id: &str) -> Vec<String> {
		let key = format!("node:{}", node_id);
		let node_payload = match self.db.get(&key) {
				Ok(Some(value)) => {
					let node_payload: NodePayload = serde_json::from_slice::<NodePayload>(&value).unwrap();
					node_payload
				}
				Ok(None) => {
					panic!("Node not found");
				}
				Err(e) => panic!("Error reading adjacency list: {}", e),
		};
		node_payload.a
	}
}

fn main() {
	let graph = Graph::new("/var/lib/rocksdb/dev");

	// graph.add_node("A");
	// graph.add_node("B");

	create_node_type! {
    SomeNodeType {
			some_metadata: String,
    }
	}

	let node = SomeNodeType::new(None, "here's some data".to_string());
	println!("{:?}", node);

	// graph.add_node(node);

	// graph.add_edge("A", "B");
	// graph.add_edge("A", "C");
	// graph.add_edge("B", "C");
	// graph.add_edge("D", "A");
}
