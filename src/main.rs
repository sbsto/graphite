use std::string::FromUtf8Error;

use rocksdb::{TransactionDB, Error as RocksError, Options, DB};
use serde::{Serialize, Deserialize};
use serde_json::{Error as SerdeError};
use xid;

pub trait IceNode: Serialize + for<'de> Deserialize<'de> + Clone {
	fn id(&self) -> &str;
	fn nbs(&self) -> &Vec<String>;
	fn nbs_mut(&mut self) -> &mut Vec<String>;
}

macro_rules! create_node_type {
	($struct_name:ident { $($field_name:ident: $field_type:ty),* $(,)? }) => {
		#[derive(Debug, Serialize, Deserialize, Clone)]
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

    	fn nbs_mut(&mut self) -> &mut Vec<String> {
    	    &mut self.nbs
    	}
		}
	};
}


pub struct Graph {
  db: TransactionDB,
	path: String
}

#[derive(Debug)]
pub enum GraphError {
	SerdeError(SerdeError),
	OpenError,
	FindKeyError,
	RocksError(RocksError),
	NeighbourIndexError,
	ParseUtf8Error(FromUtf8Error),
}

impl From<SerdeError> for GraphError {
	fn from(error: SerdeError) -> Self {
			GraphError::SerdeError(error)
	}
}

impl From<FromUtf8Error> for GraphError {
	fn from(error: FromUtf8Error) -> Self {
			GraphError::ParseUtf8Error(error)
	}
}

impl From<RocksError> for GraphError {
	fn from(error: RocksError) -> Self {
			GraphError::RocksError(error)
	}
}

impl std::fmt::Display for GraphError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			match self {
					GraphError::SerdeError(error) => write!(f, "Serde error: {}", error),
					GraphError::RocksError(error) => write!(f, "RocksDB error: {}", error),
					GraphError::FindKeyError => write!(f, "Find key error"),
					GraphError::OpenError => write!(f, "Error opening database"),
					GraphError::NeighbourIndexError => write!(f, "Neighbour index error"),
					GraphError::ParseUtf8Error(error) => write!(f, "Parse UTF8 error: {}", error),
			}
	}
}

impl Graph {
	pub fn new(path: &str) -> Result<Graph, GraphError> {
		let db = TransactionDB::open_default(path)?;
		let path = path.to_string();
		Ok(Graph { db, path })
	}

	pub fn add_node<T>(&self, node: T) -> Result<T, GraphError> where T: IceNode + Serialize {
		let txn: rocksdb::Transaction<TransactionDB> = self.db.transaction();
		txn.put(format!("{}", node.id()), serde_json::to_string(&node)?)?;
		txn.commit()?;
		Ok(node)
	}

	pub fn get_node<T>(&self, node_id: &str) -> Result<T, GraphError> 
	where T: IceNode {
    let value = self.db.get(&node_id)?;

    match value {
			Some(value) => {
				let node_payload = serde_json::from_slice::<T>(&value)?;
				Ok(node_payload)
			}
			None => Err(GraphError::FindKeyError)
    }
	}

	pub fn add_edge<T>(&self, from_node_id: &str, to_node_id: &str) -> Result<(), GraphError> 
	where T: IceNode {
		let node_payload: Result<T, GraphError> = match self.db.get(&from_node_id) {
				Ok(Some(value)) => {
					let mut node_payload = serde_json::from_slice::<T>(&value)?;
					node_payload.nbs_mut().push(to_node_id.to_string());
					Ok(node_payload)
				}
				Ok(None) => Err(GraphError::FindKeyError),
				Err(e) => Err(GraphError::RocksError(e)),
		};

		let txn = self.db.transaction();
		txn.put(from_node_id, serde_json::to_vec(&node_payload?)?)?;
		txn.commit()?;
		Ok(())
	}

	pub fn remove_edge<T>(&self, from_node_id: &str, to_node_id: &str) -> Result<(), GraphError>
	where T: IceNode {
		let key = format!("node:{}", from_node_id);
		let node_payload: Result<T, GraphError> = match self.db.get(&key) {
				Ok(Some(value)) => {
					let mut node_payload = serde_json::from_slice::<T>(&value)?;
					let index = node_payload.nbs()
						.iter()
						.position(|x| *x == to_node_id.to_string()).ok_or(GraphError::NeighbourIndexError)?;

					node_payload.nbs_mut().remove(index);
					Ok(node_payload)
				}
				Ok(None) => Err(GraphError::FindKeyError),
				Err(e) => Err(GraphError::RocksError(e)),
		};

		let txn = self.db.transaction();
		txn.put(&key, serde_json::to_vec(&node_payload?)?)?;
		txn.commit()?;
		Ok(())
	}

	pub fn get_adjacents<T>(&self, node_id: &str) -> Result<Vec<String>, GraphError>
	where T: IceNode {
		let key = format!("node:{}", node_id);
		let node_payload: Result<T, GraphError> = match self.db.get(&key) {
				Ok(Some(value)) => {
					let node_payload = serde_json::from_slice::<T>(&value)?;
					Ok(node_payload)
				}
				Ok(None) => Err(GraphError::FindKeyError),
				Err(_) => Err(GraphError::FindKeyError),
		};
		Ok(node_payload?.nbs().to_vec())
	}

	pub fn destroy_everything(&self) -> Result<(), GraphError> {
		let records = self.db.iterator(rocksdb::IteratorMode::Start);
		for record in records {
			match record {
				Ok((key, _)) => {
					self.db.delete(&key)?;
				}
				Err(_) => return Err(GraphError::FindKeyError),
			}
		}

    let _ = DB::destroy(&Options::default(), self.path.clone())?;
		Ok(())
	}

	pub fn display(&self) -> Result<(), GraphError> {
    let records = self.db.iterator(rocksdb::IteratorMode::Start);
    for record in records {
			match record {
				Ok((key, value)) => {
						let key_str = String::from_utf8(key.to_vec()).map_err(GraphError::ParseUtf8Error)?;
						let value_str = String::from_utf8(value.to_vec()).map_err(GraphError::ParseUtf8Error)?;
						println!("{}: {}", key_str, value_str);
				}
				Err(_) => return Err(GraphError::FindKeyError),
			}
    }
    Ok(())
	}
}

fn main() {
	let graph = Graph::new("/var/lib/rocksdb/dev").unwrap();

	create_node_type! {
    SomeNodeType {
			some_metadata: String,
    }
	}

	// let node = SomeNodeType::new(None, "here's some data".to_string());
	// let node2 = SomeNodeType::new(None, "here's some data".to_string());
	// graph.add_node(node);
	// graph.add_node(node2);
	graph.display().unwrap();
	
}
