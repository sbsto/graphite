pub mod edge;
pub mod node;

use std::{string::FromUtf8Error, sync::Arc};
use rocksdb::{TransactionDB, Options, Error as RocksError, ColumnFamilyDescriptor, TransactionDBOptions, DB, MultiThreaded, Transaction};
use serde_json::Error as SerdeError;

pub use node::Node;
pub use edge::Edge;
pub use serde::{Serialize, Deserialize};
pub use xid;

pub struct Graph {
  db: Arc<TransactionDB<MultiThreaded>>,
	path: String,
}

// pub trait GraphOperations {
// 	fn create_family(&mut self, family_name: &str) -> Result<(), GraphError>;
// }

// impl GraphOperations for Graph {
// 	fn create_family(&mut self, family_name: &str) -> Result<(), GraphError> {
// 		// Check if the column family already exists
// 		if self.db.cf_handle(family_name).is_none() {
// 			let options = Options::default();
// 			self.db
// 				.create_cf(family_name, &options)
// 				.map_err(|e| GraphError::CreateFamilyError(e))?;
// 		}
// 		Ok(())
// 	}
// }

#[derive(Debug)]
pub enum GraphError {
	SerdeError(SerdeError),
	OpenDbError(RocksError),
	DestroyDbError(RocksError),
	CreateNodeError(RocksError),
	ReadNodeError(RocksError),
	DeleteNodeError(RocksError),
	UpdateNodeError(RocksError),
	CreateEdgeError(RocksError),
	DeleteError(RocksError),
	FindFamiliesError(RocksError),
	DbNotClosed,
	FindKeyError,
	NeighbourIndexError,
	ParseUtf8Error(FromUtf8Error),
	NodeFamilyError,
	CreateFamilyError(RocksError),
	FindFamilyError,
	ParseNodeIdError,
	EdgeFamilyError,
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

impl std::fmt::Display for GraphError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			GraphError::SerdeError(error) => write!(f, "Serde error: {}", error),
			GraphError::CreateNodeError(error) => write!(f, "Error creating node: {}", error),
			GraphError::ReadNodeError(error) => write!(f, "Error reading node: {}", error),
			GraphError::UpdateNodeError(error) => write!(f, "Error updating node: {}", error),
			GraphError::DeleteNodeError(error) => write!(f, "Error deleting node: {}", error),
			GraphError::CreateEdgeError(error) => write!(f, "Error creating edge: {}", error),
			GraphError::DeleteError(error) => write!(f, "Error deleting: {}", error),
			GraphError::FindKeyError => write!(f, "Find key error"),
			GraphError::OpenDbError(error) => write!(f, "Error opening database: {}", error),
			GraphError::DestroyDbError(error) => write!(f, "Error destroying database: {}", error),
			GraphError::NeighbourIndexError => write!(f, "Neighbour index error"),
			GraphError::ParseUtf8Error(error) => write!(f, "Parse UTF8 error: {}", error),
			GraphError::NodeFamilyError => write!(f, "Error accessing node family"),
			GraphError::CreateFamilyError(error) => write!(f, "Error creating family: {}", error),
			GraphError::FindFamilyError => write!(f, "Error finding node family"),
			GraphError::ParseNodeIdError => write!(f, "Error parsing node id"),
			GraphError::EdgeFamilyError => write!(f, "Error accessing edge family"),
			GraphError::FindFamiliesError(error) => write!(f, "Error finding families: {}", error),
			GraphError::DbNotClosed => write!(f, "Tried to destroy database while it was still open"),
		}
	}
}

impl Graph {
	pub fn new(path: &str) -> Result<Graph, GraphError> {
    let options = Options::default();
    let txn_db_options = TransactionDBOptions::default();

    let cfs = match DB::list_cf(&options, path) {
			Ok(cfs) => cfs,
			Err(_) => Vec::new(), // If there are no existing column families
    };

    let mut cf_descriptors = Vec::new();
    for cf in cfs {
        cf_descriptors.push(ColumnFamilyDescriptor::new(cf, Options::default()));
    }

    let db: TransactionDB<MultiThreaded> = match cf_descriptors.is_empty() {
			true => TransactionDB::open(&options, &txn_db_options, path).map_err(|e| GraphError::OpenDbError(e))?,
			false => TransactionDB::open_cf_descriptors(&options, &txn_db_options, path, cf_descriptors).map_err(|e| GraphError::OpenDbError(e))?,
    };

    let path = path.to_string();

    Ok(Graph { db: Arc::new(db), path })
	}

	pub fn add_node<T>(&self, node: T) -> Result<T, GraphError>
	where
		T: Node
	{
    let db = Arc::clone(&self.db);
    let node_family_name = node.family_name();

		let node_family = db
			.cf_handle(&node_family_name)
			.ok_or(GraphError::FindFamilyError)?;

		let txn: Transaction<TransactionDB<MultiThreaded>> = db.transaction();
		txn.put_cf(&node_family, node.id(), serde_json::to_string(&node)?)
			.map_err(|e| GraphError::CreateNodeError(e))?;
		txn.commit().map_err(|e| GraphError::CreateNodeError(e))?;
		Ok(node)
	}

	pub fn get_node<T>(&self, node_id: &str) -> Result<T, GraphError> 
	where T: Node {
    let db = Arc::clone(&self.db);
		let node_family_name = node_id.split(":").next().ok_or(GraphError::ParseNodeIdError)?;
		let node_family = db.cf_handle(&node_family_name).ok_or(GraphError::FindFamilyError)?;
    let value = db.get_cf(&node_family, node_id)
			.map_err(|e| GraphError::ReadNodeError(e))?;

    match value {
			Some(value) => {
				let node_payload = serde_json::from_slice::<T>(&value)?;
				Ok(node_payload)
			}
			None => Err(GraphError::FindKeyError)
    }
	}

	pub fn remove_node(&self, node_id: &str) -> Result<(), GraphError> {
		let db = Arc::clone(&self.db);
		let node_family_name = node_id.split(":").next().ok_or(GraphError::ParseNodeIdError)?;
		let node_family = db.cf_handle(&node_family_name).ok_or(GraphError::FindFamilyError)?;

		let txn = db.transaction();
		txn.delete_cf(&node_family, &node_id).map_err(|e| GraphError::DeleteNodeError(e))?;
		txn.commit().map_err(|e| GraphError::DeleteNodeError(e))?;
		Ok(())
	}

	pub fn update_node<T: Node>(&self, node: &T) -> Result<(), GraphError> {
		let db = Arc::clone(&self.db);
		let node_family = node.family_name();
		let node_family = db.cf_handle(&node_family).ok_or(GraphError::FindFamilyError)?;

    let serialized_node = serde_json::to_vec(node)?;
    self.db.put_cf(&node_family, &node.id(), &serialized_node).map_err(|e| GraphError::UpdateNodeError(e))?;
    Ok(())
	}

	pub fn add_edge<T, S, R>(&self, edge: T, mut from_node: S, mut to_node: R) -> Result<(), GraphError> 
	where T: Edge, S: Node, R: Node {
    let db = Arc::clone(&self.db);
    let edge_family_name = edge.family_name();
		let edge_family = self.db.cf_handle(&edge_family_name).ok_or(GraphError::EdgeFamilyError)?;

		let txn = db.transaction();
		txn.put_cf(&edge_family, edge.id(), serde_json::to_vec(&edge)?)
			.map_err(|e| GraphError::CreateEdgeError(e))?;

		from_node.add_out_edge_id(edge.id().to_string());
		to_node.add_in_edge_id(edge.id().to_string());

		self.update_node(&from_node)?;
		self.update_node(&to_node)?;

		txn.commit()
			.map_err(|e| GraphError::CreateEdgeError(e))?;
		Ok(())
	}

	// pub fn remove_edge<T>(&self, from_node_id: &str, to_node_id: &str) -> Result<(), GraphError>
	// where T: IceNode {
	// 	let node_family_name = from_node_id.split(":").next().ok_or(GraphError::ParseNodeIdError)?;
	// 	let node_family = self.db.cf_handle(&node_family_name).ok_or(GraphError::FindFamilyError)?;

	// 	let node_payload: Result<T, GraphError> = match self.db.get_cf(&node_family, &from_node_id) {
	// 			Ok(Some(value)) => {
	// 				let mut node_payload = serde_json::from_slice::<T>(&value)?;
	// 				let index = node_payload.nbs()
	// 					.iter()
	// 					.position(|x| *x == to_node_id.to_string()).ok_or(GraphError::NeighbourIndexError)?;

	// 				node_payload.nbs_mut().remove(index);
	// 				Ok(node_payload)
	// 			}
	// 			Ok(None) => Err(GraphError::FindKeyError),
	// 			Err(e) => Err(GraphError::RocksError(e)),
	// 	};

	// 	let txn = self.db.transaction();
	// 	txn.put_cf(&node_family, &from_node_id, serde_json::to_vec(&node_payload?)?)?;
	// 	txn.commit()?;
	// 	Ok(())
	// }

	// pub fn get_adjacents<T>(&self, node_id: &str) -> Result<Vec<String>, GraphError>
	// where T: IceNode {
	// 	let node_family_name = node_id.split(":").next().ok_or(GraphError::NodeFamilyError)?;
	// 	let node_family = self.db.cf_handle(&node_family_name).ok_or(GraphError::NodeFamilyError)?;

	// 	let node_payload: Result<T, GraphError> = match self.db.get_cf(&node_family, &node_id) {
	// 			Ok(Some(value)) => {
	// 				let node_payload = serde_json::from_slice::<T>(&value)?;
	// 				Ok(node_payload)
	// 			}
	// 			Ok(None) => Err(GraphError::FindKeyError),
	// 			Err(_) => Err(GraphError::FindKeyError),
	// 	};
	// 	Ok(node_payload?.nbs().to_vec())
	// }

	pub fn create_family_if_not_exists(&self, family_name: &str) -> Result<(), GraphError> {
		let db = &self.db;
		if db.cf_handle(family_name).is_none() {
				let options = Options::default();
				db.create_cf(family_name, &options)
						.map_err(|e| GraphError::CreateFamilyError(e))?;
		}
		Ok(())
	}

	pub fn destroy_everything(&self) -> Result<(), GraphError> {
		let families = DB::list_cf(&Options::default(), &self.path)
			.map_err(|e| GraphError::FindFamiliesError(e))?;

		for family_name in families {
			if family_name != "default" {
				self.db.drop_cf(&family_name).map_err(|e| GraphError::DeleteError(e))?;
			}
		}

    Ok(())
	}

	pub fn display(&self) -> Result<(), GraphError> {
    let node_families = DB::list_cf(&Options::default(), &self.path)
			.map_err(|e| GraphError::FindFamiliesError(e))?;
    for node_family_name in node_families {
			let node_family = self
				.db
				.cf_handle(&node_family_name)
				.ok_or(GraphError::NodeFamilyError)?;

			let records = self.db.iterator_cf(&node_family, rocksdb::IteratorMode::Start);

			println!("Node family: {}", node_family_name);

			for record in records.take(5) {
				match record {
						Ok((key, value)) => {
							let key_str = String::from_utf8(key.to_vec()).map_err(GraphError::ParseUtf8Error)?;
							let value_str = String::from_utf8(value.to_vec()).map_err(GraphError::ParseUtf8Error)?;
							println!("{}: {}", key_str, value_str);
						}
						Err(_) => return Err(GraphError::FindKeyError),
				}
			}
    }

    Ok(())
	}

	pub fn count_nodes(&self) -> Result<usize, GraphError> {
		let families = DB::list_cf(&Options::default(), &self.path)
			.map_err(|e| GraphError::FindFamiliesError(e))?;
		let mut count = 0;

		for family_name in families {
			let family = self
				.db
				.cf_handle(&family_name)
				.ok_or(GraphError::NodeFamilyError)?;

			let records = self.db.iterator_cf(&family, rocksdb::IteratorMode::Start);
			for record in records {
				match record {
					Ok(_) => count += 1,
					Err(_) => return Err(GraphError::FindKeyError),
				}
			}
		}

		Ok(count)
	}
}