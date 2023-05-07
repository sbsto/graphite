use std::{string::FromUtf8Error, sync::Arc, thread::Thread};
use rocksdb::{TransactionDB, Options, Error as RocksError, ColumnFamilyDescriptor, TransactionDBOptions, DB, MultiThreaded};
use serde_json::Error as SerdeError;

pub use serde::{Serialize, Deserialize};
pub use xid;

pub trait IceNode: Serialize + for<'de> Deserialize<'de> + Clone + Send {
	fn id(&self) -> &str;
	fn nbs(&self) -> &Vec<String>;
	fn nbs_mut(&mut self) -> &mut Vec<String>;
	fn family_name(&self) -> Option<String>;
}

#[macro_export]
macro_rules! create_node_struct {
	($struct_name:ident {
		$($field_name:ident: $field_type:ty),* $(,)?
	}) => {
		#[derive(Debug, Serialize, Deserialize, Clone)]
		pub struct $struct_name {
			id: String,
			nbs: Vec<String>,
			$($field_name: $field_type),*,
		}

		impl $struct_name {
			pub fn new(id: Option<String>, $($field_name: $field_type,)*) -> Self {
				Self {
					id: format!(concat!(stringify!($struct_name), ":{}"), id.unwrap_or_else(|| xid::new().to_string())),
					nbs: Vec::new(),
					$($field_name),*,
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

			fn family_name(&self) -> Option<String> {
				Some(stringify!($struct_name).to_string())
			}
		}
	};
}

pub struct Graph {
  db: Arc<TransactionDB<MultiThreaded>>,
	path: String,
}

pub trait GraphOperations {
	fn create_node_family(&mut self, family_name: &str) -> Result<(), GraphError>;
}

impl GraphOperations for Graph {
	fn create_node_family(&mut self, family_name: &str) -> Result<(), GraphError> {
		// Check if the column family already exists
		if self.db.cf_handle(family_name).is_none() {
			let options = Options::default();
			self.db
				.create_cf(family_name, &options)
				.map_err(|_| GraphError::NodeFamilyError)?;
		}

		Ok(())
	}
}

#[derive(Debug)]
pub enum GraphError {
	SerdeError(SerdeError),
	OpenError,
	FindKeyError,
	RocksError(RocksError),
	NeighbourIndexError,
	ParseUtf8Error(FromUtf8Error),
	NodeFamilyError,
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
			GraphError::NodeFamilyError => write!(f, "Error accessing node family"),
		}
	}
}

impl Graph {
	pub fn new(path: &str) -> Result<Graph, GraphError> {
    let mut options = Options::default();
    let txn_db_options = TransactionDBOptions::default();

		options.increase_parallelism(1);

    let cfs = match DB::list_cf(&options, path) {
			Ok(cfs) => cfs,
			Err(_) => Vec::new(), // If there are no existing column families
    };

    let mut cf_descriptors = Vec::new();
    for cf in cfs {
        cf_descriptors.push(ColumnFamilyDescriptor::new(cf, Options::default()));
    }

    let db: TransactionDB<MultiThreaded> = match cf_descriptors.is_empty() {
        true => TransactionDB::open(&options, &txn_db_options, path)?,
        false => TransactionDB::open_cf_descriptors(&options, &txn_db_options, path, cf_descriptors)?,
    };

    let path = path.to_string();

    Ok(Graph { db: Arc::new(db), path })
	}

	pub fn add_node<T>(&self, node: T) -> Result<T, GraphError>
	where
		T: IceNode + Send + 'static,
	{
    let db = Arc::clone(&self.db);
    let node_family_name = node.family_name().ok_or(GraphError::NodeFamilyError)?;
		if db.cf_handle(&node_family_name).is_none() {
			let options = Options::default();
			db
				.create_cf(&node_family_name, &options)
				.map_err(|_| GraphError::NodeFamilyError)?;
		}

		let node_family = db
			.cf_handle(&node_family_name)
			.ok_or(GraphError::NodeFamilyError)?;

		let txn: rocksdb::Transaction<TransactionDB<MultiThreaded>> = db.transaction();
		txn.put_cf(&node_family, format!("{}", node.id()), serde_json::to_string(&node)?)?;
		txn.commit()?;
		Ok(node)
	}

	pub fn get_node<T>(&self, node_id: &str) -> Result<T, GraphError> 
	where T: IceNode {
		let node_family_name = node_id.split(":").next().ok_or(GraphError::NodeFamilyError)?;
		let node_family = self.db.cf_handle(&node_family_name).ok_or(GraphError::NodeFamilyError)?;
    let value = self.db.get_cf(&node_family, node_id)?;

    match value {
			Some(value) => {
				let node_payload = serde_json::from_slice::<T>(&value)?;
				Ok(node_payload)
			}
			None => Err(GraphError::FindKeyError)
    }
	}

	pub fn remove_node(&self, node_id: &str) -> Result<(), GraphError> {
		let node_family_name = node_id.split(":").next().ok_or(GraphError::NodeFamilyError)?;
		let node_family = self.db.cf_handle(&node_family_name).ok_or(GraphError::NodeFamilyError)?;

		let txn = self.db.transaction();
		txn.delete_cf(&node_family, &node_id)?;
		txn.commit()?;
		Ok(())
	}

	pub fn save_node<T: IceNode + Serialize>(&self, node: &T) -> Result<(), GraphError> {
    let key = format!("{}", node.id());
		let node_family = node.family_name().ok_or(GraphError::NodeFamilyError)?;
		let node_family = self.db.cf_handle(&node_family).ok_or(GraphError::NodeFamilyError)?;

    let serialized_node = serde_json::to_vec(node)?;
    self.db.put_cf(&node_family, &key, &serialized_node)?;
    Ok(())
	}

	pub fn update_node<T: IceNode + Serialize>(&self, node: &T) -> Result<(), GraphError> {
    self.save_node(node)
	}

	pub fn add_edge<T>(&self, from_node_id: &str, to_node_id: &str) -> Result<(), GraphError> 
	where T: IceNode {
		let node_family_name = from_node_id.split(":").next().ok_or(GraphError::NodeFamilyError)?;
		let node_family = self.db.cf_handle(&node_family_name).ok_or(GraphError::NodeFamilyError)?;

		let node_payload: Result<T, GraphError> = match self.db.get_cf(&node_family, &from_node_id) {
			Ok(Some(value)) => {
				let mut node_payload = serde_json::from_slice::<T>(&value)?;
				node_payload.nbs_mut().push(to_node_id.to_string());
				Ok(node_payload)
			}
			Ok(None) => Err(GraphError::FindKeyError),
			Err(e) => Err(GraphError::RocksError(e)),
		};

		let txn = self.db.transaction();
		txn.put_cf(&node_family, from_node_id, serde_json::to_vec(&node_payload?)?)?;
		txn.commit()?;
		Ok(())
	}

	pub fn remove_edge<T>(&self, from_node_id: &str, to_node_id: &str) -> Result<(), GraphError>
	where T: IceNode {
		let node_family_name = from_node_id.split(":").next().ok_or(GraphError::NodeFamilyError)?;
		let node_family = self.db.cf_handle(&node_family_name).ok_or(GraphError::NodeFamilyError)?;

		let node_payload: Result<T, GraphError> = match self.db.get_cf(&node_family, &from_node_id) {
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
		txn.put_cf(&node_family, &from_node_id, serde_json::to_vec(&node_payload?)?)?;
		txn.commit()?;
		Ok(())
	}

	pub fn get_adjacents<T>(&self, node_id: &str) -> Result<Vec<String>, GraphError>
	where T: IceNode {
		let node_family_name = node_id.split(":").next().ok_or(GraphError::NodeFamilyError)?;
		let node_family = self.db.cf_handle(&node_family_name).ok_or(GraphError::NodeFamilyError)?;

		let node_payload: Result<T, GraphError> = match self.db.get_cf(&node_family, &node_id) {
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
		let node_families = DB::list_cf(&Options::default(), &self.path)?;

		for node_family_name in node_families {
			if node_family_name == "default" {
				continue;
			}

			let node_family = self.db.cf_handle(&node_family_name).ok_or(GraphError::NodeFamilyError)?;
			let records = self.db.iterator_cf(&node_family, rocksdb::IteratorMode::Start);

			for record in records {
				match record {
					Ok((key, _)) => {
						self.db.delete_cf(&node_family, &key)?;
					}
					Err(_) => return Err(GraphError::FindKeyError),
				}
			}

			// Drop the column family
			self.db.drop_cf(&node_family_name)?;
		}

    let _ = DB::destroy(&Options::default(), self.path.clone())?;
		Ok(())
	}

	pub fn display(&self) -> Result<(), GraphError> {
    let node_families = DB::list_cf(&Options::default(), &self.path)?;
    for node_family_name in node_families {
			let node_family = self
				.db
				.cf_handle(&node_family_name)
				.ok_or(GraphError::NodeFamilyError)?;

			let records = self.db.iterator_cf(&node_family, rocksdb::IteratorMode::Start);

			println!("Node family: {}", node_family_name);

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
    }

    Ok(())
	}

	pub fn count_nodes(&self) -> Result<usize, GraphError> {
		let node_families = DB::list_cf(&Options::default(), &self.path)?;
		let mut count = 0;

		for node_family_name in node_families {
			let node_family = self
				.db
				.cf_handle(&node_family_name)
				.ok_or(GraphError::NodeFamilyError)?;

			let records = self.db.iterator_cf(&node_family, rocksdb::IteratorMode::Start);

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