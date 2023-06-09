pub mod generated;

use rocksdb::{
    ColumnFamilyDescriptor, Error as RocksError, MultiThreaded, Options, Transaction,
    TransactionDB, TransactionDBOptions, DB,
};

use rmp_serde::{decode::Error as DecodeError, encode::Error as EncodeError};
use std::{string::FromUtf8Error, sync::Arc};

pub use generated::*;
pub use serde::{Deserialize, Serialize};
pub use xid;

pub struct Graph {
    db: Arc<TransactionDB<MultiThreaded>>,
    path: String,
}

#[derive(Debug)]
pub enum GraphError {
    EncodeError(EncodeError),
    DecodeError(DecodeError),
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

impl From<EncodeError> for GraphError {
    fn from(error: EncodeError) -> Self {
        GraphError::EncodeError(error)
    }
}

impl From<DecodeError> for GraphError {
    fn from(error: DecodeError) -> Self {
        GraphError::DecodeError(error)
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
            GraphError::EncodeError(error) => write!(f, "Encoding error: {}", error),
            GraphError::DecodeError(error) => write!(f, "Decoding error: {}", error),
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
            GraphError::DbNotClosed => {
                write!(f, "Tried to destroy database while it was still open")
            }
        }
    }
}

impl Graph {
    pub fn new(path: &str) -> Result<Graph, GraphError> {
        let mut options = Options::default();
        options.create_if_missing(true);

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
            true => TransactionDB::open(&options, &txn_db_options, path)
                .map_err(GraphError::OpenDbError)?,
            false => {
                TransactionDB::open_cf_descriptors(&options, &txn_db_options, path, cf_descriptors)
                    .map_err(GraphError::OpenDbError)?
            }
        };

        let path = path.to_string();

        let graph = Graph {
            db: Arc::new(db),
            path,
        };

        let families = families();
        for family in families {
            graph.create_family_if_not_exists(family)?;
        }

        Ok(graph)
    }

    pub fn add_node<T>(&self, node: T) -> Result<T, GraphError>
    where
        T: Node,
    {
        let db = Arc::clone(&self.db);
        let node_family_name = node.family_name();

        let node_family = db
            .cf_handle(&node_family_name)
            .ok_or(GraphError::FindFamilyError)?;

        let txn: Transaction<TransactionDB<MultiThreaded>> = db.transaction();
        txn.put_cf(
            &node_family,
            node.id().to_string(),
            rmp_serde::to_vec(&node)?,
        )
        .map_err(GraphError::CreateNodeError)?;

        txn.commit().map_err(GraphError::CreateNodeError)?;
        Ok(node)
    }

    pub fn get_node<T>(&self, node_id: String) -> Result<T, GraphError>
    where
        T: Node,
    {
        let db = Arc::clone(&self.db);
        let node_family_name = node_id
            .split(':')
            .next()
            .ok_or(GraphError::ParseNodeIdError)?;
        let node_family = db
            .cf_handle(node_family_name)
            .ok_or(GraphError::FindFamilyError)?;
        let value = db
            .get_cf(&node_family, node_id)
            .map_err(GraphError::ReadNodeError)?;

        match value {
            Some(value) => {
                let node_payload = rmp_serde::from_slice::<T>(&value)?;
                Ok(node_payload)
            }
            None => Err(GraphError::FindKeyError),
        }
    }

    pub fn remove_node(&self, node_id: &str) -> Result<(), GraphError> {
        let db = Arc::clone(&self.db);
        let node_family_name = node_id
            .split(':')
            .next()
            .ok_or(GraphError::ParseNodeIdError)?;
        let node_family = db
            .cf_handle(node_family_name)
            .ok_or(GraphError::FindFamilyError)?;

        let txn = db.transaction();
        txn.delete_cf(&node_family, node_id)
            .map_err(GraphError::DeleteNodeError)?;
        txn.commit().map_err(GraphError::DeleteNodeError)?;
        Ok(())
    }

    pub fn update_node<T: Node>(&self, node: &T) -> Result<(), GraphError> {
        let db = Arc::clone(&self.db);
        let node_family = node.family_name();
        let node_family = db
            .cf_handle(&node_family)
            .ok_or(GraphError::FindFamilyError)?;

        let serialized_node = rmp_serde::to_vec(node)?;
        self.db
            .put_cf(&node_family, node.id().to_string(), serialized_node)
            .map_err(GraphError::UpdateNodeError)?;
        Ok(())
    }

    pub fn add_edge<T, S, R>(&self, edge: T) -> Result<(), GraphError>
    where
        T: Edge,
        S: Node,
        R: Node,
    {
        let db = Arc::clone(&self.db);
        let edge_family_name = edge.family_name();
        let edge_family = self
            .db
            .cf_handle(&edge_family_name)
            .ok_or(GraphError::EdgeFamilyError)?;

        let txn = db.transaction();
        txn.put_cf(
            &edge_family,
            edge.id().to_string(),
            rmp_serde::to_vec(&edge)?,
        )
        .map_err(GraphError::CreateEdgeError)?;

        let connection = edge.connection();
        from_node.add_out_connection(connection.clone());

        from_node.add_out_edge_id(edge.id().to_string());
        to_node.add_in_edge_id(edge.id().to_string());

        self.update_node(&from_node)?;
        self.update_node(&to_node)?;

        txn.commit().map_err(GraphError::CreateEdgeError)?;
        Ok(())
    }

    pub fn get_edge<T, R>(&self, edge_id: T) -> Result<R, GraphError>
    where
        T: EdgeId,
        R: Edge,
    {
        let db = Arc::clone(&self.db);
        let edge_family_name = edge_id.family_name();
        let edge_family = db
            .cf_handle(&edge_family_name)
            .ok_or(GraphError::EdgeFamilyError)?;

        let value = db
            .get_cf(&edge_family, edge_id.to_string())
            .map_err(GraphError::ReadNodeError)?;

        match value {
            Some(value) => {
                let edge_payload = rmp_serde::from_slice::<R>(&value)?;
                Ok(edge_payload)
            }
            None => Err(GraphError::FindKeyError),
        }
    }

    pub fn remove_edge<T, R>(self, edge_id: T) -> Result<(), GraphError>
    where
        T: EdgeId,
        R: Edge,
    {
        let db = Arc::clone(&self.db);
        let edge_family_name = edge_id.family_name();
        let edge_family = self
            .db
            .cf_handle(&edge_family_name)
            .ok_or(GraphError::EdgeFamilyError)?;

        let edge = self.get_edge::<T, R>(edge_id)?;
        let from_node_id = edge.connection();

        let txn = db.transaction();

        txn.delete_cf(&edge_family, edge.id().to_string())
            .map_err(GraphError::DeleteError)?;
        txn.commit().map_err(GraphError::DeleteError)?;
        Ok(())
    }

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

    fn create_family_if_not_exists(&self, family_name: &str) -> Result<(), GraphError> {
        let db = &self.db;
        if db.cf_handle(family_name).is_none() {
            let options = Options::default();
            db.create_cf(family_name, &options)
                .map_err(GraphError::CreateFamilyError)?;
        }
        Ok(())
    }

    pub fn destroy_everything(&self) -> Result<(), GraphError> {
        let families =
            DB::list_cf(&Options::default(), &self.path).map_err(GraphError::FindFamiliesError)?;

        for family_name in families {
            if family_name != "default" {
                self.db
                    .drop_cf(&family_name)
                    .map_err(GraphError::DeleteError)?;
            }
        }

        Ok(())
    }

    pub fn display_family_head<T>(&self) -> Result<(), GraphError>
    where
        T: Node,
    {
        let node_families =
            DB::list_cf(&Options::default(), &self.path).map_err(GraphError::FindFamiliesError)?;
        for node_family_name in node_families {
            let node_family = self
                .db
                .cf_handle(&node_family_name)
                .ok_or(GraphError::NodeFamilyError)?;

            let records = self
                .db
                .iterator_cf(&node_family, rocksdb::IteratorMode::Start);

            println!("Node family: {}", node_family_name);

            for record in records.take(5) {
                match record {
                    Ok((key, value)) => {
                        let key_str =
                            String::from_utf8(key.to_vec()).map_err(GraphError::ParseUtf8Error)?;
                        let value_str: T = rmp_serde::from_slice(&value)?;
                        println!("{}: {:?}", key_str, value_str)
                    }
                    Err(_) => return Err(GraphError::FindKeyError),
                }
            }
        }

        Ok(())
    }

    pub fn count_nodes(&self) -> Result<usize, GraphError> {
        let families =
            DB::list_cf(&Options::default(), &self.path).map_err(GraphError::FindFamiliesError)?;
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

    pub fn get_type_name<T>(&self) -> String {
        let type_name = std::any::type_name::<T>();
        let type_name = type_name.split("::").last().unwrap();
        type_name.to_string()
    }
}
