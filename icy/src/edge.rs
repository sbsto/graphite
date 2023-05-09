use serde::{Serialize, Deserialize};

pub trait IceEdge: Serialize + for<'de> Deserialize<'de> + Clone {
	fn id(&self) -> &str;
	fn from_node_id(&self) -> &str;
	fn to_node_id(&self) -> &str;
	fn family_name(&self) -> String;
}

#[macro_export]
macro_rules! create_edge_struct {
	($graph:expr, $struct_name:ident {
		$($field_name:ident: $field_type:ty),* $(,)?
	}) => {
		#[derive(Debug, Serialize, Deserialize, Clone)]
		pub struct $struct_name {
			id: String,
			from_node_id: String,
			to_node_id: String,
			$($field_name: $field_type),*,
		}

		impl $struct_name {
			pub fn new_between(id: Option<String>, from_node_id: &str, to_node_id: &str, $($field_name: $field_type,)*) -> Self {
				Self {
					id: format!(concat!(stringify!($struct_name), ":{}"), id.unwrap_or(xid::new().to_string())),
					from_node_id: from_node_id.to_string(),
					to_node_id: to_node_id.to_string(),
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

		impl IceEdge for $struct_name {
			fn id(&self) -> &str {
				&self.id
			}

			fn from_node_id(&self) -> &str {
				&self.from_node_id
			}

			fn to_node_id(&self) -> &str {
				&self.to_node_id
			}

			fn family_name(&self) -> String {
				stringify!($struct_name).to_string()
			}
		}

		$graph.create_family_if_not_exists(stringify!($struct_name)).unwrap();
	};
}