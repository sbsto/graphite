use serde::{Serialize, Deserialize};

pub trait Node: Serialize + for<'de> Deserialize<'de> + Clone {
	fn id(&self) -> &str;
	fn in_edge_ids(&self) -> Vec<String>;
	fn out_edge_ids(&self) -> Vec<String>;
	fn family_name(&self) -> String;
	fn add_in_edge_id(&mut self, edge_id: String);
	fn remove_in_edge_id(&mut self, edge_id: &str);
	fn add_out_edge_id(&mut self, edge_id: String);
	fn remove_out_edge_id(&mut self, edge_id: &str);
}

#[macro_export]
macro_rules! create_node_struct {
	($graph:expr, $struct_name:ident {
		$($field_name:ident: $field_type:ty),* $(,)?
	}) => {
		#[derive(Debug, Serialize, Deserialize, Clone)]
		pub struct $struct_name {
			id: String,
			in_edge_ids: Vec<String>,
			out_edge_ids: Vec<String>,
			$($field_name: $field_type),*,
		}

		impl $struct_name {
			pub fn new(id: Option<String>, $($field_name: $field_type,)*) -> Self {
				Self {
					id: format!(concat!(stringify!($struct_name), ":{}"), id.unwrap_or(xid::new().to_string())),
					in_edge_ids: Vec::new(),
					out_edge_ids: Vec::new(),
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

		impl Node for $struct_name {
			fn id(&self) -> &str {
				&self.id
			}

			fn in_edge_ids(&self) -> Vec<String> {
				self.in_edge_ids.clone()
			}

			fn add_in_edge_id(&mut self, edge_id: String) {
				self.in_edge_ids.push(edge_id);
			}

			fn remove_in_edge_id(&mut self, edge_id: &str) {
				self.in_edge_ids.retain(|x| x != edge_id);
			}

			fn out_edge_ids(&self) -> Vec<String> {
				self.out_edge_ids.clone()
			}

			fn add_out_edge_id(&mut self, edge_id: String) {
				self.out_edge_ids.push(edge_id);
			}

			fn remove_out_edge_id(&mut self, edge_id: &str) {
				self.out_edge_ids.retain(|x| x != edge_id);
			}

			fn family_name(&self) -> String {
				stringify!($struct_name).to_string()
			}
		}

		$graph.create_family_if_not_exists(stringify!($struct_name)).unwrap();
	};
}