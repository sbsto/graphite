use serde::{Serialize, Deserialize};

pub trait IceNode: Serialize + for<'de> Deserialize<'de> + Clone {
	fn id(&self) -> &str;
	fn family_name(&self) -> String;
}

#[macro_export]
macro_rules! create_node_struct {
	($struct_name:ident {
		$($field_name:ident: $field_type:ty),* $(,)?
	}) => {
		#[derive(Debug, Serialize, Deserialize, Clone)]
		pub struct $struct_name {
			id: String,
			edge_ids: Vec<String>,
			$($field_name: $field_type),*,
		}

		impl $struct_name {
			pub fn new(id: Option<String>, $($field_name: $field_type,)*) -> Self {
				Self {
					id: format!(concat!(stringify!($struct_name), ":{}"), id.unwrap_or(xid::new().to_string())),
					edge_ids: Vec::new(),
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

			fn family_name(&self) -> String {
				stringify!($struct_name).to_string()
			}
		}
	};
}