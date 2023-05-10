use quote::quote;
use serde::Deserialize;
use std::fs::File;
use std::io::Write;

#[derive(Debug, Deserialize)]
struct Field {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
}

#[derive(Debug, Deserialize)]
struct NodeType {
    name: String,
    fields: Vec<Field>,
}

#[derive(Debug, Deserialize)]
struct Connection {
    from: String,
    to: String,
}

#[derive(Debug, Deserialize)]
struct EdgeType {
    name: String,
    connections: Vec<Connection>,
    fields: Vec<Field>,
}

#[derive(Debug, Deserialize)]
struct Schema {
    nodes: Vec<NodeType>,
    edges: Vec<EdgeType>,
}

fn main() {
    let schema: Schema = serde_yaml::from_reader(File::open("schema.yml").unwrap()).unwrap();
    let mut output = File::create("src/generated.rs").unwrap();
    let mut families: Vec<String> = Vec::new();

    let imports_impl = quote! {
        use serde::{Serialize, Deserialize};
        use xid;
    };

    writeln!(output, "{}", imports_impl.to_string()).unwrap();

    let node_impl = quote! {
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
    };

    let edge_impl = quote! {
        pub trait Edge: Serialize + for<'de> Deserialize<'de> + Clone {
            fn id(&self) -> &str;
            fn from_node_id(&self) -> &str;
            fn to_node_id(&self) -> &str;
            fn family_name(&self) -> String;
        }
    };

    writeln!(output, "{}", node_impl.to_string()).unwrap();
    writeln!(output, "{}", edge_impl.to_string()).unwrap();

    for node in &schema.nodes {
        let struct_name = syn::Ident::new(&node.name, proc_macro2::Span::call_site());
        families.push(struct_name.to_string());

        let mut field_idents = Vec::new();
        let mut field_types = Vec::new();
        for field in &node.fields {
            field_idents.push(syn::Ident::new(&field.name, proc_macro2::Span::call_site()));
            field_types.push(syn::Ident::new(
                &field.type_name,
                proc_macro2::Span::call_site(),
            ));
        }

        let node_impl = quote! {
            #[derive(Debug, Serialize, Deserialize, Clone)]
            pub struct #struct_name {
                id: String,
                in_edge_ids: Vec<String>,
                out_edge_ids: Vec<String>,
                #( #field_idents: #field_types, )*
            }

            impl #struct_name {
                pub fn new(id: Option<String>, #( #field_idents: #field_types, )*) -> Self {
                    Self {
                        id: format!(concat!(stringify!(#struct_name), ":{}"), id.unwrap_or(xid::new().to_string())),
                        in_edge_ids: Vec::new(),
                        out_edge_ids: Vec::new(),
                        #( #field_idents ),*,
                    }
                }
            }

            impl std::str::FromStr for #struct_name {
                type Err = serde_json::Error;

                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    serde_json::from_str::<Self>(s)
                }
            }

            impl Node for #struct_name {
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
                    stringify!(#struct_name).to_string()
                }
            }
        };

        writeln!(output, "{}", node_impl.to_string()).unwrap();
    }

    for edge in &schema.edges {
        let struct_name = syn::Ident::new(&edge.name, proc_macro2::Span::call_site());
        families.push(struct_name.to_string());

        let mut field_idents = Vec::new();
        let mut field_types = Vec::new();
        for field in &edge.fields {
            field_idents.push(syn::Ident::new(&field.name, proc_macro2::Span::call_site()));
            field_types.push(syn::Ident::new(
                &field.type_name,
                proc_macro2::Span::call_site(),
            ));
        }

        let edge_impl = quote! {
                #[derive(Debug, Serialize, Deserialize, Clone)]
                pub struct #struct_name {
                    id: String,
                    from_node_id: String,
                    to_node_id: String,
                    #( #field_idents: #field_types, )*
                }

                impl #struct_name {
                    pub fn new_between(id: Option<String>, from_node_id: &str, to_node_id: &str, #( #field_idents: #field_types, )*) -> Self {
                        Self {
                            id: format!(concat!(stringify!(#struct_name), ":{}"), id.unwrap_or_else(|| xid::new().to_string())),
                            from_node_id: from_node_id.to_string(),
                            to_node_id: to_node_id.to_string(),
                            #( #field_idents ),*
                        }
                    }
                }

                impl std::str::FromStr for #struct_name {
                    type Err = serde_json::Error;

                    fn from_str(s: &str) -> Result<Self, Self::Err> {
                        serde_json::from_str::<Self>(s)
                    }
                }

                impl Edge for #struct_name {
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
                        stringify!(#struct_name).to_string()
                    }
                }
        };
        writeln!(output, "{}", edge_impl.to_string()).unwrap();
    }
    let families_impl = quote! {
        pub fn families() -> Vec<&'static str> {
            vec![#( #families ),*]
        }
    };

    writeln!(output, "{}", families_impl.to_string()).unwrap();
}
