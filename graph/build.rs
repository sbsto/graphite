use quote::quote;
use serde::Deserialize;
use std::fs::File;
use std::io::Write;

#[derive(Debug, Deserialize)]
struct SchemaField {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
}

#[derive(Debug, Deserialize)]
struct ScheaNode {
    name: String,
    fields: Vec<SchemaField>,
}

#[derive(Debug, Deserialize)]
struct SchemaConnection {
    from: String,
    to: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct SchemaEdge {
    name: String,
    connections: Vec<SchemaConnection>,
    fields: Vec<SchemaField>,
}

#[derive(Debug, Deserialize)]
struct Schema {
    nodes: Vec<ScheaNode>,
    edges: Vec<SchemaEdge>,
}

fn main() {
    let schema: Schema = serde_yaml::from_reader(File::open("schema.yml").unwrap()).unwrap();
    let mut output = File::create("src/generated.rs").unwrap();
    let mut families: Vec<String> = Vec::new();

    let imports_impl = quote! {
        use serde::{Serialize, Deserialize};
        use xid;
    };

    writeln!(output, "{}", imports_impl).unwrap();

    let node_impl = quote! {
            pub trait NodeId: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug {
                                fn new(id: Option<String>) -> Self;
                fn to_string(&self) -> String;
                fn family_name(&self) -> String;
            }

            pub trait Node: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug {
                type Id: NodeId;

                fn id(&self) -> &Self::Id;
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
            pub trait EdgeId: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug {
              fn to_string(&self) -> String;
                            fn family_name(&self) -> String;
            }

            pub trait EdgeConnection: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug {}

            pub trait Edge: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug {
                type Id: EdgeId;
                type Connection: EdgeConnection;

                fn id(&self) -> &Self::Id;
                fn connection(&self) -> &Self::Connection;
                fn family_name(&self) -> String;
            }
    };

    writeln!(output, "{}", node_impl).unwrap();
    writeln!(output, "{}", edge_impl).unwrap();

    for node in &schema.nodes {
        let struct_name = syn::Ident::new(&node.name, proc_macro2::Span::call_site());
        let struct_name_id =
            syn::Ident::new(&format!("{}Id", &node.name), proc_macro2::Span::call_site());

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
            pub struct #struct_name_id(String);

            impl NodeId for #struct_name_id {
                fn new (id: Option<String>) -> Self {
                    Self(format!(concat!(stringify!(#struct_name), ":{}"), id.unwrap_or_else(|| xid::new().to_string())))
                }

                fn to_string(&self) -> String {
                    self.0.clone()
                }

                fn family_name(&self) -> String {
                    stringify!(#struct_name).to_string()
                }
            }

            #[derive(Debug, Serialize, Deserialize, Clone)]
            pub struct #struct_name {
                id: #struct_name_id,
                in_edge_ids: Vec<String>,
                out_edge_ids: Vec<String>,
                #( #field_idents: #field_types, )*
            }

            impl #struct_name {
                pub fn new(id: Option<String>, #( #field_idents: #field_types, )*) -> Self {
                    Self {
                        id: #struct_name_id(format!(concat!(stringify!(#struct_name), ":{}"), id.unwrap_or(xid::new().to_string()))),
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
                type Id = #struct_name_id;

                fn id(&self) -> &#struct_name_id {
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

        writeln!(output, "{}", node_impl).unwrap();
    }

    for edge in &schema.edges {
        let struct_name = syn::Ident::new(&edge.name, proc_macro2::Span::call_site());
        let struct_name_id =
            syn::Ident::new(&format!("{}Id", &edge.name), proc_macro2::Span::call_site());
        let struct_name_connection = syn::Ident::new(
            &format!("{}Connection", &edge.name),
            proc_macro2::Span::call_site(),
        );

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

        let mut connection_variants = Vec::new();

        for connection in &edge.connections {
            let from = syn::Ident::new(
                &format!("{}Id", &connection.from),
                proc_macro2::Span::call_site(),
            );
            let to = syn::Ident::new(
                &format!("{}Id", &connection.to),
                proc_macro2::Span::call_site(),
            );
            let connection_variant =
                syn::Ident::new(&connection.name, proc_macro2::Span::call_site());
            connection_variants.push(quote! { #connection_variant(#from, #to) });
        }

        let edge_impl = quote! {
            #[derive(Debug, Serialize, Deserialize, Clone)]
            pub struct #struct_name_id(String);

            impl EdgeId for #struct_name_id {
                fn to_string(&self) -> String {
                    self.0.clone()
                }

                fn family_name(&self) -> String {
                    stringify!(#struct_name).to_string()
                }
            }

            #[derive(Debug, Serialize, Deserialize, Clone)]
            pub enum #struct_name_connection {
                    #( #connection_variants ),*
            }

            impl EdgeConnection for #struct_name_connection {}



            #[derive(Debug, Serialize, Deserialize, Clone)]
            pub struct #struct_name {
                id: #struct_name_id,
                connection: #struct_name_connection,
                #( #field_idents: #field_types, )*
            }

            impl #struct_name {
                pub fn new(id: Option<String>, connection: #struct_name_connection, #( #field_idents: #field_types, )*) -> Self {
                    Self {
                        id: #struct_name_id(format!(concat!(stringify!(#struct_name), ":{}"), id.unwrap_or_else(|| xid::new().to_string()))),
                        connection,
                        #( #field_idents ),*
                    }
                }

                pub fn id(&self) -> &#struct_name_id {
                    &self.id
                }
            }

            impl std::str::FromStr for #struct_name {
                type Err = serde_json::Error;

                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    serde_json::from_str::<Self>(s)
                }
            }

            impl Edge for #struct_name {
                type Id = #struct_name_id;
                type Connection = #struct_name_connection;

                fn id(&self) -> &#struct_name_id {
                    &self.id
                }

                fn connection(&self) -> &Self::Connection {
                    &self.connection
                }

                fn family_name(&self) -> String {
                    stringify!(#struct_name).to_string()
                }
            }
        };

        writeln!(output, "{}", edge_impl).unwrap();
    }

    let families_impl = quote! {
        pub fn families() -> Vec<&'static str> {
        vec![#( #families ),*]
        }
    };

    writeln!(output, "{}", families_impl).unwrap();
}
