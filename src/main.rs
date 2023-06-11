use graph::{Album, Artist, ArtistId, By, ByConnection, Graph, Node, Song};
use rayon::prelude::*;
use xid::Id;

enum InEdgeType {
    A(IdOne),
    B(IdOne),
    C(IdTwo),
}

enum OutEdgeType {
    D(IdTwo),
    E(IdOne),
    F(IdOne),
}

struct IdOne(String);
struct IdTwo(String);

trait OutEdge {
    fn get_out_edge_id(&self) -> String;
}
trait InEdge {
    fn get_in_edge_id(&self) -> String;
}

impl OutEdge for OutEdgeType {
    fn get_out_edge_id(&self) -> String {
        match self {
            OutEdgeType::D(s) => s.0.clone(),
            OutEdgeType::E(s) => s.0.clone(),
            OutEdgeType::F(s) => s.0.clone(),
        }
    }
}

impl InEdge for InEdgeType {
    fn get_in_edge_id(&self) -> String {
        match self {
            InEdgeType::A(s) => s.0.clone(),
            InEdgeType::B(s) => s.0.clone(),
            InEdgeType::C(s) => s.0.clone(),
        }
    }
}

fn add_edge<T, R>(in_edge: T, out_edge: R)
where
    T: InEdge,
    R: OutEdge,
{
    let edge_id = in_edge.get_in_edge_id();
    println!("Called function!")
}

fn main() {
    add_edge(
        InEdgeType::A(IdOne("hello".to_string())),
        OutEdgeType::D(IdTwo("world".to_string())),
    );
    let new_artist_id = Artist::new_id("what".to_string());
    // let graph = Graph::new("/Users/sambrownstone/graphite_data").unwrap();

    // let start = std::time::Instant::now();

    // (0..1000000).into_par_iter().for_each(|_| {
    //     let song = Song::new(None, "Matter".to_string());
    //     let artist = Artist::new(None, "Family Stereo".to_string());
    //     let album = Album::new(None, "Matter".to_string());

    //     let song_is_by_artist_connection =
    //         ByConnection::SongIsBy(song.id().clone(), artist.id().clone());
    //     let album_is_by_artist_connection =
    //         ByConnection::AlbumIsBy(album.id().clone(), artist.id().clone());

    //     let song_is_by_artist_edge = By::new(None, song_is_by_artist, 0.5);
    //     let album_is_by_artist_edge = By::new(None, album_is_by_artist, 0.5);

    //     graph.add_edge(song_is_by_artist_edge).unwrap();
    // });

    // println!("time to add 2m nodes and 1m edges: {:?}", start.elapsed());

    // let nodes_num = graph.count_nodes().unwrap();
    // println!("nodes: {}", nodes_num);
}
