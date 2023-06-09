use graph::{Album, Artist, By, ByConnection, Graph, Node, Song};
use rayon::prelude::*;

fn main() {
    let graph = Graph::new("/Users/sambrownstone/graphite_data").unwrap();

    let start = std::time::Instant::now();

    (0..1000000).into_par_iter().for_each(|_| {
        let song = Song::new(None, "Matter".to_string());
        let artist = Artist::new(None, "Family Stereo".to_string());
        let album = Album::new(None, "Matter".to_string());

        let song_is_by_artist_connection =
            ByConnection::SongIsBy(song.id().clone(), artist.id().clone());
        let album_is_by_artist_connection =
            ByConnection::AlbumIsBy(album.id().clone(), artist.id().clone());

        let song_is_by_artist_edge = By::new(None, song_is_by_artist, 0.5);
        let album_is_by_artist_edge = By::new(None, album_is_by_artist, 0.5);

        graph.add_edge(song_is_by_artist_edge).unwrap();
    });

    println!("time to add 2m nodes and 1m edges: {:?}", start.elapsed());

    // let nodes_num = graph.count_nodes().unwrap();
    // println!("nodes: {}", nodes_num);
}
