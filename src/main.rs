use bitcoin::blockdata::constants::genesis_block;
use bitcoin::Network;
use bitcoin::secp256k1::SecretKey;
use lightning;
use lightning::ln::peer_handler::IgnoringMessageHandler;
use lightning::routing::network_graph::{NetGraphMsgHandler, NetworkGraph};
use lightning::util::test_utils::TestLogger;
// use rand::{RngCore, thread_rng};
use rand::thread_rng;
use tokio_postgres::{Error, NoTls};
use warp::Filter;

mod compression;
mod sample;

#[tokio::main]
async fn main() {
    let logger = TestLogger::new();
    let mut key = [0; 32];

    // let mut random_data = [0; 32];
    // thread_rng().fill_bytes(&mut key);
    // thread_rng().fill_bytes(&mut random_data);
    // let our_node_secret = SecretKey::from_slice(&key).unwrap();

    // let network_graph = NetworkGraph::new(genesis_block(Network::Bitcoin).header.block_hash());
    // let routing_message_handler = NetGraphMsgHandler::new(network_graph, None, &logger);
    // let peer_handler = lightning::ln::peer_handler::PeerManager::new_routing_only(routing_message_handler, our_node_secret, &random_data, &logger);

    build_graph_response().await;
    // sample::start_ldk().await;

    let hello = warp::path!("hello" / String)
        .map(|name| format!("Hello, {}!", name));

    let bye = warp::path!("bye" / String)
        .map(|name| format!("Bye, {}!", name));

    let routes = warp::get().and(hello.or(bye));

    warp::serve(routes)
        .run(([127, 0, 0, 1], 3030))
        .await;
}

async fn build_graph_response() -> Result<(), Error> {
    let (client, connection) = tokio_postgres::connect("host=localhost user=arik dbname=ln_graph_sync", NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let mut vector: Vec<u8> = Vec::new();

    println!("building graph response…");
    // Now we can execute a simple statement that just returns its parameter.
    let rows = client
        .query("SELECT * FROM channel_updates", &[])
        .await?;

    for current_row in rows {
        let blob: String = current_row.get("blob_unsigned");
        let mut data = hex::decode(blob).unwrap();
        vector.append(&mut data);
        // let some_value  = current_row.get(1);
        // println!("here we are");
    }
    println!("done!");


    Ok(())

}