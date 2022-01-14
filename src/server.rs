use std::convert::Infallible;
use std::io::prelude::*;
use std::io::Write;
use std::time::Instant;

use tokio_postgres::NoTls;
use warp::Filter;
use warp::http::HeaderValue;

use crate::config;
use crate::sample::hex_utils;

pub(crate) async fn serve_gossip() {
	let hello = warp::path!("hello" / String).map(|name| format!("Hello, {}!", name));

	let bye = warp::path!("bye" / String).map(|name| format!("Bye, {}!", name));

	// let composite = warp::path!("composite" / "block" / u32 / "timestamp" / u64).and_then()

	// let composite = warp::path!("composite" / "block" / u32 / "timestamp" / u64).and_then(serve_composite);

	// let routes = warp::get().and(hello.or(bye));
	// let routes = warp::get().and(
	//     composite
	//         .or(hello)
	//         .or(bye)
	// );

	// let routes = warp::get().and_then(composite);
	let routes = warp::path!("composite" / "block" / u32 / "timestamp" / u64)
		.and_then(serve_composite);
	// .with(warp::filters::compression::gzip());

	warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}

async fn serve_composite(block: u32, timestamp: u64) -> Result<impl warp::Reply, Infallible> {
	// let response = format!("block, timestamp: {}, {}", block, timestamp);

	let start = Instant::now();

	let (client, connection) =
		tokio_postgres::connect(config::db_connection_string().as_str(), NoTls).await.unwrap();

	tokio::spawn(async move {
		if let Err(e) = connection.await {
			panic!("connection error: {}", e);
		}
	});

	let mut vector: Vec<u8> = vec![76, 68, 75, 2];
	let mut gossip_message_count = 0u32;

	{
		println!("fetching channels…");
		let rows = client.query("SELECT * FROM channels", &[]).await.unwrap();
		gossip_message_count += rows.len() as u32;
		for current_row in rows {
			let blob: String = current_row.get("announcement_unsigned");
			let mut data = hex_utils::to_vec(&blob).unwrap();
			vector.append(&mut data);
		}
	}

	{
		println!("fetching updates…");
		// let timestamp_minimum = timestamp as i64;
		let timestamp_minimum = 0i64;
		// let rows = client.query("SELECT * FROM channel_updates", &[]).await.unwrap();
		let rows = client.query("SELECT DISTINCT ON (short_channel_id, direction) * FROM channel_updates WHERE timestamp >= $1 ORDER BY short_channel_id ASC, direction ASC, timestamp DESC", &[&timestamp_minimum]).await.unwrap();
		gossip_message_count += rows.len() as u32;
		for current_row in rows {
			let blob: String = current_row.get("blob_unsigned");
			let mut data = hex_utils::to_vec(&blob).unwrap();
			vector.append(&mut data);
		}
	}

	let response_length = vector.len();

	println!("packaging response!");

	let mut compressor = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
	compressor.write_all(&vector);
	let compressed_response = compressor.finish().unwrap();

	let compressed_length = compressed_response.len();
	let compression_efficacy = 1.0 - (compressed_length as f64) / (response_length as f64);

	// let end = Instant::now();
	let duration = start.elapsed();
	// let seconds = duration.as_millis();

	let elapsed_time = format!("{:?}", duration);
	let efficacy_header = format!("{}%", (compression_efficacy * 1000.0).round()/10.0);

	let response = warp::http::Response::builder()
		.header("X-LDK-Gossip-Message-Count", HeaderValue::from(gossip_message_count))
		.header("X-LDK-Raw-Output-Length", HeaderValue::from(response_length))
		.header("X-LDK-Compressed-Output-Length", HeaderValue::from(compressed_length))
		.header("X-LDK-Compression-Efficacy", HeaderValue::from_str(efficacy_header.as_str()).unwrap())
		.header("X-LDK-Elapsed-Time", HeaderValue::from_str(elapsed_time.as_str()).unwrap())
		.header("Content-Encoding", HeaderValue::from_static("gzip"))
		.header("Content-Length", HeaderValue::from(compressed_length))
		.body(compressed_response);

	// let response = format!("block: {}<br/>\ntimestamp: {}<br/>\nlength: {}<br/>\nelapsed: {:?}", block, timestamp, response_length, duration);
	Ok(response)
}

// async fn build_graph_response() -> Result<(), Error> {
//     let (client, connection) =
//         tokio_postgres::connect("host=localhost user=arik dbname=ln_graph_sync", NoTls).await?;
//
//     // The connection object performs the actual communication with the database,
//     // so spawn it off to run on its own.
//     tokio::spawn(async move {
//         if let Err(e) = connection.await {
//             eprintln!("connection error: {}", e);
//         }
//     });
//
//     let mut vector: Vec<u8> = Vec::new();
//
//     println!("building graph response…");
//     // Now we can execute a simple statement that just returns its parameter.
//     let rows = client.query("SELECT * FROM channel_updates", &[]).await?;
//
//     for current_row in rows {
//         let blob: String = current_row.get("blob_unsigned");
//         let mut data = hex::decode(blob).unwrap();
//         vector.append(&mut data);
//         // let some_value  = current_row.get(1);
//         // println!("here we are");
//     }
//     println!("done!");
//
//     Ok(())
// }
