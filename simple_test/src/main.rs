use async_std::task;
use async_std_http_client::*;
use std::time::Duration;

async fn test() {
    log::info!("starting test");

    let client = Client::builder("https://www.rust-lang.org:443/")
        .build()
        .expect("could not build client");

    let rs1 = client.req(Request::get("/").build()).await.unwrap();

    log::info!("request 1 done, got {} bytes", rs1.body.len());

    let mut builder: RequestBuilder = Request::get("static/styles/solarized-dark.css");

    let rs2 = client
        .req(
            builder
                .with_header(USER_AGENT, HeaderValue::from_bytes(b"test").unwrap())
                .build(),
        )
        .await
        .unwrap();

    log::info!("request 2 done, got {} bytes", rs2.body.len());

    task::sleep(Duration::from_secs(1)).await;

    let rs3 = client
        .req(
            Request::get("what/cli")
                .with_request_args(vec![("öüpooui", "qwdhiuqwd"), ("h huqwd", "öäü")])
                .build(),
        )
        .await
        .unwrap();

    log::info!("request 3 done, got {} bytes", rs3.body.len());

    log::info!("test done");
}

fn main() {
    simple_logger::init_with_level(log::Level::Trace).unwrap();
    task::block_on(test())
}
