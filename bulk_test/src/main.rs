use async_std::task;
use async_std_http_client::*;
use std::time::{Duration, Instant};

const REQUESTS: usize = 1000;

async fn test() {
    let client = Client::builder("http://host.docker.internal:80/")
        .connection_count(25)
        .build()
        .expect("could not build client");

    let client2 = client.clone();

    task::spawn(async move {
        loop {
            async_std::task::sleep(Duration::from_millis(10000)).await;
            log::error!("{}", client2.print_internal_state());
        }
    });

    loop {
        let start = Instant::now();

        let reqs: Vec<_> = std::iter::repeat("/")
            .take(REQUESTS)
            .map(|path| client.req(Request::get(path).build()))
            .collect();

        let _result = futures::future::try_join_all(reqs).await.unwrap();

        log::warn!(
            "{} requests done in {} ms",
            REQUESTS,
            Instant::now().duration_since(start).as_millis()
        );
    }
}

fn main() {
    simple_logger::init_with_level(log::Level::Warn).unwrap();
    task::block_on(test())
}
