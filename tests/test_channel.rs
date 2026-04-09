use std::time::{Duration, Instant};

use etcd_client::BalancedChannelBuilder;
use tonic::transport::{Endpoint, channel::Change};
use tower::{Service, ServiceExt};

use rust_etcd_utils::channel::{
    EndpointStatus, ReliableBalancedChannelBuilder, connect_with_reliable_balanced_channel,
};

mod common;

/// Verifies the reliable builder works as a drop-in replacement for a single healthy endpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reliable_channel_connect_put_get_single_endpoint() {
    let mut client = connect_with_reliable_balanced_channel(["http://localhost:2379"], None)
        .await
        .expect("failed to connect with reliable builder");

    let key = format!("test-channel-single-{}", common::random_str(12));
    let value = "value-single";

    client
        .put(key.as_str(), value, None)
        .await
        .expect("put failed");

    let resp = client.get(key.as_str(), None).await.expect("get failed");
    let kv = resp.kvs().first().expect("missing kv");
    assert_eq!(kv.value_str().expect("utf8 value"), value);
}

/// Verifies unreachable endpoints do not prevent requests when at least one healthy endpoint exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reliable_channel_works_with_unreachable_endpoint_in_list() {
    let mut client = connect_with_reliable_balanced_channel(
        ["http://127.0.0.1:1", "http://localhost:2379"],
        None,
    )
    .await
    .expect("failed to connect with mixed endpoints");

    let key = format!("test-channel-mixed-{}", common::random_str(12));
    let value = "value-mixed";

    client
        .put(key.as_str(), value, None)
        .await
        .expect("put failed with mixed endpoints");

    let resp = client.get(key.as_str(), None).await.expect("get failed");
    let kv = resp.kvs().first().expect("missing kv");
    assert_eq!(kv.value_str().expect("utf8 value"), value);
}

/// Verifies adding and removing a bad endpoint does not break ongoing operations.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reliable_channel_add_and_remove_unreachable_endpoint() {
    let mut client = connect_with_reliable_balanced_channel(["http://localhost:2379"], None)
        .await
        .expect("failed to connect with reliable builder");

    client
        .add_endpoint("http://127.0.0.1:1")
        .await
        .expect("failed to add unreachable endpoint");

    let key1 = format!("test-channel-add-remove-{}", common::random_str(12));
    client
        .put(key1.as_str(), "v1", None)
        .await
        .expect("put failed after add_endpoint");

    client
        .remove_endpoint("http://127.0.0.1:1")
        .await
        .expect("failed to remove unreachable endpoint");

    let key2 = format!("test-channel-add-remove-{}", common::random_str(12));
    client
        .put(key2.as_str(), "v2", None)
        .await
        .expect("put failed after remove_endpoint");
}

/// Verifies explicit endpoint removal cleans both routing state and exposed status registry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removing_quarantined_endpoint_removes_it_from_registry() {
    let mut builder = ReliableBalancedChannelBuilder::default();
    builder.probe_timeout = Duration::from_millis(100);
    builder.quarantine_retry_interval = Duration::from_millis(250);
    let stats_registry = builder.stats_registry();

    let client = etcd_client::Client::connect_with_balanced_channel(
        ["http://localhost:2379"],
        None,
        builder,
    )
    .await
    .expect("failed to connect with reliable builder");

    let bad_endpoint = "http://127.0.0.1:1";
    client
        .add_endpoint(bad_endpoint)
        .await
        .expect("failed to add unreachable endpoint");

    let quarantined_seen = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if stats_registry
                .endpoint_status_snapshot()
                .iter()
                .any(|(k, v)| k.contains("127.0.0.1:1") && *v == EndpointStatus::Quarantined)
            {
                break true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for quarantined endpoint");
    assert!(quarantined_seen);

    client
        .remove_endpoint(bad_endpoint)
        .await
        .expect("failed to remove unreachable endpoint");

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if !stats_registry
                .endpoint_status_snapshot()
                .keys()
                .any(|k| k.contains("127.0.0.1:1"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for endpoint removal from registry");
}

/// Verifies a down endpoint is quarantined, then reactivated after the same endpoint comes back.
///
/// Test phases:
/// 1. Add a fake endpoint while nothing is listening on its port.
/// 2. Wait for quarantine transition and assert stats were updated.
/// 3. Start a mock server on the same endpoint.
/// 4. Wait for active transition and assert quarantine count returns to zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quarantined_endpoint_becomes_active_when_remote_comes_back_online() {
    let mut builder = ReliableBalancedChannelBuilder::default();
    builder.probe_timeout = Duration::from_millis(100);
    builder.quarantine_retry_interval = Duration::from_millis(150);
    let stats_registry = builder.stats_registry();

    let client = etcd_client::Client::connect_with_balanced_channel(
        ["http://localhost:2379"],
        None,
        builder,
    )
    .await
    .expect("failed to connect with reliable builder");

    let fake_addr = common::reserve_localhost_addr();
    let fake_endpoint = common::endpoint_url(fake_addr);

    // Phase 1: insert endpoint while down.
    client
        .add_endpoint(fake_endpoint.as_str())
        .await
        .expect("failed to add fake endpoint");

    // Phase 2: verify quarantine.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if stats_registry
                .endpoint_status_snapshot()
                .iter()
                .any(|(k, v)| {
                    k.contains(fake_endpoint.as_str()) && *v == EndpointStatus::Quarantined
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for fake endpoint quarantine");

    let stats_after_quarantine = stats_registry.snapshot();
    assert!(stats_after_quarantine.quarantine_transitions >= 1);
    assert!(stats_after_quarantine.quarantined_endpoints >= 1);

    // Phase 3: bring endpoint back.
    let fake_server = common::spawn_fake_tcp_server(fake_addr).await;

    // Phase 4: verify recovery to active.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if stats_registry
                .endpoint_status_snapshot()
                .iter()
                .any(|(k, v)| k.contains(fake_endpoint.as_str()) && *v == EndpointStatus::Active)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for fake endpoint recovery to active");

    let stats_after_recovery = stats_registry.snapshot();
    assert!(stats_after_recovery.active_endpoints >= 2);
    assert_eq!(stats_after_recovery.quarantined_endpoints, 0);

    fake_server.shutdown().await;
}

/// Verifies request path behavior across endpoint outage and restart using a mock HTTP/2 service.
///
/// Test phases:
/// 1. Start endpoint and verify requests succeed.
/// 2. Stop endpoint and verify a new request fails.
/// 3. Drive additional requests while down and verify quarantine becomes visible.
/// 4. Restart endpoint, verify active status, and verify requests succeed again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_fails_when_mock_endpoint_goes_down_then_recovers_after_restart() {
    let mut builder = ReliableBalancedChannelBuilder::default();
    builder.probe_timeout = Duration::from_millis(100);
    builder.quarantine_retry_interval = Duration::from_millis(150);
    let stats_registry = builder.stats_registry();

    let (mut channel, updater) = builder
        .balanced_channel(16)
        .expect("failed to build reliable balanced channel");

    let addr = common::reserve_localhost_addr();
    let endpoint_url = common::endpoint_url(addr);
    let mut mock_server = common::spawn_mock_h2_server(addr).await;

    let uri: http::Uri = endpoint_url.parse().expect("valid endpoint uri");
    let endpoint = Endpoint::from_shared(endpoint_url.clone()).expect("valid tonic endpoint");

    // Phase 1: endpoint is up and should serve requests.
    updater
        .send(Change::Insert(uri.clone(), endpoint))
        .await
        .expect("failed to insert mock endpoint");

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if stats_registry
                .endpoint_status_snapshot()
                .iter()
                .any(|(k, v)| k.contains(endpoint_url.as_str()) && *v == EndpointStatus::Active)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for mock endpoint active");

    let req = http::Request::builder()
        .uri("http://mock.local/ping")
        .body(tonic::body::Body::empty())
        .expect("failed to build request");
    let first_resp = channel
        .ready()
        .await
        .expect("channel should be ready while mock server is up")
        .call(req)
        .await;
    assert!(
        first_resp.is_ok(),
        "request should succeed when endpoint is up"
    );
    assert_eq!(
        stats_registry
            .get_call_count(endpoint_url.as_str())
            .expect("missing call count for endpoint"),
        1
    );

    // Phase 2: endpoint goes down and a new request should fail.
    mock_server.shutdown().await;

    let req = http::Request::builder()
        .uri("http://mock.local/ping")
        .body(tonic::body::Body::empty())
        .expect("failed to build request");
    let second_resp = channel
        .ready()
        .await
        .expect("channel should be ready before down request")
        .call(req)
        .await;
    assert!(
        second_resp.is_err(),
        "request should fail after server shutdown"
    );

    // Phase 3: continue observing until error-reporting-driven quarantine becomes visible.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if stats_registry
                .endpoint_status_snapshot()
                .iter()
                .any(|(k, v)| {
                    k.contains(endpoint_url.as_str()) && *v == EndpointStatus::Quarantined
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for endpoint quarantine after failure");

    // Phase 4: restart endpoint and verify recovery.
    mock_server = common::spawn_mock_h2_server(addr).await;

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if stats_registry
                .endpoint_status_snapshot()
                .iter()
                .any(|(k, v)| k.contains(endpoint_url.as_str()) && *v == EndpointStatus::Active)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for endpoint to become active after restart");

    let req = http::Request::builder()
        .uri("http://mock.local/ping")
        .body(tonic::body::Body::empty())
        .expect("failed to build request");
    let third_resp = channel
        .ready()
        .await
        .expect("channel should be ready after endpoint restart")
        .call(req)
        .await;
    assert!(
        third_resp.is_ok(),
        "request should succeed after endpoint restart"
    );
    assert!(
        stats_registry
            .get_call_count(endpoint_url.as_str())
            .expect("missing call count for endpoint")
            >= 3
    );

    mock_server.shutdown().await;
}

/// Verifies load is distributed across two healthy mock endpoints.
///
/// Test phases:
/// 1. Start two mock servers and insert both URIs.
/// 2. Wait until both endpoints are marked active.
/// 3. Send many requests through the balanced channel.
/// 4. Assert both endpoints receive a meaningful share of requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requests_are_distributed_between_two_mock_endpoints() {
    let mut builder = ReliableBalancedChannelBuilder::default();
    builder.probe_timeout = Duration::from_millis(100);
    builder.quarantine_retry_interval = Duration::from_millis(150);
    let stats_registry = builder.stats_registry();

    let (mut channel, updater) = builder
        .balanced_channel(32)
        .expect("failed to build reliable balanced channel");

    let addr_a = common::reserve_localhost_addr();
    let addr_b = common::reserve_localhost_addr();
    let endpoint_a = common::endpoint_url(addr_a);
    let endpoint_b = common::endpoint_url(addr_b);

    let server_a = common::spawn_mock_h2_server(addr_a).await;
    let server_b = common::spawn_mock_h2_server(addr_b).await;

    let uri_a: http::Uri = endpoint_a.parse().expect("valid endpoint a uri");
    let uri_b: http::Uri = endpoint_b.parse().expect("valid endpoint b uri");
    updater
        .send(Change::Insert(
            uri_a,
            Endpoint::from_shared(endpoint_a.clone()).expect("valid tonic endpoint a"),
        ))
        .await
        .expect("failed to insert endpoint a");
    updater
        .send(Change::Insert(
            uri_b,
            Endpoint::from_shared(endpoint_b.clone()).expect("valid tonic endpoint b"),
        ))
        .await
        .expect("failed to insert endpoint b");

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = stats_registry.endpoint_status_snapshot();
            let a_active = snapshot
                .iter()
                .any(|(k, v)| k.contains(endpoint_a.as_str()) && *v == EndpointStatus::Active);
            let b_active = snapshot
                .iter()
                .any(|(k, v)| k.contains(endpoint_b.as_str()) && *v == EndpointStatus::Active);
            if a_active && b_active {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for both endpoints to become active");

    let total_requests = 400usize;
    for _ in 0..total_requests {
        let req = http::Request::builder()
            .uri("http://mock.local/ping")
            .body(tonic::body::Body::empty())
            .expect("failed to build request");
        let resp = channel
            .ready()
            .await
            .expect("channel should be ready")
            .call(req)
            .await;
        assert!(
            resp.is_ok(),
            "request should succeed with two healthy endpoints"
        );
    }

    let a_count = stats_registry
        .get_call_count(endpoint_a.as_str())
        .expect("missing call count for endpoint a");
    let b_count = stats_registry
        .get_call_count(endpoint_b.as_str())
        .expect("missing call count for endpoint b");
    let observed_total = a_count + b_count;

    assert_eq!(
        observed_total, total_requests,
        "all requests should be attributed to one of the two endpoints"
    );

    // Keep this broad to avoid flakiness while still proving non-trivial distribution.
    let min_share = total_requests / 5; // 20%
    assert!(
        a_count >= min_share,
        "endpoint a received too few requests: {a_count}/{total_requests}"
    );
    assert!(
        b_count >= min_share,
        "endpoint b received too few requests: {b_count}/{total_requests}"
    );

    server_a.shutdown().await;
    server_b.shutdown().await;
}

/// Verifies fair distribution before failure, then successful drain after one endpoint is killed.
///
/// Test phases:
/// 1. Start two mock servers and insert both URIs.
/// 2. Send 500 requests and assert both endpoints receive a fair share.
/// 3. Kill one endpoint.
/// 4. Retry requests until 1000 successful sends are reached or 30 seconds pass.
/// 5. Assert one endpoint handled at least 500 more requests than the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fair_share_then_failover_drains_to_1000_successful_requests() {
    let mut builder = ReliableBalancedChannelBuilder::default();
    builder.probe_timeout = Duration::from_millis(100);
    builder.quarantine_retry_interval = Duration::from_millis(150);
    let stats_registry = builder.stats_registry();

    let (mut channel, updater) = builder
        .balanced_channel(32)
        .expect("failed to build reliable balanced channel");

    let addr_a = common::reserve_localhost_addr();
    let addr_b = common::reserve_localhost_addr();
    let endpoint_a = common::endpoint_url(addr_a);
    let endpoint_b = common::endpoint_url(addr_b);

    let mut server_a = Some(common::spawn_mock_h2_server(addr_a).await);
    let mut server_b = Some(common::spawn_mock_h2_server(addr_b).await);

    let uri_a: http::Uri = endpoint_a.parse().expect("valid endpoint a uri");
    let uri_b: http::Uri = endpoint_b.parse().expect("valid endpoint b uri");
    updater
        .send(Change::Insert(
            uri_a,
            Endpoint::from_shared(endpoint_a.clone()).expect("valid tonic endpoint a"),
        ))
        .await
        .expect("failed to insert endpoint a");
    updater
        .send(Change::Insert(
            uri_b,
            Endpoint::from_shared(endpoint_b.clone()).expect("valid tonic endpoint b"),
        ))
        .await
        .expect("failed to insert endpoint b");

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = stats_registry.endpoint_status_snapshot();
            let a_active = snapshot
                .iter()
                .any(|(k, v)| k.contains(endpoint_a.as_str()) && *v == EndpointStatus::Active);
            let b_active = snapshot
                .iter()
                .any(|(k, v)| k.contains(endpoint_b.as_str()) && *v == EndpointStatus::Active);
            if a_active && b_active {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for both endpoints to become active");

    let total_requests = 1000usize;
    let first_phase_requests = 500usize;

    for _ in 0..first_phase_requests {
        let req = http::Request::builder()
            .uri("http://mock.local/ping")
            .body(tonic::body::Body::empty())
            .expect("failed to build request");
        let resp = channel
            .ready()
            .await
            .expect("channel should be ready")
            .call(req)
            .await;
        assert!(
            resp.is_ok(),
            "request should succeed with two healthy endpoints"
        );
    }

    let a_after_500 = stats_registry
        .get_call_count(endpoint_a.as_str())
        .unwrap_or(0);
    let b_after_500 = stats_registry
        .get_call_count(endpoint_b.as_str())
        .unwrap_or(0);
    assert_eq!(a_after_500 + b_after_500, first_phase_requests);

    let min_share_after_500 = first_phase_requests / 5; // 20%
    assert!(
        a_after_500 >= min_share_after_500,
        "endpoint a received too few first-phase requests: {a_after_500}/{first_phase_requests}"
    );
    assert!(
        b_after_500 >= min_share_after_500,
        "endpoint b received too few first-phase requests: {b_after_500}/{first_phase_requests}"
    );

    // Kill the endpoint with fewer first-phase requests so the survivor ends with a clear +500 skew.
    let killed_a = a_after_500 <= b_after_500;
    if killed_a {
        server_a
            .take()
            .expect("server a should exist")
            .shutdown()
            .await;
    } else {
        server_b
            .take()
            .expect("server b should exist")
            .shutdown()
            .await;
    }

    let mut successful_requests = first_phase_requests;
    let deadline = Instant::now() + Duration::from_secs(30);
    while successful_requests < total_requests && Instant::now() < deadline {
        let req = http::Request::builder()
            .uri("http://mock.local/ping")
            .body(tonic::body::Body::empty())
            .expect("failed to build request");

        match channel.ready().await {
            Ok(ready) => {
                if ready.call(req).await.is_ok() {
                    successful_requests += 1;
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    assert_eq!(
        successful_requests, total_requests,
        "expected to complete {total_requests} successful requests within 30 seconds"
    );

    let a_final = stats_registry
        .get_call_count(endpoint_a.as_str())
        .unwrap_or(0);
    let b_final = stats_registry
        .get_call_count(endpoint_b.as_str())
        .unwrap_or(0);
    assert!(
        a_final + b_final >= total_requests,
        "expected at least {total_requests} attributed calls, got {}",
        a_final + b_final
    );

    let skew = a_final.abs_diff(b_final);
    assert!(
        skew >= 500,
        "expected at least 500 request skew after failover, got {skew} (a={a_final}, b={b_final})"
    );

    if let Some(server) = server_a.take() {
        server.shutdown().await;
    }
    if let Some(server) = server_b.take() {
        server.shutdown().await;
    }
}
