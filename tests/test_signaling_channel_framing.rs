//! Tests for signaling channel framing over TCP.
//!
//! When multiple signaling messages are sent rapidly over TCP, they may be
//! coalesced into a single segment. The library's TransportSignalingChannel
//! now uses newline-delimited framing with buffered recv to handle this.

mod common;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn test_transport_signaling_channel_handles_coalesced_messages() {
    use ego_transport::transport::rtc_signaling::*;

    ego_platform::init();

    let relay_addr = "127.0.0.1:19984";
    let _relay = tokio::spawn(async {
        common::test_harness::run_dumb_relay(relay_addr).await;
    });
    ego_platform::sleep(std::time::Duration::from_millis(200)).await;

    let sender_transport = ego_transport::transport::connect(relay_addr)
        .await
        .expect("sender connect failed");
    ego_platform::sleep(std::time::Duration::from_millis(100)).await;
    let receiver_transport = ego_transport::transport::connect(relay_addr)
        .await
        .expect("receiver connect failed");

    let mut sender = TransportSignalingChannel::new(sender_transport);
    let mut receiver = TransportSignalingChannel::new(receiver_transport);

    // Send 3 messages rapidly — TCP will likely coalesce them
    let room = "framing-test";
    let sdp = SdpBuilder::new().build_offer();
    sender
        .send_signal(&SignalingMessage::offer(room, &sdp))
        .await
        .unwrap();
    let ice = IceCandidate::new(
        "candidate:1 1 udp 2130706431 10.0.0.1 5000 typ host",
        "0",
        0,
    );
    sender
        .send_signal(&SignalingMessage::ice(room, &ice))
        .await
        .unwrap();
    sender
        .send_signal(&SignalingMessage::ice_done(room))
        .await
        .unwrap();

    // Give the relay time to forward
    ego_platform::sleep(std::time::Duration::from_millis(200)).await;

    // Receive all 3
    let mut received = Vec::new();
    for _ in 0..3 {
        match ego_platform::timeout(std::time::Duration::from_secs(2), receiver.recv_signal()).await
        {
            Ok(Ok(msg)) => received.push(msg.kind),
            Ok(Err(e)) => {
                log::error!("recv error: {:?}", e);
                break;
            }
            Err(_) => {
                log::warn!("recv timed out");
                break;
            }
        }
    }

    log::info!("Received {}/3 messages: {:?}", received.len(), received);

    assert_eq!(
        received.len(),
        3,
        "TransportSignalingChannel should receive all 3 coalesced messages"
    );
    assert_eq!(received[0], SignalingKind::Offer);
    assert_eq!(received[1], SignalingKind::Ice);
    assert_eq!(received[2], SignalingKind::IceDone);
}
