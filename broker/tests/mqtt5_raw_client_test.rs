mod common;

#[test]
fn mqtt5_raw_helpers_build_connect_and_subscribe_packets() {
    let connect = common::mqtt5::connect_packet("task-harness", true, 30);
    assert_eq!(connect[0], 0x10);
    assert_eq!(&connect[2..8], &[0x00, 0x04, b'M', b'Q', b'T', b'T']);
    assert_eq!(connect[8], 0x05);

    let subscribe = common::mqtt5::subscribe_packet(1, "test/mqtt5/task", 1);
    assert_eq!(subscribe[0], 0x82);

    assert_eq!(common::mqtt5::disconnect_packet(), vec![0xE0, 0x00]);
    assert_eq!(common::mqtt5::pingreq_packet(), vec![0xC0, 0x00]);
}

#[test]
fn mqtt5_raw_helpers_parse_basic_server_packets() {
    let connack =
        common::mqtt5::parse_connack(&[0x20, 0x03, 0x00, 0x00, 0x00]).expect("parse connack");
    assert_eq!(connack.reason_code, 0);
    assert!(!connack.session_present);

    let puback = common::mqtt5::parse_puback(&[0x40, 0x02, 0x00, 0x01]).expect("parse puback");
    assert_eq!(puback.packet_id, 1);
    assert_eq!(puback.reason_code, 0);
}
