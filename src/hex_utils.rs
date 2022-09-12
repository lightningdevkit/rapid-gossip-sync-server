use bitcoin::secp256k1::PublicKey;

pub fn to_vec(hex: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(hex.len() / 2);

    let mut b = 0;
    for (idx, c) in hex.as_bytes().iter().enumerate() {
        b <<= 4;
        match *c {
            b'A'..=b'F' => b |= c - b'A' + 10,
            b'a'..=b'f' => b |= c - b'a' + 10,
            b'0'..=b'9' => b |= c - b'0',
            _ => return None,
        }
        if (idx & 1) == 1 {
            out.push(b);
            b = 0;
        }
    }

    Some(out)
}

pub fn to_composite_index(scid: i64, timestamp: i64, direction: bool) -> String {
	let scid_be = scid.to_be_bytes();
	let res = format!("{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}:{}:{}",
		scid_be[0], scid_be[1], scid_be[2], scid_be[3],
		scid_be[4], scid_be[5], scid_be[6], scid_be[7],
		timestamp, direction as u8);
	assert_eq!(res.len(), 29); // Our SQL Type requires len of 29
	res
}

pub fn to_compressed_pubkey(hex: &str) -> Option<PublicKey> {
    let data = match to_vec(&hex[0..33 * 2]) {
        Some(bytes) => bytes,
        None => return None,
    };
    match PublicKey::from_slice(&data) {
        Ok(pk) => Some(pk),
        Err(_) => None,
    }
}
