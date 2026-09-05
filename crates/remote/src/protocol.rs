use anyhow::Result;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use prost::Message as _;
use rpc::proto::Envelope;

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct MessageId(pub u32);

pub type MessageLen = u32;
pub const MESSAGE_LEN_SIZE: usize = size_of::<MessageLen>();

pub fn message_len_from_buffer(buffer: &[u8]) -> MessageLen {
    MessageLen::from_le_bytes(buffer.try_into().unwrap())
}

pub async fn read_message_with_len<S: AsyncRead + Unpin>(
    stream: &mut S,
    buffer: &mut Vec<u8>,
    message_len: MessageLen,
) -> Result<Envelope> {
    buffer.resize(message_len as usize, 0);
    stream.read_exact(buffer).await?;
    Ok(Envelope::decode(buffer.as_slice())?)
}

pub async fn read_message<S: AsyncRead + Unpin>(
    stream: &mut S,
    buffer: &mut Vec<u8>,
) -> Result<Envelope> {
    buffer.resize(MESSAGE_LEN_SIZE, 0);
    stream.read_exact(buffer).await?;

    let len = message_len_from_buffer(buffer);

    read_message_with_len(stream, buffer, len).await
}

pub async fn write_message<S: AsyncWrite + Unpin>(
    stream: &mut S,
    buffer: &mut Vec<u8>,
    message: Envelope,
) -> Result<()> {
    let message_len = message.encoded_len() as u32;
    stream
        .write_all(message_len.to_le_bytes().as_slice())
        .await?;
    buffer.clear();
    buffer.reserve(message_len as usize);
    message.encode(buffer)?;
    stream.write_all(buffer).await?;
    Ok(())
}

/// Encodes one envelope exactly as [`write_message`] would write it to a stream:
/// `u32 LE length || prost bytes`. One WebSocket binary frame carries one such buffer.
pub fn encode_envelope_frame(message: &Envelope) -> Vec<u8> {
    let message_len = message.encoded_len() as MessageLen;
    let mut buffer = Vec::with_capacity(MESSAGE_LEN_SIZE + message_len as usize);
    buffer.extend_from_slice(&message_len.to_le_bytes());
    // `encode` only fails when the buffer has insufficient capacity, which a `Vec` never does.
    message
        .encode(&mut buffer)
        .expect("Vec<u8> has unbounded capacity");
    buffer
}

/// Inverse of [`encode_envelope_frame`]. Rejects a buffer shorter than the length prefix, a
/// prefix that does not match `bytes.len() - MESSAGE_LEN_SIZE`, and frames larger than
/// `max_len` bytes (prefix included).
pub fn decode_envelope_frame(bytes: &[u8], max_len: usize) -> Result<Envelope> {
    anyhow::ensure!(
        bytes.len() >= MESSAGE_LEN_SIZE,
        "envelope frame is shorter than its length prefix ({} bytes)",
        bytes.len()
    );
    anyhow::ensure!(
        bytes.len() <= max_len,
        "envelope frame of {} bytes exceeds the {max_len} byte limit",
        bytes.len()
    );
    let (prefix, payload) = bytes.split_at(MESSAGE_LEN_SIZE);
    let message_len = message_len_from_buffer(prefix) as usize;
    anyhow::ensure!(
        message_len == payload.len(),
        "envelope frame length prefix ({message_len}) does not match payload length ({})",
        payload.len()
    );
    Ok(Envelope::decode(payload)?)
}

pub async fn write_size_prefixed_buffer<S: AsyncWrite + Unpin>(
    stream: &mut S,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    let len = buffer.len() as u32;
    stream.write_all(len.to_le_bytes().as_slice()).await?;
    stream.write_all(buffer).await?;
    Ok(())
}

pub async fn read_message_raw<S: AsyncRead + Unpin>(
    stream: &mut S,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    buffer.resize(MESSAGE_LEN_SIZE, 0);
    stream.read_exact(buffer).await?;

    let message_len = message_len_from_buffer(buffer);
    buffer.resize(message_len as usize, 0);
    stream.read_exact(buffer).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rpc::proto::{self, EnvelopedMessage as _};

    fn sample_envelope() -> Envelope {
        proto::Test { id: 42 }.into_envelope(7, Some(3), None)
    }

    #[test]
    fn encode_envelope_frame_matches_stream_framing() {
        let envelope = sample_envelope();
        let bytes = encode_envelope_frame(&envelope);

        let mut buffer = Vec::new();
        let read_back = smol::block_on(read_message(&mut &bytes[..], &mut buffer)).unwrap();
        assert_eq!(read_back, envelope);

        let decoded = decode_envelope_frame(&bytes, 1024).unwrap();
        assert_eq!(decoded, envelope);

        let mut written = Vec::new();
        let mut scratch = Vec::new();
        smol::block_on(write_message(&mut written, &mut scratch, envelope)).unwrap();
        assert_eq!(written, bytes);
    }

    #[test]
    fn decode_envelope_frame_rejects_malformed_frames() {
        assert!(decode_envelope_frame(&[1, 2, 3], 1024).is_err());

        let mut bytes = encode_envelope_frame(&sample_envelope());
        let len = bytes.len();
        assert!(decode_envelope_frame(&bytes, len - 1).is_err());

        bytes[0] = bytes[0].wrapping_add(1);
        assert!(decode_envelope_frame(&bytes, 1024).is_err());
    }
}
