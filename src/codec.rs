// src/codec.rs
use bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;
use crate::cmd::Command;
use crate::error::{KvError, Result};

pub struct CommandCodec;

impl Decoder for CommandCodec {
    // 明确告诉框架：我这一顿操作，最终要吐出什么类型的数据？
    type Item = Command;
    type Error = KvError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        // 1. 检查缓冲区是否有足够的字节读取 Header（4字节）
        if src.len() < 4 {
            // 返回 Ok(None) 告诉 Tokio：包没完整，别催，底层 TCP 继续去读数据。
            return Ok(None);
        }

        // 2. Peek（探查）长度。
        // 警告：这里绝对不能直接使用 src.get_u32()！
        // 因为 get_u32() 会移动 src 的读指针，直接把这 4 个字节“吃掉”了。
        // 如果这时候 Payload 还没到齐，你返回 None 等待下一次数据，这 4 字节的长度前缀就永久丢失了！
        // 正确做法：只看前 4 个字节的切片，不修改原 src。
        let mut length_bytes = &src[..4];
        let payload_len = length_bytes.get_u32() as usize;
        // get_u32()将前 4 个字节解释为一个大端序的 u32 整数，并返回该整数值。
        // 大端序 先读到的内存字节放在数据的高位，后读到的字节放在低位。
        // 内存地址的从小到大排列（读取顺序先后排列），大端序也就是从左到右
        // e.g., 如果前 4 个字节是 0x00 0x00 0x04 0x00，get_u32() as usize 会返回 0x00000400 = 1024。
        // 那么 payload_len 就是 0x00000400 = 1024。

        if payload_len > 8 * 1024 * 1024 {
            return Err(KvError::ParseError("Payload is too large".to_string()));
        }

        // 3. 计算一帧的总长度
        let frame_len = 4 + payload_len;

        // 4. 判断半包：数据到底齐没齐？
        if src.len() < frame_len {
            // Payload 还没接收完，继续等。
            return Ok(None);
        }

        // 5. 走到这里，说明 src 里面【至少】包含了一个完整的帧（处理粘包）。
        // 此时，必须把读指针往前推进 4 字节，正式吃掉 Header。
        // TODO: 调用 src 的什么方法可以安全地推进读指针？（提示：见我让你查阅的文档）
        src.advance(4);

        // 6. 精准切割出 Payload。
        // TODO: 调用 src 的什么方法可以切出 payload_len 长度的内存块？
        // 这个方法会返回一个新的 BytesMut，而且不会发生深拷贝。
        // Afterwards self contains elements [at, len), and the returned BytesMut contains elements [0, at).
        // 之后，self剩下的字节就是原 src 中从 payload_len 开始的部分，而返回的 BytesMut 包含原 src 中从 0 到 payload_len 的部分。
        let payload_bytes = src.split_to(payload_len);

        // 7. 将字节数组反序列化为强类型的 Command
        // 因为你目前还没引入更高级的二进制序列化库，我们暂且用 serde_json 把 bytes 当 JSON 字符串解析。
        let cmd: Command = serde_json::from_slice(&payload_bytes)?;

        // 成功提取一帧！返回给业务层。
        Ok(Some(cmd))
    }
}


use tokio_util::codec::Encoder;
use bytes::BufMut; // 注意：写操作需要引入 BufMut trait

// TODO: 假设我们要发回给客户端的不再是 Command，而是普通的字符串（或者是将来定义的 Response 枚举）。
// 为了简单，我们目前把 Item 设为 String。
impl Encoder<String> for CommandCodec {
    type Error = KvError;

    fn encode(&mut self, item: String, dst: &mut BytesMut) -> Result<()> {
        let payload_bytes = item.as_bytes();
        let payload_len = payload_bytes.len();

        // 1. TODO: 检查 payload_len 是否超过限制（如 8MB），超限则返回错误。
        if payload_len > 8 * 1024 * 1024 {
            return Err(KvError::ParseError("Payload is too large".to_string()));
        }

        // 2. 预先为底层的 BytesMut 分配足够的容量，避免多次扩容带来的性能损耗。
        // 总长度 = 4字节长度前缀 + payload长度
        dst.reserve(4 + payload_len);

        // 3. TODO: 将 payload_len 作为大端序 u32 写入 dst。
        // 查阅文档关键词：bytes::BufMut::put_u32
        dst.put_u32(payload_len as u32);

        // 4. TODO: 将真正的字节序列追加到 dst 中。
        // 查阅文档关键词：bytes::BufMut::put_slice
        dst.put_slice(payload_bytes);

        Ok(())
    }
}




pub struct ClientCodec;

// 客户端的 Decoder：负责解析服务端发回来的响应 (String)
impl Decoder for ClientCodec {
    type Item = String; // 吐出 String
    type Error = KvError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.len() < 4 {
             return Ok(None); 
        }

        let mut length_bytes = &src[..4];
        let payload_len = length_bytes.get_u32() as usize;

        if payload_len > 8 * 1024 * 1024 {
            return Err(KvError::ParseError("Payload too large".to_string()));
        }

        if src.len() < 4 + payload_len { 
            return Ok(None); 
        }

        src.advance(4);

        let payload_bytes = src.split_to(payload_len);

        // TODO: 将 payload_bytes (BytesMut) 转换为 String。
        // 提示：你可以用 String::from_utf8(payload_bytes.to_vec())，
        // 或者更高效地用 std::str::from_utf8(&payload_bytes) 然后 .to_string()
        let response = std::str::from_utf8(&payload_bytes)
            .map_err(|_| KvError::ParseError("Invalid UTF-8".to_string()))?
            .to_string();

        Ok(Some(response))
    }
}


// use tokio_util::codec::Encoder;
// 客户端的 Encoder：负责把终端里生成的 Command 发往服务端
impl Encoder<Command> for ClientCodec {
    type Error = KvError;

    fn encode(&mut self, item: Command, dst: &mut BytesMut) -> Result<()> {
        // 1. 将 Command 序列化为 JSON 字节数组 (Vec<u8>)
        let payload_bytes = serde_json::to_vec(&item)?;
        let payload_len = payload_bytes.len();

        // TODO: 参照你之前写的 CommandCodec::encode，把长度前缀和 payload_bytes 写入 dst 中
        // 1. 检查超限
        if payload_len > 8 * 1024 * 1024 {
            return Err(KvError::ParseError("Payload is too large".to_string()));
        }
        // 2. dst.reserve
        dst.reserve(4 + payload_len);
        // 3. dst.put_u32
        dst.put_u32(payload_len as u32);
        // 4. dst.put_slice
        dst.put_slice(&payload_bytes);

        Ok(())
    }
}