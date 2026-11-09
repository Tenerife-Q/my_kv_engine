use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::sink::SinkExt;
use tokio_stream::StreamExt;
use std::io::{self, Write};
use tokio::io::{AsyncBufReadExt, BufReader};

use my_kv_engine::codec::ClientCodec;
use my_kv_engine::cmd::Command;

// 本地语法解析器：将终端输入的一行字符串，变成强类型的 Command
fn parse_command(input: &str) -> Option<Command> {
    let parts: Vec<&str> = input.trim().split_whitespace().collect();
    match parts.as_slice() {
        ["set", key, value] => Some(Command::Set {
            key: key.to_string(),
            value: value.to_string(),
        }),
        // TODO: 仿照上面，把 get, rm, compact 的模式匹配补全，如果匹配失败返回 None
        ["get", key] => Some(Command::Get {
            key: key.to_string(),
        }),
        ["rm", key] => Some(Command::Remove {
            key: key.to_string(),
        }),
        ["compact"] => Some(Command::Compact),
        _ => None,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect("127.0.0.1:6379").await?;
    println!("Connected to KV server (v2).");

    // 绑定客户端专属的编解码器
    let mut framed = Framed::new(stream, ClientCodec);

    let stdin = tokio::io::stdin();
    let mut stdin_reader = BufReader::new(stdin);
    let mut input_buf = String::new();

    loop {
        print!("my-kv-v2> ");
        io::stdout().flush()?;
        input_buf.clear();

        let n = stdin_reader.read_line(&mut input_buf).await?;
        if n == 0 { 
            break; 
        }

        let text = input_buf.trim();
        
        if text.is_empty() { 
            continue;
        }
        if text == "quit" || text == "exit" {
            println!("Bye!");
            break;
        }

        // 1. 本地解析命令
        let cmd = match parse_command(text) {
            Some(c) => c,
            None => {
                println!("-ERR Invalid command format locally");
                continue;
            }
        };

        // 2. 发送给服务端（底层的 Encoder 会自动把它变成长度前缀的 JSON 字节流）
        framed.send(cmd).await?;

        // 3. 等待服务端的响应（底层的 Decoder 会自动处理半包粘包，解出 String）
        match framed.next().await {
            Some(Ok(response)) => println!("{}", response),
            Some(Err(e)) => eprintln!("-ERR Network/Codec error: {}", e),
            None => {
                println!("Server closed connection.");
                break;
            }
        }
    }

    Ok(())
}