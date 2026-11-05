// use tokio::net::TcpStream;
// use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
// use std::io::{self, Write};

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // 1. 建立与服务端的 TCP 连接
//     let mut stream = TcpStream::connect("127.0.0.1:6379").await?;
//     println!("Connected to KV server.");

//     // 2. 准备一个标准输入的读取器 (用于接收键盘输入)
//     let stdin = io::stdin();
    
//     // 3. 准备读写缓冲区
//     let mut input_buf = String::new();
//     let mut response_buf = String::new();

//     loop {
//         // 4. 打印交互式提示符，并强制刷新标准输出
//         print!("my-kv> ");
//         io::stdout().flush()?;

//         input_buf.clear();
//         response_buf.clear();

//         // 5. 阻塞读取键盘输入
//         // 提示：std::io::stdin().read_line 是阻塞的，但在简单的客户端场景可以接受。
//         // 如果想极致全异步，可以查阅 tokio::io::stdin()
//         stdin.read_line(&mut input_buf)?;

//         let text = input_buf.trim();
//         if text.is_empty() {
//             continue;
//         }

//         if text == "quit" || text == "exit" {
//             println!("Bye!");
//             break;
//         }

//         // 6. 将命令追加 \n 并发送给服务器
//         let cmd = format!("{}\n", text);
//         stream.write_all(cmd.as_bytes()).await?;

//         // 7. 读取服务器的响应
//         // 注意：你需要把 &mut stream 临时包进 BufReader 来读取一行
//         let mut reader = BufReader::new(&mut stream);
//         reader.read_line(&mut response_buf).await?;// read_line作用: 从缓冲区中读取一行数据，直到遇到换行符 '\n' 为止。
//         // 这里reader.read_line 会把服务器返回的一行数据读取到 response_buf 中。

//         // 8. 打印响应结果
//         print!("{}", response_buf);
//     }

//     Ok(())
// }

use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
use std::io::{self, Write};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await?;
    println!("Connected to KV server.");

    // 1. 获取全异步的 stdin
    let stdin = tokio::io::stdin();
    // 2. 将其用 BufReader 包裹，以便使用 read_line()
    let mut stdin_reader = BufReader::new(stdin);
    
    let mut input_buf = String::new();
    let mut response_buf = String::new();

    loop {
        print!("my-kv> ");
        // 标准输出没有提供异步刷新，简单的 flush 还是可以接受的，因为它几乎不阻塞
        io::stdout().flush()?; 

        input_buf.clear();
        response_buf.clear();

        // 3. 全异步读取标准输入！当前线程不会被阻塞，Tokio 调度器可以去干别的
        let n = stdin_reader.read_line(&mut input_buf).await?;
        if n == 0 {
            // EOF (如 Ctrl+D) 退出
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

        // 格式化并发送
        let cmd = format!("{}\n", text);
        stream.write_all(cmd.as_bytes()).await?;

        // 接收响应
        let mut stream_reader = BufReader::new(&mut stream);
        stream_reader.read_line(&mut response_buf).await?;

        print!("{}", response_buf);
    }

    Ok(())
}