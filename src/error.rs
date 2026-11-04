use std::io;
use std::fmt;
use std::error::Error;

#[derive(Debug)]
pub enum KvError {
    KeyNotFound(String),
    IoError(io::Error),
    ParseError(String),
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyNotFound(s) => write!(f, "KvError is KeyNotFoundError, which detailed is {}", s),
            Self::IoError(r) => write!(f, "KvError is IoError {}", r),
            Self::ParseError(s) => write!(f, "KvError is ParseError, which detailed is {}", s),
        }
    }
}

impl From<io::Error> for KvError {
    fn from(error: io::Error) -> Self {
        KvError::IoError(error)
    }
}

impl From<serde_json::Error> for KvError {
    fn from(error: serde_json::Error) -> Self {
        KvError::ParseError(error.to_string())
    }
}

impl Error for KvError {}

pub type Result<T> = std::result::Result<T, KvError>;
// Rust 标准库自带的 Result 长这样：Result<T, E>（T 是成功的值，E 是错误的类型）。
// 如果你不搞别名，你的 main 函数就要这么写：
// fn main() -> std::result::Result<(), my_kv_engine::error::KvError>
// 你的 get 函数就要这么写：
// fn get() -> std::result::Result<Option<String>, my_kv_engine::error::KvError>
// 是不是太长、太啰嗦了？
// 所以，我们用 type 关键字“包了一层”。把后面的 E 固定死为我们自己的 KvError，只留下前面的 T 让你填。
// 因此，你在 main 里写的 my_kv_engine::error::Result<()>，
// 在编译器眼里，等价于告诉它：“这个函数成功的话返回空 ()，失败的话就返回我写的 KvError！”
// 这就是为什么你的 ? 操作符能完美工作的原因：? 发现出错了，一看 main 要求的也是 KvError，直接就扔出去了。