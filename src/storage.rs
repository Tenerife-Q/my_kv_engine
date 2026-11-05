use crate::error::{KvError, Result};
use std::collections::HashMap;
use std::sync::Arc;

use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock}; // 注意：引入了 Tokio 的 Mutex 用来锁文件句柄
use tokio::io::{AsyncBufReadExt, BufReader};


// trait version 1.0
// pub trait StorageEngine {
//     fn set(&mut self, key: String, value: String) -> Result<()>;
//     fn get(&self, key: String) -> Result<Option<String>>;
//     fn remove(&mut self, key: String) -> Result<()>;
// }


// trait version 2.0
// pub trait StorageEngine {
//     fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> Result<()>;
//     fn get(&self, key: &str) -> Result<Option<String>>; // 思考一下：为什么 get 只需要 &str？
//     fn remove(&mut self, key: impl Into<String>) -> Result<()>;
// }

// trait version 3.0
// pub trait StorageEngine {
//     fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()>;
//     fn get(&self, key: &str) -> Result<Option<String>>;
//     fn remove(&self, key: impl Into<String>) -> Result<()>;
// }

#[allow(async_fn_in_trait)]
pub trait StorageEngine {
    async fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<String>>;
    async fn remove(&self, key: impl Into<String>) -> Result<()>;
}

#[derive(Clone)]
pub struct MemoryStorage {
    // Arc允许多个线程安全地共享内存，RwLock允许多个读者或一个写者访问数据。
    map: Arc<RwLock<HashMap<String, String>>>,
}

pub struct DiskStorage {
    map: Arc<RwLock<HashMap<String, String>>>,
    file_path: String,
    wal_file: Arc<Mutex<File>>,
}

// new() for DiskStorage version 1.0
// impl DiskStorage {
//     pub async fn new(path: impl Into<String>) -> Result<Self> {
//         let file_path = path.into();
//         let map = if std::path::Path::new(&file_path).exists() {
//             // let content = std::fs::read_to_string(&file_path)?;
//             let content = tokio::fs::read_to_string(&file_path).await?;
//             serde_json::from_str(&content)?
//         } else {
//             HashMap::new()
//         };
        
//         Ok(Self {
//             map: Arc::new(RwLock::new(map)),
//             file_path,
//         })
//     }
// }


// new() for DiskStorage version 2.0
impl DiskStorage {
    pub async fn new(path: impl Into<String>) -> Result<Self> {
        let file_path = path.into();
        let mut map = HashMap::new();

        // 1. 重放阶段：尝试以纯读模式打开文件
        // 查阅文档关键词：tokio::fs::File::open
        if let Ok(file) = tokio::fs::File::open(&file_path).await {
            let reader = BufReader::new(file);
            let mut lines = reader.lines(); // 必须引入 AsyncBufReadExt trait 才能用 lines()

            // 逐行读取文件
            while let Some(line) = lines.next_line().await? {
                // todo: 1. 按 '\t' 分割当前行，收集为 Vec<&str>
                let parts: Vec<&str> = line.split('\t').collect();

                // todo: 2. 模式匹配切片 (回忆 server.rs 里的写法)
                match parts.as_slice() {
                    ["S", key, value] => {
                        // 执行 map.insert
                        // 注意：你需要将 key 和 value 转换为 String 再存入，因为 map 的类型是 HashMap<String, String>
                        map.insert(key.to_string(), value.to_string());
                    },
                    ["D", key] => {
                        // 执行 map.remove
                        map.remove(*key);
                    },
                    _ => {
                        // 遇到格式不对的行，直接跳过或者记录警告日志，不要 panic
                    }
                }
            }
        }

        // 2. 准备阶段：重放结束后，为了后续追加写入，以追加模式（Create + Append）再次打开文件
        let append_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_path)
            .await?;

        Ok(Self {
            map: Arc::new(RwLock::new(map)),
            wal_file: Arc::new(Mutex::new(append_file)),
            file_path,
        })
    }

    pub async fn compact(&self) -> Result<()> {
        let tmp_path = format!("{}.tmp", self.file_path);

        let mut tmp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;

        {
            // 获取内存 RwLock 的读锁
            // 思考：为什么拿读锁？因为我们要遍历导出数据。
            // 代价：在此期间，所有的写操作（set/rm）会被阻塞，但读操作（get）可以并发进行。这叫 Stop-The-World (STW)。
            let locked_map = self.map.read().await;

            // 遍历locked_map内存中的 HashMap，将每个键值对写入临时文件tmp_file
            for (key, value) in locked_map.iter() {
                let log_line = format!("S\t{}\t{}\n", key, value);
                tmp_file.write_all(log_line.as_bytes()).await?;
            }

            // 强制刷盘，确保数据写入磁盘
            tmp_file.flush().await?;
        }


        // 原子替换文件
        {
            // 获取文件的独占锁，确保此刻没有 set 正在写入旧文件
            let mut wal = self.wal_file.lock().await;
            
            // 执行操作系统的原子重命名
            tokio::fs::rename(&tmp_path, &self.file_path).await?;

            // 重新以追加模式打开新的 data.log
            let new_file = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(&self.file_path)
                .await?;
            
            // 将锁里的旧文件句柄替换为新文件句柄！
            *wal = new_file;
        }

        Ok(())
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new()))
        }
    }
}

/*
implement version 1.0

impl StorageEngine for MemoryStorage {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key, value);
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        match self.map.get(&key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }
    /*
    fn get(&self, key: String) -> Result<Option<String>> {
        // get 返回 Option<&String>
        // .cloned() 把它变成了 Option<String>
        // Ok() 把整个东西包成了 Result
        Ok(self.map.get(&key).cloned()) 
    }
    */

    fn remove(&mut self, key: String) -> Result<()> {
        match self.map.remove(&key) {
            Some(_) => Ok(()),
            None => Err(KvError::KeyNotFound("Key not found".to_string())),
        }
    }
}
*/


/*
implement version 2.0

impl StorageEngine for MemoryStorage {
    fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        self.map.insert(key.into(), value.into());
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<String>> {
        Ok(self.map.get(key).cloned()) 
        // get 返回 Option<&String>，.cloned() 把它变成了 Option<String>，Ok() 把整个东西包成了 Result
    }

    fn remove(&mut self, key: impl Into<String>) -> Result<()> {
        let key = key.into();
        match self.map.remove(&key) {
            Some(_) => Ok(()),
            None => Err(KvError::KeyNotFound(key)), // 返回 KeyNotFound 错误
        }
    }
}


impl StorageEngine for DiskStorage {
    fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        self.map.insert(key.into(), value.into());
        let content = serde_json::to_string(&self.map)?;
        std::fs::write(&self.file_path, content)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<String>> {
        match self.map.get(key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: impl Into<String>) -> Result<()> {
        let k = key.into();
        match self.map.remove(&k) {
            Some(_) => {
                let content = serde_json::to_string(&self.map)?;
                //正因为remove，内存中的map已经被修改了，所以需要重新写入硬盘
                std::fs::write(&self.file_path, content)?;
                Ok(())
            },
            None => Err(KvError::KeyNotFound(k)),

        }
    }
}
*/

impl StorageEngine for MemoryStorage {
    async fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        // let mut locked_map = self.map.write().unwrap(); // 获取写锁
        let mut locked_map = self.map.write().await;
        locked_map.insert(key.into(), value.into());
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        // let locked_map = self.map.read().unwrap(); // 获取读锁
        let locked_map = self.map.read().await;
        Ok(locked_map.get(key).cloned())
    }

    async fn remove(&self, key: impl Into<String>) -> Result<()> {
        // let mut locked_map = self.map.write().unwrap();
        let mut locked_map = self.map.write().await;
        let k = key.into();
        match locked_map.remove(&k) {
            Some(_) => Ok(()),
            None => Err(KvError::KeyNotFound(k)),
        }
    }
}
// unwrap()
// 一旦线程 A 在持锁期间崩溃，Rust 会立刻将这个锁标记为**“已投毒（Poisoned）”**。
// 当线程 B 来获取锁时，Rust 不会给他锁，而是返回一个 Err(PoisonError)，
// 警告他：“上一个拿锁的人死在里面了，数据可能脏了，你千万别碰！”
// 那我们为什么要 unwrap() 呢？
// 因为在绝大多数后端工程中，如果一个共享的数据结构被写崩溃了，说明代码有致命 Bug。
// 此时最安全的做法就是“让当前线程也跟着一起崩溃（Panic）”，也就是 Fail Fast（快速失败）。
// 直接 unwrap() 遇到 Err 就会触发 Panic，
// 这正是我们期望的——与其继续操作脏数据，不如直接挂掉报错，触发服务器重启。
// 这就是为什么在锁的操作上，标准做法全是 unwrap()！



/*
impl for DiskStorage version 3.0

impl StorageEngine for DiskStorage {

    /*
    async fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        // let mut locked_map = self.map.write().unwrap(); // 获取写锁
        let mut locked_map = self.map.write().await;
        locked_map.insert(key.into(), value.into());

        // 将内存中的 HashMap 序列化为 JSON 字符串，并写入文件
        // &*locked_map 解引用 Arc<RwLock<HashMap<...>>>，得到 HashMap 的引用
        let content = serde_json::to_string(&*locked_map)?;
        // std::fs::write(&self.file_path, content)?;

        // 但是上面有一把全局写锁还没释放，tokio::fs::write 是执行很慢的网络/磁盘io
        // 如果我们在持锁期间去做慢操作，其他线程像是get读取数据就会被阻塞，性能会很差。
        // 所以我们要把锁的作用域缩小，先把内存数据序列化成字符串，然后释放锁，再去写磁盘。
        // 这样其他线程就可以在我们写磁盘的时候去读取数据了，性能就会好很多。
        // 但是我们要注意，写磁盘的过程中，其他线程可能又去 set 了新的数据，导致我们写磁盘的时候把新数据覆盖掉了。
        // 这是一个典型的“读-改-写”竞态条件（read-modify-write race condition），
        // 我们要在文档里明确说明这个问题，或者在设计上避免这个问题，比如使用事务或者版本号等机制。
        // 这里我们就简单地在文档里说明这个问题，暂时不做复杂的处理。
        //
        // 另外，tokio::fs::write 是异步的，如果我们在持锁期间去 await 一个异步操作，可能会导致死锁，
        // 因为 tokio 的调度器可能会在我们持锁期间切换到其他任务，而其他任务可能也在等待这个锁，导致死锁。
        // 为了避免这个问题，我们要在持锁期间不要去 await 异步操作，所以我们要把锁的作用域缩小，
        // 先把内存数据序列化成字符串，然后释放锁，再去写磁盘。
        // 具体修改见下面的代码。
        tokio::fs::write(&self.file_path, content).await?;
        Ok(())
    }
    */


    async fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        let content = { // 开辟一个小作用域
            let mut locked_map = self.map.write().await;// 获取写锁

            // 将内存中的 HashMap 序列化为 JSON 字符串，并写入文件
            // &*locked_map 解引用 Arc<RwLock<HashMap<...>>>，得到 HashMap 的引用
            locked_map.insert(key.into(), value.into());
            serde_json::to_string(&*locked_map)?
        }; // 离开作用域，locked_map 自动销毁，写锁瞬间释放！
        
        // 此时别的客户端已经可以并发去 get 数据了！
        // 我们自己在这个任务里慢慢去写磁盘
        tokio::fs::write(&self.file_path, content).await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        // let locked_map = self.map.read().unwrap(); // 获取读锁
        let locked_map = self.map.read().await;
        match locked_map.get(key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }

    async fn remove(&self, key: impl Into<String>) -> Result<()> {
        let key = key.into();
        // let mut locked_map = self.map.write().unwrap();
        let mut locked_map = self.map.write().await;
        match locked_map.remove(&key) {
            Some(_) => {
                let content = serde_json::to_string(&*locked_map)?;
                // std::fs::write(&self.file_path, content)?;
                tokio::fs::write(&self.file_path, content).await?;
                Ok(())
            }
            None => Err(KvError::KeyNotFound(key)),
        }
    }
}
*/

impl StorageEngine for DiskStorage {
    async fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        let k = key.into();
        let v = value.into();

        // 1. 格式化日志行。格式标准："S\t{key}\t{value}\n"
        let log_line = format!("S\t{}\t{}\n", k, v);

        // 2. 写入 WAL 文件 (先写磁盘)
        {
            // todo: 获取 wal_file 的 Mutex 锁 (需要 await)
            let mut file = self.wal_file.lock().await;
            
            // todo: 将 log_line 转换为字节切片，并使用 write_all 写入
            file.write_all(log_line.as_bytes()).await?;
            file.flush().await?;// 确保数据写入磁盘 AsyncWriteExt trait 提供 flush 方法
        } // 离开作用域，自动释放文件互斥锁

        // 3. 更新内存索引 (后更新内存)
        {
            // todo: 获取 map 的 RwLock 写锁，执行 insert
            let mut locked_map = self.map.write().await;
            locked_map.insert(k, v);
        }
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        let locked_map = self.map.read().await;
        match locked_map.get(key) {
            Some(val) => Ok(Some(val.clone())),
            None => Ok(None),
        }
    }

    async fn remove(&self, key: impl Into<String>) -> Result<()> {
        let k = key.into();
        let log_line = format!("D\t{}\n", k);

        // 错误写法
        // {
        //     let mut locked_map = self.map.write().await;
        //     match locked_map.remove(&k) {
        //         Some(_) => {
        //             let mut file = self.wal_file.lock().await;
        //             file.write_all(log_line.as_bytes()).await?;
        //             Ok(())
        //         },
        //         None => Err(KvError::KeyNotFound(k)),
        //     }
        // }

        {
            let mut file = self.wal_file.lock().await;
            file.write_all(log_line.as_bytes()).await?;
            file.flush().await?;// 确保数据写入磁盘 AsyncWriteExt trait 提供 flush 方法
        }

        {
            let mut locked_map = self.map.write().await;
            match locked_map.remove(&k) {
                Some(_) => Ok(()),
                None => Err(KvError::KeyNotFound(k)),
            }
        }

    }
}
