// Uncomment this block to pass the first stage
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpListener,
    string::FromUtf8Error,
    sync::{Arc, Mutex, RwLock},
    thread,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    eprintln!("Logs from your program will appear here!");

    let store: Arc<RwLock<HashMap<String, Mutex<String>>>> = Arc::new(RwLock::new(HashMap::new()));

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                eprintln!("Connection from: {}", stream.peer_addr().unwrap());

                let store = Arc::clone(&store);
                thread::spawn(move || {
                    handle_connection(stream, store);
                });
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
    Set(String, String),
    Get(String),
}

// enum RespData {
//     SimpleString(String),
//     Error(String),
//     Integer(i64),
//     BulkString(Option<String>),
//     Array(Vec<RespData>),
// }

// "*2\r\n$4\r\necho\r\n$3\r\nhey\r\n"

#[derive(Debug)]
enum Resp {
    Array,
    BulkString,
}

impl Resp {
    fn from_byte(byte: u8) -> Option<Resp> {
        match byte {
            b'*' => Some(Resp::Array),
            b'$' => Some(Resp::BulkString),
            _ => None,
        }
    }
    // fn new(first_byte: u8) -> Option<Self> {}
}

fn get_bulkstring(
    buf: &mut [u8; 512],
    mut bytes_processed: usize,
) -> (Result<String, FromUtf8Error>, usize) {
    let mut size = vec![];
    dbg!(&bytes_processed);
    dbg!(&buf[bytes_processed]);
    while buf[bytes_processed] != 13 {
        size.push(buf[bytes_processed]);
        bytes_processed += 1;
    }

    let mut size_num: u64 = 0;
    for ch in size {
        size_num = (size_num * 10) + (ch as u64 - 48);
    }
    dbg!(&size_num);
    bytes_processed += 2;
    dbg!(&bytes_processed);
    (
        String::from_utf8(buf[bytes_processed..(bytes_processed + size_num as usize)].into()),
        bytes_processed,
    )
}

fn handle_connection(
    mut stream: std::net::TcpStream,
    store: Arc<RwLock<HashMap<String, Mutex<String>>>>,
) {
    let mut buf = [0; 512];
    loop {
        let bytes_read = stream.read(&mut buf).expect("Failed to read from client");

        if bytes_read == 0 {
            return;
        }
        dbg!(&buf[..bytes_read]);

        bytes_read.to_string();
        dbg!(&String::from_utf8_lossy(&buf[..bytes_read]).into_owned());

        let mut current_resp = None;
        let mut commands: Vec<Command> = vec![];
        let mut bytes_processed = 0;
        // let mut current_byte = 0;

        loop {
            if bytes_processed >= bytes_read {
                break;
            }

            // current_byte = bytes_processed;
            match current_resp {
                None => {
                    current_resp = Resp::from_byte(buf[bytes_processed]);
                    bytes_processed += 1;
                }

                Some(resp) => match resp {
                    Resp::Array => {
                        // let size_char = buf[bytes_processed] as char;
                        bytes_processed += 3; // Ignoring the terminator
                                              // let size: u32 = size_char.to_digit(10).expect("Not a digit in array size");
                                              // dbg!(&size);
                        current_resp = None;
                    }
                    Resp::BulkString => {
                        let result = get_bulkstring(&mut buf, bytes_processed);

                        bytes_processed = result.1; // processed the bulstring size
                        dbg!(&bytes_processed);
                        current_resp = None;

                        if let Ok(command) = result.0 {
                            dbg!(&command);
                            bytes_processed += 2 + command.len();
                            // dbg!(&bytes_processed);
                            // dbg!(&buf[bytes_processed]);
                            if command == "ping" {
                                commands.push(Command::Ping);
                            }
                            if command == "echo" {
                                bytes_processed += 1; // assuming it is bulk string
                                let result = get_bulkstring(&mut buf, bytes_processed);
                                let string = result.0.expect("Failed to parse bulk String");
                                bytes_processed = result.1; // processed the bulstring size
                                bytes_processed += 2 + string.len();
                                let echo_command = Command::Echo(string);
                                dbg!(&echo_command);
                                commands.push(echo_command);
                            }
                            if command == "set" {
                                bytes_processed += 1;
                                let result = get_bulkstring(&mut buf, bytes_processed);
                                let key = result.0.expect("Failed to parse bulk String");
                                bytes_processed = result.1; // processed the bulstring size
                                bytes_processed += 2 + key.len();
                                dbg!(&key);

                                bytes_processed += 1;
                                let result = get_bulkstring(&mut buf, bytes_processed);
                                let value = result.0.expect("Failed to parse bulk String");
                                bytes_processed = result.1; // processed the bulstring size
                                bytes_processed += 2 + value.len();
                                dbg!(&value);

                                commands.push(Command::Set(key, value));
                            }

                            if command == "get" {
                                bytes_processed += 1;
                                let result = get_bulkstring(&mut buf, bytes_processed);
                                let key = result.0.expect("Failed to parse bulk String");
                                bytes_processed = result.1; // processed the bulstring size
                                bytes_processed += 2 + key.len();
                                dbg!(&key);
                                commands.push(Command::Get(key));
                            }
                        }
                    }
                },
            }
            // if current_byte == bytes_processed {
            //     // bytes_processed += 1;
            // }
            println!("######## {}", bytes_processed);
        }

        // println!("Processed the request");
        let pong = b"+PONG\r\n";
        let ok = b"+OK\r\n";

        for command in commands {
            match command {
                Command::Ping => stream.write_all(pong).expect("Failed to write to client"),
                Command::Echo(mut s) => {
                    s.insert(0, '+');
                    s.push_str("\r\n");
                    stream
                        .write_all(s.as_bytes())
                        .expect("Failed to write teh echo response");
                }
                Command::Set(key, value) => {
                    let mut map = store.write().expect("RwLock poisoned");
                    // thread::sleep(Duration::from_millis(50));
                    map.insert(key, Mutex::new(value));
                    stream
                        .write_all(ok)
                        .expect("Failed to write teh echo response");
                }
                Command::Get(key) => {
                    let map = store.read().expect("RwLock poisoned");
                    // thread::sleep(Duration::from_millis(50));

                    let mut resp = "$-1\r\n".to_owned();

                    if let Some(value) = map.get(&key) {
                        resp = value.lock().unwrap().to_owned();
                        let len = resp.len() as u8;
                        resp.insert_str(0, "\r\n");
                        resp.push_str("\r\n");
                        resp.insert_str(0, &len.to_string());
                        resp.insert_str(0, "$");
                    }

                    stream
                        .write_all(resp.as_bytes())
                        .expect("Failed to write teh echo response");
                }
            }
        }
    }
}
