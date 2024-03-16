// Uncomment this block to pass the first stage
use std::{
    io::{Read, Write},
    net::TcpListener,
    string::FromUtf8Error,
    thread,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    eprintln!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                eprintln!("Connection from: {}", stream.peer_addr().unwrap());

                thread::spawn(|| {
                    handle_connection(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
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
        size_num = (size_num * 10) + (ch  as u64 - 48);
    }
    dbg!(&size_num);
    bytes_processed += 2;
    dbg!(&bytes_processed);
    (
        String::from_utf8(buf[bytes_processed..(bytes_processed + size_num as usize)].into()),
        bytes_processed,
    )
}

fn handle_connection(mut stream: std::net::TcpStream) {
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
        let mut current_byte = 0;

        loop {
            if bytes_processed >= bytes_read {
                break;
            }
            
            current_byte = bytes_processed;
            match current_resp {
                None => {
                    current_resp = Resp::from_byte(buf[bytes_processed]);
                    bytes_processed += 1;
                }

                Some(resp) => match resp {
                    Resp::Array => {
                        let size_char = buf[bytes_processed] as char;
                        bytes_processed += 3; // Ignoring the terminator
                        let size: u32 = size_char.to_digit(10).expect("Not a digit in array size");
                        dbg!(&size);
                        current_resp = None;
                    }
                    Resp::BulkString => {
                        let result = get_bulkstring(&mut buf, bytes_processed);

                        bytes_processed = result.1; // processed the bulstring size
                        dbg!(&bytes_processed);
                        current_resp = None;

                        if let Ok(command) = result.0 {
                            bytes_processed += 6;
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
                        }
                    }
                },
            }
            if current_byte == bytes_processed {
                // bytes_processed += 1;
            }
            println!("######## {}", bytes_processed);
        }

        println!("Processed the request");
        let pong = b"+PONG\r\n";

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
            }
        }
    }
}
