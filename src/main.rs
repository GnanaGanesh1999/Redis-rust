// Uncomment this block to pass the first stage
use std::{
    io::{Read, Write},
    net::TcpListener, 
    thread,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    eprintln!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for  stream in listener.incoming() {
        match stream {
            Ok( stream) => {
                eprintln!("Connection from: {}", stream.peer_addr().unwrap());

                thread::spawn(|| {
                    handle_connection( stream);
                });
     
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: std::net::TcpStream) {
    let mut buf = [0; 512];
    loop {
        let bytes_read = stream.read(&mut buf).expect("Failed to read from client");

        if bytes_read == 0 {
            return;
        }

        let pong = b"+PONG\r\n";

        stream.write_all(pong).expect("Failed to write to client");
    }
}
