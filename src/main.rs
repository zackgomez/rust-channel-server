extern crate redis;
extern crate serde_json;
extern crate ws;

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

fn start_redis_server() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;

    println!("connected? {:?}", con.is_open());

    redis::cmd("SET").arg("my_key").arg(42).query(&con)?;
    let val: String = redis::cmd("GET").arg("my_key").query(&con)?;
    println!("result: {:?}", val);

    let mut pubsub = con.as_pubsub();
    pubsub.subscribe("channel_1")?;
    loop {
        let msg = pubsub.get_message()?;
        let payload: String = msg.get_payload()?;
        println!("channel '{}': {}", msg.get_channel_name(), payload);
    }
}

struct WsConnection {
    sender: ws::Sender,
    topic_to_senders: Rc<RefCell<HashMap<String, Vec<ws::Sender>>>>,
    subscribed_topics: HashSet<String>,
}

impl WsConnection {
    fn handle_message(&mut self, msg: &str) -> serde_json::Result<()> {
        let v: serde_json::Value = serde_json::from_str(msg)?;

        println!("parsed: {:?}", v);
        let msg_type = v["type"].as_str().unwrap_or("");
        match msg_type {
            "subscribe" => {
                let topic = v["topic"].as_str().unwrap_or("");
                self.subscribe(topic);
            }
            "unsubscribe" => {
                let topic = v["topic"].as_str().unwrap_or("");
                self.unsubscribe(topic);
            }
            _ => println!("unknown message type: {:?}", msg_type),
        };

        Ok(())
    }

    fn subscribe(&mut self, topic: &str) {
        if self.subscribed_topics.insert(String::from(topic)) {
            println!("subscribe: {:?}", topic);

            let mut map = self.topic_to_senders.borrow_mut();
            let list = map.entry(String::from(topic)).or_insert(Vec::new());
            list.push(self.sender.clone());
            if list.len() == 1 {
                println!("subscribe to ws topic {:?}", topic);
                // TODO subscribe to ws
            }
        }
    }
    fn unsubscribe(&mut self, topic: &str) {
        if self.subscribed_topics.remove(topic) {
            println!("unsubscribed: {:?}", topic);

            let mut map = self.topic_to_senders.borrow_mut();
            let list = map.get_mut(topic);
            if let Some(senders) = list {
                let pos = senders.iter().position(|x| x == &self.sender);
                if let Some(pos) = pos {
                    senders.remove(pos);
                }
                if senders.is_empty() {
                    println!("unsubscribe to ws topic {:?}", topic);
                }
            }
        }
    }
}

impl ws::Handler for WsConnection {
    fn on_open(&mut self, shake: ws::Handshake) -> ws::Result<()> {
        println!("new connection");
        //println!("got connection with request {:?}", shake.request);
        // TODO parse jwt in shake.request.path and authenticate user
        Ok(())
    }

    fn on_message(&mut self, msg: ws::Message) -> ws::Result<()> {
        //println!("got message from socket: {:?}", msg);

        match self.handle_message(msg.as_text()?) {
            Ok(v) => Ok(v),
            Err(error) => Err(ws::Error {
                kind: ws::ErrorKind::Custom(Box::new(error)),
                details: std::borrow::Cow::Borrowed("derp"),
            }),
        }
    }

    fn on_close(&mut self, _code: ws::CloseCode, reason: &str) {
        println!("closing socket {:?}", reason);
        let topics = self.subscribed_topics.clone();
        topics.iter().for_each(|topic| self.unsubscribe(topic));
    }
}

fn start_web_socket_server() -> () {
    let topic_to_senders = Rc::new(RefCell::new(HashMap::new()));
    ws::listen("localhost:3002", |out| WsConnection {
        sender: out,
        topic_to_senders: topic_to_senders.clone(),
        subscribed_topics: HashSet::new(),
    })
    .unwrap();
}

fn main() {
    println!("Starting");

    start_web_socket_server();

    /*
    match start_redis_server(){
        Ok(_) => (),
        Err(error) => {
            panic!("Error: {:?}", error);
        }
    }
    */
}
