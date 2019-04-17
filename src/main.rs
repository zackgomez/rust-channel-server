extern crate redis;
extern crate serde_json;
extern crate ws;

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn start_redis_server(
    channel: mpsc::Receiver<RedisCommand>,
    topic_to_senders: Arc<Mutex<HashMap<String, Vec<ws::Sender>>>>,
) -> redis::RedisResult<thread::JoinHandle<()>> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;

    println!("connected to redis");

    Ok(thread::spawn(move || {
        let mut pubsub = con.as_pubsub();
        pubsub
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        loop {
            let iter = channel.try_iter();
            iter.for_each(|command| {
                println!("got command {:?}", command);
                match command {
                    RedisCommand::SubscribeTopic(topic) => {
                        println!("Subscribing to topic in redis: {:?}", topic);
                        pubsub.subscribe(topic).unwrap();
                    }
                    RedisCommand::UnsubscribeTopic(topic) => {
                        println!("Unsubscribing to topic in redis: {:?}", topic);
                        pubsub.unsubscribe(topic).unwrap();
                    }
                }
            });

            if let Ok(msg) = pubsub.get_message() {
                let payload: String = match msg.get_payload() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                println!("channel '{}': {}", msg.get_channel_name(), payload);

                let map = topic_to_senders.lock().unwrap();
                if let Some(list) = map.get(msg.get_channel_name()) {
                    list.iter().for_each(|sender| {
                        if let Err(err) = sender.send(payload.clone()) {
                            println!("Error sending payload: {:?}", err);
                        }
                    })
                }
            }
        }
    }))
}

struct WsConnection {
    sender: ws::Sender,
    topic_to_senders: Arc<Mutex<HashMap<String, Vec<ws::Sender>>>>,
    subscribed_topics: HashSet<String>,
    redis_sender: mpsc::Sender<RedisCommand>,
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

            let mut map = self.topic_to_senders.lock().unwrap();
            let list = map.entry(String::from(topic)).or_insert(Vec::new());
            list.push(self.sender.clone());
            if list.len() == 1 {
                self.send_redis_command(RedisCommand::SubscribeTopic(String::from(topic)));
            }
        }
    }
    fn unsubscribe(&mut self, topic: &str) {
        if self.subscribed_topics.remove(topic) {
            println!("unsubscribed: {:?}", topic);

            let mut map = self.topic_to_senders.lock().unwrap();
            let list = map.get_mut(topic);
            if let Some(senders) = list {
                let pos = senders.iter().position(|x| x == &self.sender);
                if let Some(pos) = pos {
                    senders.remove(pos);
                }
                if senders.is_empty() {
                    self.send_redis_command(RedisCommand::UnsubscribeTopic(String::from(topic)));
                }
            }
        }
    }

    fn send_redis_command(&self, cmd: RedisCommand) {
        if let Err(err) = self.redis_sender.send(cmd) {
            println!("redis sender error {:?}", err);
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

fn start_web_socket_server(
    redis_sender: mpsc::Sender<RedisCommand>,
    topic_to_senders: Arc<Mutex<HashMap<String, Vec<ws::Sender>>>>,
) -> () {
    ws::listen("localhost:3002", |out| WsConnection {
        sender: out,
        topic_to_senders: topic_to_senders.clone(),
        subscribed_topics: HashSet::new(),
        redis_sender: redis_sender.clone(),
    })
    .unwrap();
}

#[derive(Debug)]
enum RedisCommand {
    SubscribeTopic(String),
    UnsubscribeTopic(String),
}

fn main() {
    println!("Starting");

    let topic_to_senders = Arc::new(Mutex::new(HashMap::new()));

    let (redis_sender, redis_receiver): (mpsc::Sender<RedisCommand>, mpsc::Receiver<RedisCommand>) =
        mpsc::channel();

    let redis_thread = start_redis_server(redis_receiver, topic_to_senders.clone()).unwrap();

    start_web_socket_server(redis_sender, topic_to_senders.clone());

    redis_thread.join().unwrap();
}
