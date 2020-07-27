#[macro_use]
extern crate serde;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{channel};
use tokio::{task, time};

// use std::sync::mpsc::channel;
// use std::thread;

use std::env;
use rumq_client::{eventloop, MqttOptions, Publish, QoS, Request};
use std::time::Duration;
use std::fs::File;
use std::io::{BufReader};
use std::io::prelude::*;

extern crate env_logger;
use log::{info};

use ctrlc::set_handler;

use rand::{distributions::Alphanumeric, Rng, thread_rng};

#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

fn handle_close(published: Arc<Mutex<HashMap<String, i64>>>) {
  info!("Closing client application.");
  
  let mut total_messages = 0;
  let mut total_topics = 0;

  let p = published.lock().unwrap();
  for (key, value) in p.clone() {
    total_topics += 1;
    total_messages += value;
    info!("Topic {}: {} messages", key, value);
  }
  drop(p);

  info!("Published {} messages in {} topics.", total_messages, total_topics);
}

#[tokio::main]
async fn main() {
  env_logger::init();

  let published: Arc<Mutex<HashMap<String, i64>>> = Arc::new(Mutex::new(HashMap::new()));
  let p = Arc::clone(&published);
  set_handler(move || handle_close(p.clone())).expect("Error setting Ctrl-C handler");

  let address = env::var("MQTT_BROKER_ADDRESS").unwrap();
  let port = env::var("MQTT_BROKER_PORT").unwrap().parse::<u16>().unwrap();

  let buffer_size = env::var("CLIENT_BUFFER_BYTE_SIZE").unwrap().parse::<usize>().unwrap_or(8);
  info!("Starting client. Host at {}:{}", address.clone(), port.clone());

  let count = get_lines();
  
  let (requests_tx, requests_rx) = channel(50);
  let mut eloop;
  let mut mqttoptions = MqttOptions::new("client", address.clone(), port);
  mqttoptions.set_keep_alive(5).set_throttle(Duration::from_secs(1));
  eloop = eventloop(mqttoptions, requests_rx);

  let tx_c = requests_tx.clone();
  for i in 0..count {
    let topic = get_topic(i);
    
    let tx = tx_c.clone();
    let published_cl = Arc::clone(&published);
    
    task::spawn(async move {
      info!("Thread {} - {}", i, topic.clone());
      let p = Arc::clone(&published_cl);
      
      let mut index: u64 = 0;
      loop {
        let payload: String = std::iter::repeat(())
          .map(|()| thread_rng().sample(Alphanumeric))
          .take(buffer_size).collect();
        let t = topic.clone();

        tx.clone().send(publish_request(&(payload.as_str()), &t.clone())).await.unwrap();
        
        {
          // Access global mutexed variable
          let mut guard = p.lock().unwrap();
          guard.entry(t.clone()).or_insert(0);
          guard.insert(t.clone(), 1);
          drop(guard);
        }

        info!("{}::{}::{}", index, t, payload);

        index += 1;
        time::delay_for(Duration::from_millis(50)).await;
      }
    });
  }

  let mut stream = eloop.connect().await.unwrap();
  while let Some(_item) = stream.next().await {}
}

fn get_qos(variable: &str) -> QoS {
  let qos_value = env::var(variable).unwrap().parse::<u8>().unwrap();

  match qos_value {
    0 => QoS::AtMostOnce,
    1 => QoS::AtLeastOnce,
    2 => QoS::ExactlyOnce,
    _ => QoS::AtMostOnce
  }
}

fn publish_request(payload: &str, topic: &str) -> Request {
  let topic = topic.to_owned();
  let message = String::from(payload);

  let qos = get_qos("MQTT_CLIENT_QOS");

  let publish = Publish::new(&topic, qos, message);
  Request::Publish(publish)
}

fn get_topic(line_num: usize) -> String {
  let f = File::open("topic_names.txt").expect("Open topic file");
  let buffer = BufReader::new(f);
  let mut topic = String::new();

  let mut counter = 0;

  for line in buffer.lines() {
    if counter == line_num {
      topic = line.unwrap();
    }
    counter += 1;
  }

  topic
}

fn get_lines() -> usize {
  let f = File::open("topic_names.txt").expect("Open topic file");
  let buffer = BufReader::new(f);

  buffer.lines().count()
}