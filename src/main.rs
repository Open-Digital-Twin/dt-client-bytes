#[macro_use]
extern crate serde;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{channel};
use tokio::{task, time};

// use std::sync::mpsc::channel;
// use std::thread;

use std::env;
use rumqttc::{MqttOptions, QoS, EventLoop, Request, Publish, Incoming, Outgoing};
use std::time::Duration;
use std::fs::File;
use std::io::{BufReader};
use std::io::prelude::*;

extern crate env_logger;
use log::{info, error};

use ctrlc::set_handler;

use rand::{distributions::Alphanumeric, Rng, thread_rng};

#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct NotificationCounter {
  Pub: u64,
  PubAck: u64,
  PubRec: u64,
  PubRel: u64,
  PubComp: u64
}

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

  let buffer_size = env::var("MQTT_CLIENT_BUFFER_BYTE_SIZE").unwrap_or("8".to_string()).parse::<usize>().unwrap();
  let message_limit = env::var("MQTT_CLIENT_MESSAGES_TO_SEND").unwrap_or("100".to_string()).parse::<u64>().unwrap();
  let message_delay_ms = env::var("MQTT_CLIENT_MESSAGE_DELAY_MS").unwrap_or("0".to_string()).parse::<u64>().unwrap();
  let topic = env::var("MQTT_CLIENT_TOPIC").unwrap();

  info!("Starting client. Host at {}:{}", address.clone(), port.clone());
  
  let id: String = std::iter::repeat(())
    .map(|()| thread_rng().sample(Alphanumeric))
    .take(10).collect();

  let mut mqttoptions = MqttOptions::new(id, address.clone(), port);
  mqttoptions.set_keep_alive(100);
  mqttoptions.clean_session(false);

  let mut eventloop = EventLoop::new(mqttoptions, 20).await;
  let requests_tx = eventloop.handle();
  
  let tx = requests_tx.clone();
  let published_cl = Arc::clone(&published);
    
  task::spawn(async move {
    info!("Thread of topic {}", topic.clone());
    let p = Arc::clone(&published_cl);
    
    let mut index: u64 = 0;
    while index < message_limit {
      let index_str = index.to_string();
      let mut payload: String = std::iter::repeat(())
        .map(|()| thread_rng().sample(Alphanumeric))
        .take(buffer_size - index_str.len() - 1).collect();

      payload.insert_str(0, &" ");
      payload.insert_str(0, &index_str);

      let t = topic.clone();

      tx.send(publish_request(&(payload.as_str()), &t.clone())).await.unwrap();

      {
        // Access global mutexed variable
        let mut guard = p.lock().unwrap();
        guard.entry(t.clone()).or_insert(0);
        guard.insert(t.clone(), 1);
        drop(guard);
      }

      info!("{}::{} \"{}\"", index, t, payload);

      index += 1;
      time::delay_for(Duration::from_millis(message_delay_ms)).await;
    }
    info!("Thread {} done with {} messages.", topic, index);
  });

  let mut notification_counter = NotificationCounter {
    Pub: 0,
    PubAck: 0,
    PubRec: 0,
    PubRel: 0,
    PubComp: 0
  };

  loop {
    match eventloop.poll().await {
      Ok(mut _notification) => {
        let (incoming, outgoing) = _notification;

        if incoming.is_some() {
          match incoming.unwrap() {
            Incoming::PubAck(ack) => {
              info!("{:?}", ack);
              notification_counter.PubAck += 1;
            },
            Incoming::PubRec(ack) => {
              info!("{:?}", ack);
              notification_counter.PubRec += 1;
            },
            Incoming::PubRel(ack) => {
              info!("{:?}", ack);
              notification_counter.PubRel += 1;
            },
            Incoming::PubComp(ack) => {
              info!("{:?}", ack);
              notification_counter.PubComp += 1;
            },
            _ => {}
          }
        }

        if outgoing.is_some() {
          match outgoing.unwrap() {
            Outgoing::Publish(publish) => {
              info!("Publish {:?}", publish);
              notification_counter.Pub += 1;
            },
            _ => {}
          }

        }
      },
      Err(e) => { error!("{:?}", e); }
    }

    if notification_counter.Pub >= message_limit {
      info!("{:#?}", notification_counter);
    }

    time::delay_for(Duration::from_millis(0)).await;
  }
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
