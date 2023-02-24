#[macro_use]
#[allow(non_snake_case)]
extern crate serde;
//use tokio::sync::mpsc::{channel};
use tokio::{task, time, signal};
use chrono::Utc;

// use std::sync::mpsc::channel;
// use std::thread;

use std::{env, vec};
use rumqttc::{MqttOptions, QoS, AsyncClient, Request, Publish, Incoming, Outgoing, Event};
use std::time::Duration;
//use std::fs::File;
//use std::io::{BufReader};
//use std::io::prelude::*;

extern crate env_logger;
use log::{info, error};

//use ctrlc::set_handler;

use rand::{distributions::Alphanumeric, Rng, thread_rng};

//#[macro_use]
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

async fn client_thread(client: usize, address: String, port: u16, topic: String, buffer_size: usize, message_limit: u64, message_delay_ms: u64) {
  info!("Starting client {}. Host at {}:{}", client, address, port);
  
  let id: String = std::iter::repeat(())
    .map(|()| thread_rng().sample(Alphanumeric))
    .take(10).collect();

  let mut mqttoptions = MqttOptions::new(id, address.clone(), port);
  mqttoptions.set_keep_alive(30);
  mqttoptions.set_clean_session(false);

  let (mut _client, mut eventloop) = AsyncClient::new(mqttoptions, 20);
  let requests_tx = eventloop.handle();
  
  let tx = requests_tx.clone();

  task::spawn(async move {
    info!("Thread of topic {}", topic.clone());
    
    let mut index: u64 = 0;
    while index < message_limit {
      let index_str = index.to_string();
      let mut payload: String = std::iter::repeat(())
        .map(|()| thread_rng().sample(Alphanumeric))
        .take(buffer_size - index_str.len() - 35).collect();

      payload.insert_str(0, &" ");
      payload.insert_str(0, &(Utc::now()).to_string());
      payload.insert_str(0, &" ");
      payload.insert_str(0, &index_str);

      let t = topic.clone();

      tx.send(publish_request(&(payload.as_str()), &t.clone())).await.unwrap();

      // {
      //   // Access global mutexed variable
      //   let mut guard = p.lock().unwrap();
      //   guard.entry(t.clone()).or_insert(0);
      //   guard.insert(t.clone(), 1);
      //   drop(guard);
      // }

      info!("{}::{} \"{}\"", index, t, index);

      index += 1;
      time::sleep(Duration::from_millis(message_delay_ms)).await;
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
      Ok(event) => {
        match event {
          Event::Incoming(packet) => {
            match packet {
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
          Event::Outgoing(packet) => {
            match packet {
              Outgoing::Publish(publish) => {
                info!("Publish {:?}", publish);
                notification_counter.Pub += 1;
              },
              _ => {}
            }
          }
        }
      },
      Err(e) => { error!("{:?}", e); }
    }

    if notification_counter.Pub >= message_limit {
      info!("{:#?}", notification_counter);
    }

    time::sleep(Duration::from_millis(0)).await;
  }
}

#[tokio::main]
async fn main() {
  env_logger::init();

  let published: Arc<Mutex<HashMap<String, i64>>> = Arc::new(Mutex::new(HashMap::new()));
  let p = Arc::clone(&published);
  
  let container_delay = env::var("CONTAINER_DELAY_S").unwrap_or("0".to_string()).parse::<u64>().unwrap();
  let pod_name = env::var("POD_NAME").unwrap_or("client-default-name-0".to_string());
  info!("Replica Name {}" , pod_name);
  let split = pod_name.split("-");
  let split_name = split.collect::<Vec<&str>>();
  let wait_mult = split_name[3].parse::<u64>().unwrap();
  info!("Sleeping for {}" , (wait_mult * container_delay));
  time::sleep(Duration::from_secs(wait_mult * container_delay)).await;
  
  let address = env::var("MQTT_BROKER_ADDRESS").unwrap(); 
  let port = env::var("MQTT_BROKER_PORT").unwrap().parse::<u16>().unwrap();
  
  let buffer_size = env::var("MQTT_CLIENT_BUFFER_BYTE_SIZE").unwrap_or("8".to_string()).parse::<usize>().unwrap();
  let message_limit = env::var("MQTT_CLIENT_MESSAGES_TO_SEND").unwrap_or("100".to_string()).parse::<u64>().unwrap();
  let message_delay_ms = env::var("MQTT_CLIENT_MESSAGE_DELAY_MS").unwrap_or("0".to_string()).parse::<u64>().unwrap();
  let topic = env::var("MQTT_CLIENT_TOPIC").unwrap();
  let thread_delay = env::var("MQTT_THREAD_DELAY").unwrap().parse::<u64>().unwrap();
  
  let clients = env::var("MQTT_AMOUNT_OF_CLIENTS").unwrap_or("1".to_string()).parse::<usize>().unwrap();
  let mut client_vec: Vec<usize> = [].to_vec();
  let mut tn = 0;

  for n in 1..clients+1 {
    client_vec.push(n);
  }
  
  let tasks: Vec<_> = client_vec
  .into_iter()
  .map(|client| {
    let a = address.clone();
    let mut t = topic.clone();
    let tns = tn.to_string();
    t.push_str(&tns);
    tn += 1;
    return task::spawn(async move {
      time::sleep(Duration::from_secs(thread_delay * tn)).await;
      return client_thread(client, a, port, t, buffer_size, message_limit, message_delay_ms).await;
    });
  }).collect();

  match signal::ctrl_c().await {
    Ok(()) => {},
    Err(err) => {
        eprintln!("Unable to listen for shutdown signal: {}", err);
    },
  }

  handle_close(p.clone());
  for task in tasks {
    task.abort();
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
