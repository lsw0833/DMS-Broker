var mosca = require('mosca');
var mqtt = require('mqtt');
var request = require('request');
var dateTime = require('node-datetime');
var bridge = "";
var clusterServer = process.env.cluster;
var dbServer = process.env.dbhost;
var connectManager;
var brokerId = process.env.brokerid;
var topiclist = [];
var settings = {
  port: 1883,
  http:{
    port:3000,
    bundle: true,
    static: './'
  }
};
var server = new mosca.Server(settings);
server.on('ready', setup); //on init it fires up setup()

// fired when the mqtt server is ready
function setup() {
  console.log('Borker is up and running');
  connectManager = mqtt.connect(clusterServer);
  bridge = connectManager.options.clientId;
  connectManager.on('connect', () => {
    connectManager.subscribe('broadcast');
  });
  connectManager.on('message', (topic, message) => {
    var slashindex = topic.indexOf("/") + 1;
    if (brokerId != topic.slice(0, slashindex - 1)) {
      var realmsg = {
        topic: topic.slice(slashindex, topic.length),
        payload: message, // or a Buffer
        qos: 1, // 0, 1, or 2
        retain: false, // or true
      };
      server.publish(realmsg, "ComeToBrokerManager", function() {
      });
    }
  });
}
server.on('published', function(packet, client) {
  if (packet.topic.indexOf("$SYS") != 0) {
    connectManager.publish(brokerId + "/" + packet.topic, packet.payload);
  }
});
server.on('subscribed',function(topic,client){
  let temp = -1;
  for(var i =0; i<topiclist.length; i++){
    if(topiclist[i].Topic == topic){
      temp = i;
      break;
    }
  }
  if(temp == -1){
      connectManager.subscribe("+/" + topic);
  }
  topiclist.push({Id : client.id, Topic : topic});
});
server.on('unsubscribed',function(topic,client){
  let index=0;
  for(var i = 0; i < topiclist.length; i++){
    if(topiclist[i].Id == client.id && topiclist[i].Topic == topic){
      index = i;
      break;
    }
  }
  topiclist.splice(index,1);
  index = -1;
  for(var i =0; i<topiclist.length; i++){
    if(topiclist[i].Topic == topic){
      index = i;
      break;
    }
  }
  if(index == -1){
    connectManager.unsubscribe("+/" + topic);
  }
});
// fired when a client connects
server.on('clientConnected', function(client) {
  if (bridge != client.id) {
    var dt = dateTime.create();
    var formatted = dt.format('Y-m-d H:M:S');
    var options = {
      uri: 'http://'+dbServer+':8080/client',
      method: 'POST',
      json: {
        "client_mqtt_id": client.id,
        "last_connected": formatted,
        "broker_id": brokerId
      }
    };

    request(options, function(error, response, body) {
      if (!error && response.statusCode == 200) {
      }
    });
  }
});
server.on('clientDisconnecting',function(client) {
  if (bridge != client.id) {
    var options = {
      uri: 'http://'+dbServer+':8080/client',
      method: 'DELETE',
      json: {
        "client_mqtt_id": client.id
      }
    };
    let list = [];
    for(var i =0; i<topiclist.length; i++){
      if(topiclist[i].Id == client.id){
        list.push(topiclist[i].Topic);
        topiclist.splice(i,1);
        i--;
      }
    }
    let temp = -1;
    for(var i=0; i<list.length; i++){
      for(var j=0; j<topiclist.length;j++){
          if(topiclist[j].Topic == list[i]){
            temp = j;
            break;
          }
      }
      if(temp == -1){
        connectManager.unsubscribe("+/" + list[i]);
      }else{
        temp = -1;
      }
    }
    request(options, function(error, response, body) {
      if (!error && response.statusCode == 200) {
      }
    });
  }
});
// fired when a client disconnects
server.on('clientDisconnected', function(client) {
  if (bridge != client.id) {
    var options = {
      uri: 'http://'+dbServer+':8080/client',
      method: 'DELETE',
      json: {
        "client_mqtt_id": client.id
      }
    };
    let list = [];
    for(var i =0; i<topiclist.length; i++){
      if(topiclist[i].Id == client.id){
        list.push(topiclist[i].Topic);
        topiclist.splice(i,1);
        i--;
      }
    }
    let temp = -1;
    for(var i=0; i<list.length; i++){
      for(var j=0; j<topiclist.length;j++){
          if(topiclist[j].Topic == list[i]){
            temp = j;
            break;
          }
      }
      if(temp == -1){
        connectManager.unsubscribe("+/" + list[i]);
      }else{
        temp = -1;
      }
    }
    request(options, function(error, response, body) {
      if (!error && response.statusCode == 200) {
      }
    });
  }
});
