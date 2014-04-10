"use strict";

// Dependencies
var TransformStream = require("stream").Transform;
var express = require("express");
var when = require("when");
var redis = require("then-redis");
var mongojs = require("mongojs");

// Configuration
var config = {
  port: 3000,
  database: "eventsource_test",
  collection: "events",
  sequenceKey: "events.sequence",
  channelName: "events.messages"
};

// set up mongo
var database = mongojs(config.database);
var collection = database.collection(config.collection);

// Redis requires a separate client for subscribing
var client = redis.createClient();
var subscriber = redis.createClient();

when(subscriber.subscribe(config.channelName)).done();

process.on("SIGINT", function() {
  console.log("Shutting down server...");
  subscriber.removeAllListeners();

  when().
    then(unsubscribe).
    then(logUnsubscribed).
    yield(0).
    done(process.exit);

  function unsubscribe() {
    return subscriber.unsubscribe(config.channelName);
  }

  function logUnsubscribed() {
    console.log("unsubscribed redis client");
  }

});

var server = express();

server.get("/sse/historic", function(request, response) {
  var lastEventId = request.headers["last-event-id"];
  console.log("Requested historic stream with last-event-id:", lastEventId);

  var sort = { _id: 1 };
  var query = lastEventId == null ? {} : { _id: { $gt: Number(lastEventId) } };
  var cursor = collection.find(query).sort(sort);

  var sseTransformStream = new TransformStream();

  sseTransformStream._writableState.objectMode = true;
  sseTransformStream._readableState.objectMode = false;
  sseTransformStream._transform = function(event, encoding, done) {
    var sse =
      "id: " + event._id + "\n" +
      "event: " + event.type + "\n" +
      "data: " + JSON.stringify(event) + "\n\n";
    this.push(sse);
    done();
  };

  response.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive"
  });

  cursor.pipe(sseTransformStream).pipe(response);
});

server.get("/sse/live", function(request, response) {
  var lastEventId = request.headers["last-event-id"];
  console.log("Requested live stream with last-event-id:", lastEventId);

  response.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive"
  });

  subscriber.on("message", onMessage);

  function onMessage(channel, message) {
    var event = JSON.parse(message);
    response.write("id: " + event._id + "\n");
    response.write("event: " + event.type + "\n");
    response.write("data: " + JSON.stringify(event) + "\n\n");
  }

  function unsubscribe() {
    subscriber.removeListener("message", onMessage);
  }

  // Cleanup listener on close/error
  response.on("close", function() { unsubscribe(); });
  response.on("error", function(error) { unsubscribe(); throw error; });
});

server.get("/triggerEvent", function(request, response) {
  var event = {
    type: "Ping",
    timestamp: new Date()
  };

  when().
    then(incrementSequence).
    then(assignId).
    then(insertEvent).
    then(publishMessage).
    then(respond).
    done();

  function incrementSequence() {
    return client.incr(config.sequenceKey);
  }

  function assignId(n) {
    event._id = n;
  }

  function insertEvent() {
    var $deferred = when.defer();
    collection.insert(event, onInsert);
    function onInsert(error, results) {
      if(error) {
        $deferred.reject(error);
      } else {
        $deferred.resolve(results);
      }
    }
  }

  function publishMessage() {
    var message = JSON.stringify(event);
    return client.publish(config.channelName, message);
  }

  function respond() {
    response.send(200, event);
  }

});

server.listen(config.port);
