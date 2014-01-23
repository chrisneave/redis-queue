var fs = require('fs');
var redis = require('redis');
var client = redis.createClient('6379', '192.168.33.12');
var iterations = 1000;
var started = new Date();
var messages_received = 0;
var submit_queue = 'queue:submitted';
var receive_queue = 'queue:received';
var finished_ok_queue = 'queue:finished_ok';
var finished_with_error_queue = 'queue:finished_with_error';
var Queue = require(__dirname + '/../../lib/queue');

var handleError = function(err) {
  if (!err) { return; }
  console.error('An error occured: ' + err);
  throw err;
};

function loadScript(filename, done) {
  var lua = fs.readFileSync(filename, 'utf8');
  client.script('load', lua, function(err, result) {
    done(err, result);
  });
}

function endSend() {
  var elapsed_ms = new Date() - started;
  console.log('Took %d ms to send %d messages - %d messages/second', elapsed_ms, iterations, Math.round(iterations / (elapsed_ms / 1000)));
  client.end();
  process.exit();
}

function endAction(action, adjustment) {
  if (!adjustment) { adjustment = 0; }
  var elapsed_ms = new Date() - started + adjustment;
  if (messages_received === 0) {
    console.log('No messages %s', action);
  } else {
    console.log('Took %d ms to %s %d messages - %d messages/second', elapsed_ms, action, messages_received, Math.round(messages_received / (elapsed_ms / 1000)));
  }
  client.end();
  return process.exit();
}

function sendLoop(send_function, lua_hash) {
  client.flushdb();

  for (var i = 0; i < iterations; i++) {
    send_function({
      job_type: 'test',
      job_code: 'code' + i,
      body: JSON.stringify({field: i})
    }, lua_hash);
  }
}

function sendMessageWithLua(message, lua_hash) {
  var queue = new Queue(client, {send_script_hash: lua_hash});
  queue.submit(submit_queue, message.job_code + "." + message.job_type, message.body, function(err, result) {
    if (err) { handleError(err); }
    if (!result) { return process.exit(); }
    if (result === iterations) {
      return endSend();
    }
  });
}

var receiveMessageWithLua = function(lua_hash) {
  var queue = new Queue(client, {receive_script_hash: lua_hash});
  queue.receive(submit_queue, receive_queue, function(err, result) {
    if (err) { handleError(err); }
    if (!result) {
      return endAction('received', 0);
    }
    messages_received++;
    receiveMessageWithLua(lua_hash);
  });
};

function processMessagesWithLua(lua_hash, message_id, finished_ok) {
  var finish_queue = (finished_ok) ? finished_ok_queue : finished_with_error_queue,
      status = (finished_ok) ? 'finished ok' : 'finished with error',
      queue = new Queue(client, {finish_script_hash: lua_hash});

  queue.finish(receive_queue, finish_queue, message_id, status, function(err, result) {
    if (err) { handleError(err); }
    if (!result) {
      return endAction('finished', 0);
    }
    messages_received++;
    processMessagesWithLua(lua_hash, ++message_id, !finished_ok);
  });
}

switch (process.argv[2]) {
  case '-send':
    console.log('Sending %d messages using Lua script', iterations);
    client.script('flush', function() {
      loadScript(__dirname + '/../../lua/send_message.lua', function(err, result) {
        if (err) { return console.error(err); }
        sendLoop(sendMessageWithLua, result);
      });
    });
    break;

  case '-receive':
    console.log('Receiving messages using Lua script');
    client.script('flush', function() {
      loadScript(__dirname + '/../../lua/receive_message.lua', function(err, result) {
        if (err) { return console.error(err); }
        receiveMessageWithLua(result);
      });
    });
    break;

  case '-finish':
    console.log('Finishing %d messages using Lua script', iterations);
    client.script('flush', function() {
      loadScript(__dirname + '/../../lua/process_message.lua', function(err, result) {
        if (err) { return console.error(err); }
        processMessagesWithLua(result, 1, true);
      });
    });
    break;

  default: {
    console.error('Invalid arguments passed');
    return process.exit();
  }
}
