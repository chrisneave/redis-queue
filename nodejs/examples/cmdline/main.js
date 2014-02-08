var util = require('util');
var redis = require('redis');
var client = redis.createClient('6379', '192.168.33.12');
var started;
var submit_queue = 'queue:submitted';
var receive_queue = 'queue:received';
var finished_ok_queue = 'queue:finished_ok';
var finished_with_error_queue = 'queue:finished_with_error';
var Queue = require(__dirname + '/../../lib/queue');
var program = require('commander');

var queue = new Queue(client);

program
  .usage('send|receive|finish [options]')
  .option('-m, --messages <m>', 'the number of messages to send', parseInt)
  .parse(process.argv);

var iterations = 0;

var handleError = function(err) {
  if (!err) { return; }
  console.error('An error occured: ' + err);
  throw err;
};

function dumpQueueStats(callback) {
  queue.getQueueLength([submit_queue, receive_queue, finished_ok_queue, finished_with_error_queue], function(err, result) {
    console.log('Submit queue length = %d', result[0]);
    console.log('Receive queue length = %d', result[1]);
    console.log('Finished Ok queue length = %d', result[2]);
    console.log('Finished With Error queue length = %d', result[3]);
    callback();
  });
}

function endSend() {
  var elapsed_ms = new Date() - started;
  console.log('Took %d ms to send %d messages - %d messages/second', elapsed_ms, iterations, Math.round(iterations / (elapsed_ms / 1000)));
  dumpQueueStats(function() {
    client.end();
    process.exit();
  });
}

function endAction(action, adjustment) {
  if (!adjustment) { adjustment = 0; }
  var elapsed_ms = new Date() - started + adjustment;
  if (iterations === 0) {
    console.log('No messages %s', action);
  } else {
    console.log('Took %d ms to %s %d messages - %d messages/second', elapsed_ms, action, iterations, Math.round(iterations / (elapsed_ms / 1000)));
  }
  dumpQueueStats(function() {
    client.end();
    return process.exit();
  });
}

function sendLoop(lua_hash, send_function) {
  for (var i = 0; i < iterations; i++) {
    send_function(lua_hash, {
      job_type: 'test',
      job_code: 'code' + i,
      body: JSON.stringify({field: i})
    });
  }
}

function sendMessageWithLua(lua_hash, message) {
  queue.submit(submit_queue, message.job_code + "." + message.job_type, message.body, function(err, result) {
    if (err) { handleError(err); }
    if (!result) { return process.exit(); }
    if (result === iterations) {
      return endSend();
    }
  });
}

var receiveMessageWithLua = function(lua_hash) {
  queue.receive(submit_queue, receive_queue, function(err, result) {
    if (err) { handleError(err); }
    if (!result) {
      return endAction('received', 0);
    }
    iterations++;
    receiveMessageWithLua(lua_hash);
  });
};

function processMessagesWithLua(lua_hash, message_id, finished_ok) {
  if (!message_id) { message_id = 1; }
  if (finished_ok === undefined) { finished_ok = true; }

  var finish_queue = (finished_ok) ? finished_ok_queue : finished_with_error_queue,
      status = (finished_ok) ? 'finished ok' : 'finished with error';

  queue.finish(receive_queue, finish_queue, message_id, status, function(err, result) {
    if (err) { handleError(err); }
    if (!result) {
      return endAction('finished', 0);
    }
    iterations++;
    processMessagesWithLua(lua_hash, ++message_id, !finished_ok);
  });
}

var run = function(message, exec_function) {
  console.log(message);
  started = new Date();
  exec_function();
};

client.script('flush');

switch(process.argv[2]) {
  case 'send':
    client.flushdb();
    iterations = program.messages || 1000;


    queue.init(function() {
      run(util.format('Sending %d messages using Lua script', iterations), function(result) {
        sendLoop(result, sendMessageWithLua);
      });
    });
    break;

  case 'receive':
    queue.init(function() {
      run('Receiving messages using Lua script', receiveMessageWithLua);
    });
    break;

  case 'finish':
    queue.init(function() {
      run('Finishing messages using Lua script', processMessagesWithLua);
    });
    break;

  default:
    console.error('Invalid arguments passed');
    return process.exit();
}


