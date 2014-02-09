/* Copyright 2014 Chris Neave (chrispneave@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var fs = require('fs');
var crypto = require('crypto');
var utils = require(__dirname + '/utils');
var exceptions = require(__dirname + '/exceptions');

var Queue = function(client) {
  var self = this;

  // TODO: Need a better method of determining whether client is a RedisClient.
  if (!client.server_info) {
    throw new exceptions.ArgumentException();
  }

  this._client = client;
  this._scripts = {
    send: { hash: '', filename: __dirname + '/../lua/send_message.lua' },
    receive: { hash: '', filename: __dirname + '/../lua/receive_message.lua' },
    finish: { hash: '', filename: __dirname + '/../lua/process_message.lua' }
  };
  this._listeners = [];

  var _loadScript = function(message_name, callback) {
    var script,
        md5;

    if (self._scripts[message_name].hash) { return callback(); }

    script = fs.readFileSync(self._scripts[message_name].filename, 'utf8');
    md5 = crypto.createHash('md5');
    md5.update(script);
    self._client.script('load', script, function(err, result) {
      self._scripts[message_name].hash = result;
      callback();
    });
  };

  var _getTime = function(callback) {
    self._client.time(function(err, result) {
      callback(err, utils.redisTimeToJSDate(result));
    });
  };

  var _loadScriptAndGetTime = function(script_action, next) {
    _loadScript(script_action, function() {
      _getTime(next);
    });
  };

  var _internalReceive = function(submit_queue, receive_queue, message_id, callback) {
    _loadScriptAndGetTime('receive', function(err, time) {
      self._client.evalsha(self._scripts.receive.hash, 3, submit_queue, receive_queue, message_id, time, function(err, result) {
        callback(err, _deserializeMessage(result));
      });
    });
  };

  var _deserializeMessage = function(message_array) {
    var json;

    if (message_array) {
      json = {};
      json[message_array[0]] = message_array[1];
      json[message_array[2]] = message_array[3];
      json[message_array[4]] = message_array[5];
      json[message_array[6]] = message_array[7];
      json[message_array[8]] = message_array[9];
      json[message_array[10]] = message_array[11];
    }

    return json;
  };

  this.init = function(next) {
    _loadScript('send', function() {
      _loadScript('receive', function() {
        _loadScript('finish', next);
      });
    });
  };

  this.submit = function(queue_name, message_key, message, callback) {
    _loadScriptAndGetTime('send', function(err, time) {
      self._client.evalsha(self._scripts.send.hash, 4, 'message:id', 'message:received', message_key, queue_name, JSON.stringify(message), time, callback);
    });
  };

  this.receive = function(submit_queue, receive_queue, callback) {
    _internalReceive(submit_queue, receive_queue, undefined, callback);
  };

  this.finish = function(receive_queue, finish_queue, message_id, status, callback) {
    _loadScriptAndGetTime('finish', function(err, time) {
      self._client.evalsha(self._scripts.finish.hash, 4, receive_queue, finish_queue, message_id, 'message:received', status, time, callback);
    });
  };

  this.getQueueLength = function(queue_names, callback, results) {
    var err,
        item;

    results = results || [];

    // Coerce non-array arguments into a single element Array.
    if (Object.prototype.toString.call(queue_names) !== '[object Array]') {
      queue_names = [queue_names];
    }

    item = queue_names.shift();

    // If there are no queues left to query then invoke the callback and return.
    if (!item) {
      // Coerce a single result from an array to a single value.
      if (results.length === 1) {
        results = results[0];
      }

      callback(err, results);
      return;
    }

    self._client.llen(item, function(err, result) {
      if (err) {
        callback(err, results);
        return;
      }

      results.push(result);
      self.getQueueLength(queue_names, callback, results);
    });
  };

  var _notifyListeners = function(err, result) {
    for (var i = 0; i < self._listeners.length; i++) {
      self._listeners[i](err, result);
    }
  };

  var _doListen = function(submit_queue, receive_queue) {
    var queue_length = 0;

    self.getQueueLength(submit_queue, function(err, result) {
      queue_length = result;

      while (queue_length > 0 && self._listeners.length > 0) {
        self.receive(submit_queue, receive_queue, _notifyListeners);
        queue_length--;
      }

      self._client.brpoplpush(submit_queue, receive_queue, 1, function(err, result) {
        _internalReceive(submit_queue, receive_queue, result, _notifyListeners);

        // Invoke the method again while there are still listeners to notify.
        if (self._listeners.length > 0) {
          process.nextTick(function() {
            _doListen(submit_queue, receive_queue);
          });
        }
      });
    });
  };

  this.listen = function(submit_queue, receive_queue, callback) {
    self._listeners.push(callback);

    if (self._listeners.length === 1) {
      process.nextTick(function() {
        _doListen(submit_queue, receive_queue);
      });
    }
  };
};

module.exports = Queue;
