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
    send: {
      hash: '',
      filename: __dirname + '/../lua/send_message.lua'
    },
    receive: {
      hash: '',
      filename: __dirname + '/../lua/receive_message.lua'
    },
    finish: {
      hash: '',
      filename: __dirname + '/../lua/process_message.lua'
    }
  };

  var _loadScript = function(message_name, callback) {
    var script,
        md5;

    if (self._scripts[message_name].hash) { return callback(); }

    script = fs.readFileSync(self._scripts[message_name].filename, 'utf8');
    md5 = crypto.createHash('md5');
    md5.update(script);
    return self._client.script('load', script, function(err, result) {
      self._scripts[message_name].hash = result;
      callback();
    });
  };

  var _getTime = function(callback) {
    self._client.time(function(err, result) {
      callback(err, utils.redisTimeToJSDate(result));
    });
  };

  this.init = function(next) {
    _loadScript('send', function() {
      _loadScript('receive', function() {
        _loadScript('finish', next);
      });
    });
  };

  this.submit = function(queue_name, message_key, message, callback) {
    var func = function() {
      _getTime(function(err, time) {
        self._client.evalsha(self._scripts.send.hash, 4, 'message:id', 'message:received', message_key, queue_name, JSON.stringify(message), time, callback);
      });
    };

    if (!self._scripts.send.hash) {
      _loadScript('send', func);
    } else {
      func();
    }
  };

  this.receive = function(submit_queue, receive_queue, callback) {
    var func = function() {
      _getTime(function(err, time) {
        self._client.evalsha(self._scripts.receive.hash, 2, submit_queue, receive_queue, time, callback);
      });
    };

    if (!self._scripts.receive.hash) {
      _loadScript('receive', func);
    } else {
      func();
    }
  };

  this.finish = function(receive_queue, finish_queue, message_id, status, callback) {
    var func = function() {
      _getTime(function(err, time) {
        self._client.evalsha(self._scripts.finish.hash, 4, receive_queue, finish_queue, message_id, 'message:received', status, time, callback);
      });
    };

    if (!self._scripts.finish.hash) {
      _loadScript('finish', func);
    } else {
      func();
    }
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
};

module.exports = Queue;
