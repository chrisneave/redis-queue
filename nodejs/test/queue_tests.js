'use strict';
// Disable the 'is defined but never used' rule which will show up on stubs.
/* jshint -W098 */

var expect = require('expect.js');
var sinon = require('sinon');
var rq = require(__dirname + '/../index');
var Queue = rq.Queue;
var exceptions = rq.Exceptions;
var util = require('util');
var fs = require('fs');
var crypto = require('crypto');

describe('Queue', function() {
  var client;
  var redis_time = [1389535019, 616092];
  var js_time = new Date(1970, 0, 1, 0, 0, 0, 1389535019616);
  var submit_queue = 'my_submit_queue';
  var receive_queue = 'my_receive_queue';
  var message = { foo: 'bar' };
  var message_key = '123:abc';
  var spy;
  var time_stub;
  var queue;
  var fs_stub;
  var script_hash = {};
  var script_content = {
    send: 'this is the send.lua',
    receive: 'this is the receive.lua',
    finish: 'this is the finish.lua'
  };
  var md5;
  var script_spy;
  var wrapTest = function(lua_file, lua_hash, function_under_test) {
    // Arrange
    spy.yields();
    script_spy.withArgs('load', lua_hash).yields(undefined, lua_hash);
    fs_stub.withArgs('../lua/receive_message.lua', 'utf8').returns(script_content.receive);

    // Act
    function_under_test();
  };

  beforeEach(function() {
    sinon.log = function(message) {
      console.log(message);
    };

    client = {};
    util.inherits(client, require('redis').RedisClient);
    client.time = function() {};
    client.evalsha = function() {};
    client.script = function() {};
    client.llen = function() {};
    client.server_info = {redis_version: '2.3.0'};

    spy = sinon.stub(client, 'evalsha');
    script_spy = sinon.stub(client, 'script');

    time_stub = sinon.stub(client, 'time', function(callback) {
      callback(undefined, redis_time);
    });

    fs_stub = sinon.stub(fs, 'readFileSync');
    fs_stub.withArgs('../lua/send_message.lua').returns(script_content.send);
    fs_stub.withArgs('../lua/receive_message.lua').returns(script_content.receive);
    fs_stub.withArgs('../lua/finish_message.lua').returns(script_content.finish);

    md5 = crypto.createHash('md5');
    md5.update(script_content.send, 'utf8');
    script_hash.send = md5.digest('hex');

    md5 = crypto.createHash('md5');
    md5.update(script_content.receive, 'utf8');
    script_hash.receive = md5.digest('hex');

    md5 = crypto.createHash('md5');
    md5.update(script_content.finish, 'utf8');
    script_hash.finish = md5.digest('hex');

    queue = new Queue(client);
  });

  afterEach(function() {
    client.evalsha.restore();
    client.time.restore();
    fs.readFileSync.restore();
  });

  describe('#ctor', function() {
    it('accepts a Redis client as a parameter', function() {
      // Arrange

      // Act
      var queue = new Queue(client);

      // Assert
      expect(queue).to.be.ok();
    });

    it('throws an ArgumentException when passed an empty object', function() {
      // Arrange
      var client = {};

      expect(function() {
        // Act
        var queue = new Queue(client);
      }).to.throwException(function(e) {
        // Assert
        expect(e).to.be.an(exceptions.ArgumentException);
      });
    });
  });

  describe('#submit', function() {
    it('issues the TIME command once', function(done) {
      // Arrange
      spy.yields();
      script_spy.withArgs('load', script_hash.send).yields(undefined, script_hash.send);

      // Act
      queue.submit(submit_queue, message_key, message, function() {
        // Assert
        expect(time_stub.calledOnce).to.be.ok();
        done();
      });
    });

    it('calls evalsha once', function() {
      // Arrange
      var submit_queue = 'queue:submitted',
          message_key = 'message:id',
          message = {field: '123'},
          callback = function() {};

      spy.yields();
      script_spy.withArgs('load', script_hash.send).yields(undefined, script_hash.send);

      // Act
      queue.submit(submit_queue, message_key, message, callback);

      // Assert
      expect(spy.calledWithExactly(script_hash.send, 4, 'message:id', 'message:received', message_key, submit_queue, JSON.stringify(message), js_time, callback)).to.be.ok();
    });

    it('invokes the callback with an undefined error after a successful submission', function(done) {
      wrapTest('../lua/send_message.lua', script_hash.send, function() {
        // Act
        queue.submit(submit_queue, message_key, message, function(err, result) {
          // Assert
          expect(err).not.to.be.ok();
          done();
        });
      });
    });

    it('invokes the callback with the new message ID after a successful submission', function(done) {
      // Arrange
      spy.yields(undefined, [1337]);
      script_spy.withArgs('load', script_hash.send).yields(undefined, script_hash.send);

      // Act
      queue.submit(submit_queue, message_key, message, function(err, result) {
        // Assert
        expect(result[0]).to.equal(1337);
        done();
      });
    });

    it('loads the correct lua script', function(done) {
      wrapTest('../lua/send_message.lua', script_hash.send, function() {
        // Act
        queue.submit(submit_queue, message_key, message, function() {
          // Assert
          expect(script_spy.calledOnce).to.be.ok();
          done();
        });
      });
    });

    it('loads the lua script before the first invocation', function(done) {
      wrapTest('../lua/send_message.lua', script_hash.send, function() {
        // Act
        queue.submit(submit_queue, message_key, message, function() {
          // Assert
          expect(script_spy.calledBefore(spy)).to.be.ok();
          done();
        });
      });
    });

    it('passes the loaded lua script to evalsha', function(done) {
      wrapTest('../lua/send_message.lua', script_hash.send, function() {
        // Act
        queue.submit(submit_queue, message_key, message, function() {
          // Assert
          expect(spy.args[0][0] === script_hash.send).to.be.ok();
          done();
        });
      });
    });

    it('only loads the lua script once', function() {
      // Arrange
      var callback = function() {};

      // Act
      queue.submit(submit_queue, message_key, message, callback);
      queue.submit(submit_queue, message_key, message, callback);

      // Assert
      expect(script_spy.calledOnce).to.be.ok();
    });
  });

  describe('#receive', function() {
    it('issues the TIME command once', function(done) {
      // Arrange
      spy.yields();
      script_spy.withArgs('load', script_hash.receive).yields(undefined, script_hash.receive);

      // Act
      queue.receive(submit_queue, function() {
        // Assert
        expect(time_stub.calledOnce).to.be.ok();
        done();
      });
    });

    it('calls evalsha once', function() {
      // Arrange
      var submit_queue = 'queue:submitted',
          receive_queue = 'queue:received',
          callback = function() {};

      spy.yields();
      script_spy.withArgs('load', script_hash.receive).yields(undefined, script_hash.receive);

      // Act
      queue.receive(submit_queue, receive_queue, callback);

      // Assert
      expect(spy.calledWithExactly(script_hash.receive, 2, submit_queue, receive_queue, js_time, callback)).to.be.ok();
    });

    it('invokes the callback with an undefined error after a successfully receiving a message', function(done) {
      wrapTest('../lua/receive_message.lua', script_hash.receive, function() {
        // Act
        queue.receive(submit_queue, receive_queue, function(err, result) {
          // Assert
          expect(err).not.to.be.ok();
          done();
        });
      });
    });

    it('invokes the callback with the message received from the submit queue', function(done) {
      // Arrange
      spy.yields(undefined, message);
      script_spy.withArgs('load', script_hash.receive).yields(undefined, script_hash.receive);

      // Act
      queue.receive(submit_queue, receive_queue, function(err, result) {
        // Assert
        expect(result).to.equal(message);
        done();
      });
    });

    it('loads the correct lua script', function(done) {
      wrapTest('../lua/receive_message.lua', script_hash.receive, function() {
        // Act
        queue.receive(submit_queue, receive_queue, function() {
          // Assert
          expect(script_spy.calledOnce).to.be.ok();
          done();
        });
      });
    });

    it('loads the lua script before the first invocation', function(done) {
      wrapTest('../lua/receive_message.lua', script_hash.receive, function() {
        // Act
        queue.receive(submit_queue, receive_queue, function() {
          // Assert
          expect(script_spy.calledBefore(spy)).to.be.ok();
          done();
        });
      });
    });

    it('passes the loaded lua script to evalsha', function(done) {
      wrapTest('../lua/receive_message.lua', script_hash.receive, function() {
        // Act
        queue.receive(submit_queue, receive_queue, function() {
          // Assert
          expect(spy.args[0][0] === script_hash.receive).to.be.ok();
          done();
        });
      });
    });

    it('only loads the lua script once', function() {
      // Arrange
      var callback = function() {};

      // Act
      queue.receive(submit_queue, receive_queue, callback);
      queue.receive(submit_queue, receive_queue, callback);

      // Assert
      expect(script_spy.calledOnce).to.be.ok();
    });
  });

  describe('#finish', function() {
    it('issues the TIME command once');
    it('calls evalsha once', function() {
      // Arrange
      var receive_queue = 'queue:receive',
          finish_queue = 'queue:finished_ok',
          message_id = 'message_id:123',
          status = 'finished ok',
          callback = function() {};

      spy.yields();
      script_spy.withArgs('load', script_hash.finish).yields(undefined, script_hash.finish);

      // Act
      queue.finish(receive_queue, finish_queue, message_id, status, callback);

      // Assert
      expect(spy.calledWithExactly(script_hash.finish, 4, receive_queue, finish_queue, message_id, 'message:received', status, js_time, callback)).to.be.ok();
    });
  });

  describe('#getQueueLength', function() {
    it('returns the length of the given queue', function(done) {
      // Arrange
      var err;
      var queue_length = 10;
      var queue_name = 'my_queue';
      var stub = sinon.stub(client, 'llen');
      stub.withArgs(queue_name).yields(err, queue_length);

      // Act
      queue.getQueueLength(queue_name, function(err, result) {
        // Assert
        expect(result).to.equal(queue_length);
        done();
      });
    });

    it('returns the length of the given queue', function(done) {
      // Arrange
      var err;
      var queue_name = 'my_queue';
      var stub = sinon.stub(client, 'llen');
      stub.withArgs('my_queue2').yields(err, 10);
      stub.withArgs('my_queue').yields(err, 0);

      // Act
      queue.getQueueLength(queue_name, function(err, result) {
        // Assert
        expect(result).to.equal(0);
        done();
      });
    });

    it('accepts multiple queue name parameters and returns the length of each as an array', function(done) {
      // Arrange
      var err;
      var stub = sinon.stub(client, 'llen');
      stub.withArgs('my_queue1').yields(err, 1);
      stub.withArgs('my_queue2').yields(err, 3);
      stub.withArgs('my_queue3').yields(err, 5);
      var expected = [1, 3, 5];

      // Act
      queue.getQueueLength(['my_queue1', 'my_queue2', 'my_queue3'], function(err, result) {
        // Assert
        expect(result).to.eql(expected);
        done();
      });
    });
  });
});
