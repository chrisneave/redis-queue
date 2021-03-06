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
  var message_body = { foo: 'bar' };
  var message_key = '123:abc';
  var raw_message = [
    'id', 1,
    'status', 'received',
    'requested_at', JSON.stringify(js_time),
    'concurrent_id', 'code1.test',
    'body', message_body,
    'received_at', JSON.stringify(js_time)
  ];
  var message_json = {
    id: raw_message[1],
    status: raw_message[3],
    requested_at: raw_message[5],
    concurrent_id: raw_message[7],
    body: raw_message[9],
    received_at: raw_message[11]
  };
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
    client.brpoplpush = function() {};
    client.server_info = {redis_version: '2.3.0'};

    spy = sinon.stub(client, 'evalsha');
    script_spy = sinon.stub(client, 'script');

    time_stub = sinon.stub(client, 'time', function(callback) {
      callback(undefined, redis_time);
    });

    fs_stub = sinon.stub(fs, 'readFileSync');
    fs_stub.withArgs('/Users/chris/dev/redis-queue/nodejs/lib/../lua/send_message.lua', 'utf8').returns(script_content.send);
    fs_stub.withArgs('/Users/chris/dev/redis-queue/nodejs/lib/../lua/receive_message.lua', 'utf8').returns(script_content.receive);
    fs_stub.withArgs('/Users/chris/dev/redis-queue/nodejs/lib/../lua/process_message.lua', 'utf8').returns(script_content.finish);

    md5 = crypto.createHash('md5');
    md5.update(script_content.send, 'utf8');
    script_hash.send = md5.digest('hex');

    md5 = crypto.createHash('md5');
    md5.update(script_content.receive, 'utf8');
    script_hash.receive = md5.digest('hex');

    md5 = crypto.createHash('md5');
    md5.update(script_content.finish, 'utf8');
    script_hash.finish = md5.digest('hex');

    script_spy.withArgs('load', script_content.send).yields(undefined, script_hash.send);
    script_spy.withArgs('load', script_content.receive).yields(undefined, script_hash.receive);
    script_spy.withArgs('load', script_content.finish).yields(undefined, script_hash.finish);

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
      spy.yieldsAsync();

      // Act
      queue.submit(submit_queue, message_key, message_body, function() {
        // Assert
        expect(time_stub.calledOnce).to.be.ok();
        done();
      });
    });

    it('calls evalsha once', function(done) {
      // Arrange
      var submit_queue = 'queue:submitted',
          callback = function() {};

      spy.yieldsAsync();

      // Act
      queue.submit(submit_queue, message_key, message_body, function() {
        // Assert
        expect(spy.calledWith(script_hash.send, 4, 'message:id', 'message:received', message_key, submit_queue, JSON.stringify(message_body), js_time)).to.be.ok();
        done();
      });
    });

    it('does not load the scripts if they have already been loaded', function(done) {
      // Arrange
      var submit_queue = 'queue:submitted',
          callback = function() {};

      spy.yieldsAsync();
      queue._scripts.send.hash = script_hash.send;

      // Act
      queue.submit(submit_queue, message_key, message_body, function() {
        // Assert
        expect(script_spy.called).not.to.be.ok();
        done();
      });
    });

    it('calls evalsha if the scripts have already been loaded', function(done) {
      // Arrange
      var submit_queue = 'queue:submitted',
          callback = function() {};

      spy.yieldsAsync();
      queue._scripts.send.hash = script_hash.send;

      // Act
      queue.submit(submit_queue, message_key, message_body, function() {
        // Assert
        expect(spy.calledWith(script_hash.send, 4, 'message:id', 'message:received', message_key, submit_queue, JSON.stringify(message_body), js_time)).to.be.ok();
        done();
      });
    });

    describe('the message is sent successfully', function() {
      it('invokes the callback with an undefined error after a successful submission', function(done) {
        // Arrange
        spy.yieldsAsync();

        // Act
        queue.submit(submit_queue, message_key, message_body, function(err, result) {
          // Assert
          expect(err).not.to.be.ok();
          done();
        });
      });

      it('invokes the callback with the new message ID after a successful submission', function(done) {
        // Arrange
        spy.yieldsAsync(undefined, [1337]);

        // Act
        queue.submit(submit_queue, message_key, message_body, function(err, result) {
          // Assert
          expect(result[0]).to.equal(1337);
          done();
        });
      });
    });
  });

  describe('#receive', function() {
    it('issues the TIME command once', function(done) {
      // Arrange
      spy.yieldsAsync(undefined, raw_message);

      // Act
      queue.receive(submit_queue, function() {
        // Assert
        expect(time_stub.calledOnce).to.be.ok();
        done();
      });
    });

    it('calls evalsha once', function(done) {
      // Arrange
      var submit_queue = 'queue:submitted',
          receive_queue = 'queue:received',
          callback = function() {};

      spy.yieldsAsync(undefined, raw_message);
      script_spy.withArgs('load', script_content.receive).yieldsAsync(undefined, script_hash.receive);

      // Act
      queue.receive(submit_queue, receive_queue, function() {
        // Assert
        expect(spy.calledWith(script_hash.receive, 3, submit_queue, receive_queue, undefined, js_time)).to.be.ok();
        done();
      });
    });

    it('calls evalsha if the scripts have already been loaded', function(done) {
      // Arrange
      var submit_queue = 'queue:submitted',
          receive_queue = 'queue:received',
          callback = function() {};

      spy.yieldsAsync(undefined, raw_message);
      queue._scripts.receive.hash = script_hash.receive;

      // Act
      queue.receive(submit_queue, receive_queue, function() {
        // Assert
        expect(spy.calledWith(script_hash.receive, 3, submit_queue, receive_queue, undefined, js_time)).to.be.ok();
        done();
      });
    });

    it('does not load the scripts if they have already been loaded', function(done) {
      // Arrange
      var submit_queue = 'queue:submitted',
          receive_queue = 'queue:received',
          callback = function() {};

      spy.yieldsAsync(undefined, raw_message);
      queue._scripts.receive.hash = script_hash.receive;

      // Act
      queue.receive(submit_queue, receive_queue, function() {
        // Assert
        expect(script_spy.called).not.to.be.ok();
        done();
      });
    });

    describe('the message is received successfully', function(done) {
      it('invokes the callback with an undefined error after a successfully receiving a message', function(done) {
        // Arrange
        spy.yieldsAsync(undefined, raw_message);

        // Act
        queue.receive(submit_queue, receive_queue, function(err, result) {
          // Assert
          expect(err).not.to.be.ok();
          done();
        });
      });

      it('invokes the callback with the message received from the submit queue', function(done) {
        // Arrange
        spy.yieldsAsync(undefined, raw_message);

        // Act
        queue.receive(submit_queue, receive_queue, function(err, result) {
          // Assert
          expect(result).to.eql(message_json);
          done();
        });
      });
    });

    describe('no message is received', function() {
      it('returns an undefined message', function(done) {
        // Arrange
        spy.yieldsAsync();

        // Act
        queue.receive(submit_queue, receive_queue, function(err, result) {
          // Assert
          expect(result).not.to.be.ok();
          done();
        });
      });
    });
  });

  describe('#finish', function() {
    var receive_queue = 'queue:receive',
        finish_queue = 'queue:finished_ok',
        message_id = 'message_id:123',
        status = 'finished ok';

    it('issues the TIME command once', function(done) {
      // Arrange
      spy.yieldsAsync();

      // Act
      queue.finish(receive_queue, finish_queue, message_id, status, function() {
        // Assert
        expect(time_stub.calledOnce).to.be.ok();
        done();
      });
    });

    it('calls evalsha once', function(done) {
      // Arrange
      var callback = function() {};

      spy.yieldsAsync();

      // Act
      queue.finish(receive_queue, finish_queue, message_id, status, function() {
        // Assert
        expect(spy.calledWith(script_hash.finish, 4, receive_queue, finish_queue, message_id, 'message:received', status, js_time)).to.be.ok();
        done();
      });
    });

    it('calls evalsha if the scripts have already been loaded', function(done) {
      // Arrange
      spy.yieldsAsync();
      queue._scripts.finish.hash = script_hash.finish;

      // Act
      queue.finish(receive_queue, finish_queue, message_id, status, function() {
        // Assert
        expect(spy.calledWith(script_hash.finish, 4, receive_queue, finish_queue, message_id, 'message:received', status, js_time)).to.be.ok();
        done();
      });
    });

    it('does not load the scripts if they have already been loaded', function(done) {
      // Arrange
      spy.yieldsAsync();
      queue._scripts.finish.hash = script_hash.finish;

      // Act
      queue.finish(receive_queue, finish_queue, message_id, status, function() {
        // Assert
        expect(script_spy.called).not.to.be.ok();
        done();
      });
    });
  });

  describe('#getQueueLength', function() {
    it('returns the length of the given queue', function(done) {
      // Arrange
      var err;
      var queue_length = 10;
      var queue_name = 'my_queue';
      var stub = sinon.stub(client, 'llen');
      stub.withArgs(queue_name).yieldsAsync(err, queue_length);

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
      stub.withArgs('my_queue2').yieldsAsync(err, 10);
      stub.withArgs('my_queue').yieldsAsync(err, 0);

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
      stub.withArgs('my_queue1').yieldsAsync(err, 1);
      stub.withArgs('my_queue2').yieldsAsync(err, 3);
      stub.withArgs('my_queue3').yieldsAsync(err, 5);
      var expected = [1, 3, 5];

      // Act
      queue.getQueueLength(['my_queue1', 'my_queue2', 'my_queue3'], function(err, result) {
        // Assert
        expect(result).to.eql(expected);
        done();
      });
    });
  });

  describe('#init', function() {
    it('loads the send script once', function(done) {
      // Arrange
      // Act
      queue.init(function() {
        // Assert
        expect(script_spy.withArgs('load', script_content.send).calledOnce).to.be.ok();
        done();
      });
    });

    it('loads the receive script once', function(done) {
      // Arrange
      // Act
      queue.init(function() {
        // Assert
        expect(script_spy.withArgs('load', script_content.receive).calledOnce).to.be.ok();
        done();
      });
    });

    it('loads the finish script once', function(done) {
      // Arrange
      // Act
      queue.init(function() {
        // Assert
        expect(script_spy.withArgs('load', script_content.finish).calledOnce).to.be.ok();
        done();
      });
    });

    it('loads each script once', function(done) {
      // Arrange
      // Act
      queue.init(function() {
        queue.init(function() {
          // Assert
          expect(script_spy.withArgs('load', script_content.send).calledOnce).to.be.ok();
          done();
        });
      });
    });
  });

  describe('#listen', function() {
    var err,
        llen_stub,
        brpoplpush_spy,
        blocking_timeout = 1;

    // Check length of queue
    // If greater than 1 perform non-blocking receive
    // If zero perform a blocking receive
    beforeEach(function() {
      llen_stub = sinon.stub(client, 'llen');
      brpoplpush_spy = sinon.stub(client, 'brpoplpush');
    });

    it('checks the length of the queue before receiving messages', function(done) {
      // Arrange
      llen_stub.withArgs(submit_queue).yieldsAsync(err, 0);
      spy.yieldsAsync(err, raw_message);
      brpoplpush_spy.withArgs(submit_queue, receive_queue, blocking_timeout).yieldsAsync(err, raw_message[1]);

      // Act
      queue.listen(submit_queue, receive_queue, function() {
        queue._listeners = [];

        // Assert
        expect(llen_stub.withArgs(submit_queue).calledBefore(brpoplpush_spy)).to.be.ok();
        done();
      });
    });

    describe('the queue is empty', function() {
      it('performs a blocking receive', function(done) {
        // Arrange
        llen_stub.withArgs(submit_queue).yieldsAsync(err, 0);
        spy.yieldsAsync(err, raw_message);
        brpoplpush_spy.withArgs(submit_queue, receive_queue, blocking_timeout).yieldsAsync(err, raw_message[1]);

        // Act
        queue.listen(submit_queue, receive_queue, function() {
          queue._listeners = [];

          // Assert
          expect(brpoplpush_spy.calledOnce).to.be.ok();
          done();
        });
      });
    });

    describe('the queue contains one or more messages', function() {
      it('performs a non-blocking receive', function(done) {
        // Arrange
        llen_stub.withArgs(submit_queue).yieldsAsync(err, 1);
        spy.yieldsAsync(err, raw_message);

        // Act
        queue.listen(submit_queue, receive_queue, function() {
          // Assert
          expect(spy.calledWith(script_hash.receive, 3, submit_queue, receive_queue, undefined, js_time)).to.be.ok();
          done();
        });
      });

      it('invokes each callback with the message received', function(done) {
        // Arrange
        var queue_length = 3,
            evalsha_stub,
            count = 0;
        llen_stub.withArgs(submit_queue).yieldsAsync(err, queue_length);
        client.evalsha.restore();
        evalsha_stub = sinon.stub(client, 'evalsha');
        evalsha_stub.yieldsAsync(err, raw_message);

        // Act
        queue.listen(submit_queue, receive_queue, function(err, result) {
          expect(result).to.eql(message_json);
          count++;
          queue_length--;
          if (queue_length < 1) {
            expect(count).to.equal(3);
            return done();
          }

          llen_stub.withArgs(submit_queue).yields(err, queue_length);
        });
      });
    });

    describe('a message is received during a blocking receive', function() {
      it('issues the TIME command once', function(done) {
        // Arrange
        llen_stub.withArgs(submit_queue).yieldsAsync(err, 0);
        spy.yieldsAsync(err, raw_message);
        brpoplpush_spy.withArgs(submit_queue, receive_queue, blocking_timeout).yieldsAsync(err, raw_message[1]);

        // Act
        queue.listen(submit_queue, receive_queue, function() {
          queue._listeners = [];
          // Assert
          expect(time_stub.calledOnce).to.be.ok();
          done();
        });
      });

      it('calls evalsha specifying the key to the received message preventing a second attempt to pop the message from the queue', function(done) {
        // Arrange
        llen_stub.withArgs(submit_queue).yieldsAsync(err, 0);
        brpoplpush_spy.withArgs(submit_queue, receive_queue, blocking_timeout).yieldsAsync(err, raw_message[1]);
        spy.yieldsAsync(err, raw_message);

        // Act
        queue.listen(submit_queue, receive_queue, function() {
          queue._listeners = [];
          // Assert
          expect(spy.args[0][4]).to.equal(raw_message[1]);
          done();
        });
      });
    });
  });
});
