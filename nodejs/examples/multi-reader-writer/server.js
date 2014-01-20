var cp = require('child_process'),
    workers = {},
    max_workers = 2,
    client = require('redis').createClient(6379, '192.168.33.12'),
    wait_timeout = 30,
    queues = {
      queued: 'queued',
      received: 'received',
      finished_ok: 'finished_ok',
      finished_with_error: 'finished_with_error '
    }

console.log('Parent process started => pid = %d', process.pid);

for (var i = 0; i < max_workers; i++) {
  child = cp.fork(__dirname + '/child.js');
  var worker = {child: child, state: 'idle'};

  console.log('Creating child process %d', child.pid);
  workers[child.pid] = {child: child, state: 'idle'};

  child.on('message', function(message) {
    workCompleted(message);
  });
}

var getIdleWorker = function() {
  for (var pid in workers) {
    var w = workers[pid];
    if (w.state === 'idle') {
      return w;
    }
  }
};

var waitForWork = function(callback) {
  var idle_worker = getIdleWorker();
  if (!idle_worker) {
    console.log('No free workers. Will now wait for a free worker before reading from the queue');
    return;
  }

  console.log('Waiting for work');
  client.brpoplpush(queues.queued, idle_worker.child.pid, wait_timeout, function(err, result) {
    if (result) {
      idle_worker.state = 'busy';
      console.log('Sending work %s to pid %d', result, idle_worker.child.pid);
      idle_worker.child.send(result);
    } else {
      console.log('Timed out waiting for work after %d second(s)', wait_timeout);
    }
    waitForWork();
  });
};

var workCompleted = function(message) {
  console.log('Worker [%d] has completed its work [%s]', message.pid, message.work);
  worker = workers[message.pid];

  if (worker.state === 'idle') {
    return console.log('Warning => worker [%d] has completed work but is currently idle', worker.child.pid);
  }

  worker.state = 'idle';
  waitForWork();
};

waitForWork();
