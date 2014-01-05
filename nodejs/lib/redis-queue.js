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

exports.create = function(queue_name) {
  if (!queue_name) { throw 'A queue name must be supplied'; }
  return {
    name: queue_name,
    push: function(client, message) {
      client.lpush(queue_name, message);
    },
    pop: function(client, done) {
      client.rpop(queue_name, function(err, result) {
        done(err, result[1]);
      });
    }
  };
};
