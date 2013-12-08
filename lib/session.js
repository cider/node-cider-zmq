/*
  Copyright (c) 2013 Ondřej Kupka

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"),
  to deal in the Software without restriction, including without limitation
  the rights to use, copy, modify, merge, publish, distribute, sublicense,
  and/or sell copies of the Software, and to permit persons to whom
  the Software is furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included
  in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
  IN THE SOFTWARE.
*/

// -----------------------------------------------------------------------------
// Session
// -----------------------------------------------------------------------------

var Session = {};

/*
 * Session constructor function
 */

var STATE_INITIALISED  = 0
  , STATE_CONNECTED    = 1
  , STATE_CLOSED       = 2;

function createSession(sock) {
  var session = Object.create(Session);

  session._
  session._state = STATE_INITIALISED;

  return session;
};

/*
 * Session.registerMethod
 */

Session.registerMethod = function registerMethod() {

};

/*
 * Session.unregisterMethod
 */

Session.unregisterMethod = function unregisterMethod() {

};

/*
 * Session.publish
 */

Session.publish = function publish() {

};

/*
 * Session.subscribe
 */

Session.subscribe = function subscribe() {

};

/*
 * Session.unsubscribe
 */

Session.unsubscribe = function unsubscribe() {

};

/*
 * Session.close
 */

Session.close = function() {
  var self = this;

  if (self._state !== STATE_CONNECTED)
    throw new Error("invalid state");

  self._sendKthxbye();

  // Work around the problem of Socket.close being not synchronous really,
  // dropping messages in 0MQ buffers if there are any.
  self._state = STATE_CLOSED;
  process.stderr.write('Waiting for the 0MQ sockets to settle down...\n');
  setTimeout(function() {
    self._rpcSock.close();
    self._evtSock.close();
  }, 3 * 1000);
};
