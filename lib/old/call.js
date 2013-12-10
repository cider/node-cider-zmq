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

//------------------------------------------------------------------------------
// class CallService
//------------------------------------------------------------------------------

var CallService = {};

/*
 * CallService constructor function
 */

function createCallService(socket) {
  var service = Object.create(CallService);

  service._sock  = socket;
  service._calls = {};
  service._seq   = 0;

  return service;
}

/*
 * CallService.call
 */

CallService.call = function call(method, args) {
  return createCall(this, this._sock, method, args);
};

/*
 * CallService._executeCall
 */

CallService._executeCall = function executeCall(call) {
  var id = this._nextId();
  call._id = id;

  if (this._calls[id] !== undefined) throw new Error('Call ID already taken');

  this._sock._sendRequest(call);
};

/*
 * CallService._handleProgress
 */

CallService._handleProgress = function handleProgress(callId) {
  var call = this._calls[callId]
  if (call === undefined) return;
  call._handleProgress();
};

/*
 * CallService._handleReply
 */

CallService._handleReply = function handleReply(callId, code, value) {
  var call = this._calls[callId];
  if (call === undefined) return;
  call._handleReply(code, value);
};

/*
 * CallService._nextId
 */

var maxUInt16 = Math.pow(2, 16) - 1;

CallService._nextId = function nextId() {
  var id = this._seq;
  this._seq = (this._seq + 1) % (maxUInt16 + 1);
  return id;
};

//------------------------------------------------------------------------------
// class Call
//
// Available events:
//   request.on('progress')
//   request.on('reply', code, value)
//   request.stdout.on('frame', seq, frame)
//   request.stderr.on('frame', seq, frame)
//------------------------------------------------------------------------------

const STATE_INITIALISED = 0
    , STATE_SENT        = 1
    , STATE_INTERRUPTED = 2
    , STATE_RESOLVED    = 3;

// Inherit from EventEmitter.
var Call = Object.create(EventEmitter.prototype);

/*
 * Call constructor function
 */

function createCall(service, sock, id, method, args) {
  var call = Object.create(Call);

  call._service = service;
  call._sock    = sock;
  call._state   = STATE_INITIALISED;
  call._id      = id;

  call.method = method;
  call.args   = args;
  call.stdout = Object.create(EventEmitter.prototype);
  call.stderr = Object.create(EventEmitter.prototype);

  return call;
};

/*
 * Call.send
 */

Call.send = function send() {
  if (this._state !== STATE_INITIALISED) throw new Error("Invalid state");

  this._service._executeCall(this);
  this._state = STATE_SENT;
};

/*
 * Call.interrupt
 */

Call.interrupt = function interrupt() {
  if (this._state !== STATE_SENT) throw new Error("Invalid state");

  this._sock._sendInterrupt(this._id);
  this._state = STATE_INTERRUPTED;
};

/*
 * Call._handleProgress
 */

Call._handleProgress = function handleProgress() {
  this.emit('progress');
}

/*
 * Call._handleReply
 */

Call._handleReply = function handleReply(code, value) {
  this._state = STATE_RESOLVED;
  this.emit('reply', code, value);
};
