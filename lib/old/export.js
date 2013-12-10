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
// class ExportService
//------------------------------------------------------------------------------

var ExportService = {};

/*
 * ExportService constructor function
 */

function createExportService(socket) {
  var service = Object.create(ExportService);
  service._sock = socket;
  service._exports = {};
  service._requests = {};
  return service;
}

/*
 * ExportService.export
 */

ExportService.export = function export(method, callback) {
  // Register the callback locally.
  this._exports[method] = callback;

  // Notify the broker so that it can start routing requests.
  this._sock.sendExport(method);
};

/*
 * ExportService._handleRequest
 */

ExportService._handleRequest = function handleRequest(requester, reqId, method, args, opts) {
  // Create a Request instance.
  var req = createRequest(this, requester, reqId, method, args, opts);

  // Register the instance under given request ID.
  this._requests[requester + '#' + reqId] = req;

  // Get the callback exported under given method name.
  var handler = this._exports[method];

  // Ignore the request is the method is not exported. This should never happen
  // since such requests should never be routed to this client, but it is better
  // to be careful and take this scenario into account.
  if (handler === undefined) {
    delete this._requests[requester + '#' + reqId];
    return;
  }

  // Run the request handler.
  handler(req);
};

/*
 * ExportService._handleInterrupt
 */

ExportService._handleInterrupt = function handleInterrupt(requester, reqId) {
  var req = this._requests[requester + '#' + reqId];
  if (req === undefined) return;
  req.emit('interrupt');
};

/*
 * ExportService._resolveRequest
 */

ExportService._resolveRequest = function(req, code, value) {
  this._sock.sendReply(req._requester, req._id, code, value);
  delete this._requests[req._requester + '#' + req._id];
};

//------------------------------------------------------------------------------
// class Request
//
// Available events:
//   request.on('interrupt')
//------------------------------------------------------------------------------

const STATE_RUNNING     = 0
    , STATE_INTERRUPTED = 1
    , STATE_RESOLVED    = 2;

// Inherit from EventEmitter.
var Request = Object.create(EventEmitter.prototype);

// A noop stream being used if no stream is requested from the other side.
var discardStream = new EventEmitter();

/*
 * Request constructor function
 */

function createRequest(service, requester, reqId, method, args, opts) {
  var req = Object.create(Request);

  req._state     = STATE_RUNNING;
  req._service   = service;
  req._requester = requester;
  req._id        = reqId;

  req.method = method;
  req.args   = args;

  // Set up output streams according to the options received with the request.
  if (opts['Stdout-Tag'] !== undefined) {
    try {
      var streamId = opts['Stdout-Tag'].readUInt16BE(0);
      req.stdout = createStream(sock, requester, streamId);
    } catch (err) {
      return;
    }
  } else {
    req.stdout = discardStream;
  }

  if (opts['Stderr-Tag'] !== undefined) {
    try {
      var streamId = opts['Stderr-Tag'].readUInt16BE(0);
      req.stderr = createStream(sock, requester, streamId);
    } catch (err) {
      return;
    }
  } else {
    req.stderr = discardStream;
  }
};

/*
 * Request.signalProgress
 */

Request.signalProgress = function signalProgress() {
  if (this._state !== STATE_RUNNING) return;
  this._service._sock.sendProgress(this._requester, this._id);
};

/*
 * Request.resolve
 */

Request.resolve = function resolve(code, value) {
  if (this._state === STATE_RESOLVED) return;
  this._service._resolveRequest(this, code, value);
  this._state = STATE_RESOLVED;
};

/*
 * Request._handleInterrupt
 */

Request._handleInterrupt = function handleInterrupt() {
  this._state = STATE_INTERRUPTED;
  this.emit('interrupt');
};

//------------------------------------------------------------------------------
// class Stream
//
// Available events:
//   stream.on('frame', seq, frame)
//------------------------------------------------------------------------------

// Inherit from EventEmitter.
var Stream = Object.create(EventEmitter.prototype);

/*
 * Stream constructor function
 */

function createStream(sock, receiver, tag) {
  var stream = Object.create(Stream);

  stream._sock = sock;
  stream._receiver = receiver;
  stream._tag = tag;
  stream._seq = 0;

  return stream;
};

/*
 * Stream.write
 */

Stream.write = function write(frame) {
  var seq = this._nextSeq();
  this._sock._sendStreamFrame(this._receiver, this._tag, seq, frame);
};

/*
 * Stream._nextSeq
 */

Stream._nextSeq = function() {
  this._seq = (this._seq + 1) % (maxUInt16 + 1);
  return this._seq;
};

//------------------------------------------------------------------------------
// Exports
//------------------------------------------------------------------------------

module.exports.createExportService = createExportService;
