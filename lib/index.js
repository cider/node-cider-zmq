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
// Socket
// -----------------------------------------------------------------------------

var Socket = {};

/*
 * Socket.connect
 */

Socket.connect = function connect(opts, next) {
  // Handle the situation when opts are skipped.
  next = next || opts;
  if (next === opts) opts = {};

  // Check that all required arguments are set.
  var identity    = this._identity   || process.env['CIDER_IDENTITY']

    , rpcEndpoint = opts.rpcEndpoint || process.env['CIDER_ZMQ_RPC_ENDPOINT']
    , rpcSndhwm   = opts.rpcSndhwm   || process.env['CIDER_ZMQ_RPC_SNDHWM']
    , rpcRcvhwm   = opts.rpcRcvhwm   || process.env['CIDER_ZMQ_RPC_RCVHWM']

    , evtEndpoint = opts.evtEndpoint || process.env['CIDER_ZMQ_EVT_ENDPOINT']
    , evtRcvhwm   = opts.evtRcvhwm   || process.env['CIDER_ZMQ_EVT_RCVHWM'];

  if (identity === undefined) {
    next(new Error("argument 'identity' is not set"));
    return;
  }

  if (rpcEndpoint === undefined) {
    next(new Error("argument 'rpcEndpoint' is not set"));
    return;
  }

  if (evtEndpoint === undefined) {
    next(new Error("argument 'evtEndpoint' is not set"));
    return;
  }

  if (rpcSndhwm !== undefined) rpcSndhwm = parseInt(rpcSndhwm);
  if (rpcRcvhwm !== undefined) rpcRcvhwm = parseInt(rpcRcvhwm);
  if (evtRcvhwm !== undefined) evtRcvhwm = parseInt(evtRcvhwm);

  // Create the session object.
  var session = newSession(this._identity);

  // Set up handling of incoming messages.
  var msgHandler = new EventEmitter();

  msgHandler.on('PROGRESS', function(reqId) {
    var req = session._outboundRequests[reqId];
    if (req === undefined) return;
    req.emit('progress');
  });

  msgHandler.on('STREAM_FRAME', function(streamTag, seq, body) {
    var stream = session._inboundStreams[streamTag];
    if (stream === undefined) return;
    stream.emit('frame', seq, body);
  });

  msgHandler.on('REPLY', function(reqId, returnCode, returnValue) {
    var req = session._outboundRequests[reqId];
    if (req === undefined) return;

    // Free the stream connected to the requests.
    if (req.stdout._id !== undefined)
      delete session._inboundStreams[req.stdout._id]
    if (req.stderr._id !== undefined)
      delete session._inboundStreams[req.stderr._id]

    // Free the request itself.
    delete session._outboundRequests[reqId];

    req.emit('reply', returnCode, returnValue);
  });

  msgHandler.on('PING', function() {
    session._sendPong();
  });

  // Create the internal 0MQ sockets.
  var rpcSock = zmq.socket('dealer')
    , evtSock = zmq.socket('sub');

  rpcSock.setsockopt(zmq.ZMQ_IDENTITY, new Buffer(identity));
  if (rpcSndhwm !== undefined) rpcSock.setsockopt(zmq.ZMQ_SNDHWM, rpcSndhwm);
  if (rpcRcvhwm !== undefined) rpcSock.setsockopt(zmq.ZMQ_RCVHWM, rpcRcvhwm);

  if (evtRcvhwm !== undefined) evtSock.setsockopt(zmq.ZMQ_RCVHWM, evtRcvhwm);

  // Set up the raw message handlers. These handlers parse incoming messages
  // and emit them as events for our high-level message handler defined above.
  rpcSock.on('message', function() {
    var msg = arguments;

    if (msg.length < 3) return;
    if (msg[1].toString() !== HEADER) return;
    if (msg[2].length !== 1) return;
    
    switch (msg[2][0]) {
      case MSGTYPE_OHAI:
        if (msg.length % 2 !== 1) {
          next(new Error("invalid OHAI message received"));
          return;
        }

        var opts = {};
        for (i = 3; i < msg.length; i += 2) {
          opts[msg[i]] = msg[i+1];
        }

        msgHandler.handleOhai(opts, next);
        break;
      case MSGTYPE_REQUEST:
        if (msg.length < 6 || msg.length % 2 !== 0) return;

        var requester = msg[0].toString()
          , reqId
          , method = msg[4].toString()
          , args
          , opts = {};
        try {
          reqId = msg[3].readUInt16BE(0);
          args = session._codec.decode(msg[5]);
        } catch (err) {
          return;
        }
        for (i = 6; i < msg.length; i += 2) {
          opts[msg[i]] = msg[i+1];
        }

        inboundRequestsService.handleRequest(requester, reqId, method, args, opts);
        break;
      case MSGTYPE_INTERRUPT:
        if (msg.length != 4) return;

        var requester = msg[0].toString()
          , reqId;
        try {
          reqId = msg[3].readUInt16BE(0);
        } catch (err) {
          return;
        }

        inboundRequestsService.handleInterrupt(requester, reqId);
        break;
      case MSGTYPE_PROGRESS:
        if (msg.length != 4) return;

        var reqId;
        try {
          reqId = msg[3].readUInt16BE(0);
        } catch (err) {
          return;
        };

        msgHandler.emit('PROGRESS', reqId);
        break;
     case MSGTYPE_STREAM_FRAME:
        if (msg.length != 6) return;

        var streamTag
          , seq
          , body = msg[5];
        try {
          streamTag = msg[3].readUInt16BE(0);
          seq = msg[4].readUInt16BE(0);
        } catch (err) {
          return;
        }

        msgHandler.emit('STREAM_FRAME', streamTag, seq, body);
        break;
      case MSGTYPE_REPLY:
        if (msg.length != 6) return;

        var reqId
          , returnCode
          , returnValue;
        try {
          reqId = msg[3].readUInt16BE(0);
          returnCode = msg[4].readUInt8(0);
          returnValue = session._codec.decode(msg[5]);
        } catch (err) {
          return;
        }

        msgHandler.emit('REPLY', reqId, returnCode, returnValue);
        break;
      case MSGTYPE_SUBSCRIBE_ACK:
        if (msg.length != 5) return;

        var kind = msg[3].toString()
          , seq;
        try {
          seq = msg[4].readUInt16BE(0);
        } catch (err) {
          return;
        }

        eventService.handleSubscribeAck(kind, seq);
        break;
      case MSGTYPE_PING:
        msgHandler.emit('PING');
        break;
    }
  });
  rpcSock.on('error', function(err) {
    session.emit('error', err);
  });

  evtSock.on('message', function() {
    var msg = arguments;

    if (msg.length !== 6) return;
    if (msg[2].toString() != HEADER) return;
    if (msg[3].length !== 1 || msg[3][0] != MSGTYPE_EVENT) return;

    var kind = msg[0].toString()
      , body = {}
      , seq;

    try {
      if (msg[4].length !== 0) body = session._codec.decode(msg[4]);
      seq = msg[5].readUInt16BE(0);
    } catch (err) {
      return;
    }

    eventService.handleEvent(kind, seq, body);
  });
  evtSock.on('error', function(err) {
    session.emit('error', err);
  });

  // Connect to the broker, at last.
  session._rpcSock = rpcSock;
  session._evtSock = evtSock;

  try {
    rpcSock.connect(rpcEndpoint);
    evtSock.connect(evtEndpoint);
    session._sendHello();
  } catch (err) {
    next(err);
    return;
  }
}

// -----------------------------------------------------------------------------
// Session
// -----------------------------------------------------------------------------

var STATE_INITIALISED  = 0
  , STATE_CONNECTED    = 1
  , STATE_CLOSED       = 2;

var Session = Object.create(EventEmitter.prototype);

function newSession(identity) {
  var session = Object.create(Session);

  session._identity = identity;
  session._state = STATE_INITIALISED;
  session._exports = Object.create(EventEmitter.prototype);
  session._events = Object.create(EventEmitter.prototype);
  session._eventSeqs = {};
  session._inboundRequests = {};
  session._inboundStreams = {};
  session._outboundRequests = {};
  session._seq = 0;

  return session;
};

/*
 * Session.export
 */

Session.export = function(method, callback) {
  if (this._exports.listeners(method).length != 0)
    throw new Error('method already exported: ' + method);

  this._exports.on(method, callback);
  this._sendExport(method);
};

/*
 * Session.request
 */

Session.request = function(method, args) {
  var id = this._nextSeq();
  return newOutboundRequest(this, id, method, args);
};

/*
 * Session.publish
 */

Session.publish = function(eventKind, eventBody) {
  this._sendEvent(eventKind, eventBody);
};

/*
 * Session.subscribe
 */

Session.subscribe = function(eventKind, callback) {
  this._events.on(eventKind, callback);

  eventKind = new Buffer(eventKind);
  this._evtSock.setsockopt(zmq.ZMQ_SUBSCRIBE, eventKind);

  this._sendSubscribe(eventKind);
};

/*
 * Session.unsubscribe
 */

Session.unsubscribe = function(eventKind) {
  this._evtSock.setsockopt(zmq.ZMQ_UNSUBSCRIBE, new Buffer(eventKind));
  this._events.removeAllListeners(eventKind);
  this._sendUnsubscribe(eventKind);
  delete this._eventSeqs[eventKind];
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

/*
 * Session helpers for sending messages
 */

Session._sendHello = function() {
  this._rpcSock.send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_MSGTYPE_HELLO
  ]);
};

Session._sendKthxbye = function() {
  this._send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_MSGTYPE_KTHXBYE
  ]);
};

Session._sendExport = function(method) {
  this._send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_MSGTYPE_EXPORT,
      method,
  ]);
};

Session._sendRequest = function(req) {
  req._id = this._nextSeq();

  var reqId = new Buffer(2);
  reqId.writeUInt16BE(req._id, 0);

  var msgLength = 6;
  if (req.stdout.listeners('frame').length !== 0)
    msgLength += 2;
  if (req.stderr.listeners('frame').length !== 0)
    msgLength += 2;

  var msg = new Array(msgLength);
  msg[0] = FRAME_EMPTY;
  msg[1] = FRAME_HEADER;
  msg[2] = FRAME_MSGTYPE_REQUEST;
  msg[3] = reqId;
  msg[4] = req.method;
  msg[5] = this._codec.encode(req.args);

  var i = 6;
  if (req.stdout.listeners('frame').length !== 0) {
    msg[i] = new Buffer('Stdout-Tag');
    var tag = this._nextSeq()
      , tagBuffer = new Buffer(2);
    tagBuffer.writeUInt16BE(tag, 0);
    this._inboundStreams[tag] = req.stdout;
    msg[i+1] = tagBuffer;
    i += 2;
  }
  if (req.stderr.listeners('frame').length !== 0) {
    msg[i] = new Buffer('Stderr-Tag');
    var tag = this._nextSeq()
      , tagBuffer = new Buffer(2);
    tagBuffer.writeUInt16BE(tag, 0);
    this._inboundStreams[tag] = req.stderr;
    msg[i+1] = tagBuffer;
  }

  this._send(msg);
  this._outboundRequests[req._id] = req;
};

Session._sendInterrupt = function(reqId) {
  var reqIdBuffer = new Buffer(2);
  reqIdBuffer.writeUInt16BE(reqId, 0);

  this._send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_MSGTYPE_INTERRUPT,
      reqIdBuffer
  ]);
};

Session._sendProgress = function(requester, reqId) {
  var reqIdBuffer = new Buffer(2);
  reqIdBuffer.writeUInt16BE(reqId, 0);

  this._send([
      requester,
      FRAME_HEADER,
      FRAME_MSGTYPE_PROGRESS,
      reqIdBuffer
  ]);
};

Session._sendStreamFrame = function(receiver, streamTag, seq, body) {
  var streamTagBuffer = new Buffer(2);
  streamTagBuffer.writeUInt16BE(streamTag, 0);

  var seqBuffer = new Buffer(2);
  seqBuffer.writeUInt16BE(seq, 0);

  this._send([
      receiver,
      FRAME_HEADER,
      FRAME_MSGTYPE_STREAM_FRAME,
      streamTagBuffer,
      seqBuffer,
      body
  ]);
};

Session._sendReply = function(requester, reqId, returnCode, returnValue) {
  var reqIdBuffer = new Buffer(2);
  reqIdBuffer.writeUInt16BE(reqId, 0);

  var returnCodeBuffer = new Buffer(1);
  returnCodeBuffer.writeUInt8(returnCode, 0);

  this._send([
      requester,
      FRAME_HEADER,
      FRAME_MSGTYPE_REPLY,
      reqIdBuffer,
      returnCodeBuffer,
      this._codec.encode(returnValue)
  ]);

  delete this._inboundRequests[requester + "#" + reqId];
};

Session._sendSubscribe = function(eventKind) {
  this._send([
      eventKind,
      FRAME_HEADER,
      FRAME_MSGTYPE_SUBSCRIBE
  ]);
};

Session._sendUnsubscribe = function(eventKind) {
  this._send([
      eventKind,
      FRAME_HEADER,
      FRAME_MSGTYPE_UNSUBSCRIBE
  ]);
};

Session._sendEvent = function(eventKind, eventBody) {
  this._send([
      eventKind,
      FRAME_HEADER,
      FRAME_MSGTYPE_EVENT,
      this._codec.encode(eventBody)
  ]);
};

Session._sendPong = function() {
  this._rpcSock.send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_MSGTYPE_PONG
  ]);
};

Session._send = function(frames) {
  if (this._state !== STATE_CONNECTED)
    throw new Error("invalid state");

  this._rpcSock.send(frames);
};

/*
 * Other Session helpers
 */

var maxUInt16 = Math.pow(2, 16); // In fact max uint16 + 1 ...

Session._nextSeq = function() {
  this._seq = (this._seq + 1) % maxUInt16;
  return this._seq;
};
